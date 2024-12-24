// Copyright © 2023 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-grpc-client/fromproto"
	pb "github.com/conduitio-labs/conduit-connector-grpc-client/proto/v1"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/test/bufconn"
)

var records = []opencdc.Record{
	{
		Position:  opencdc.Position("foo"),
		Operation: opencdc.OperationSnapshot,
		Key:       opencdc.StructuredData{"id1": "6"},
		Payload: opencdc.Change{
			After: opencdc.StructuredData{
				"foo": "bar",
			},
		},
	},
	{
		Position:  opencdc.Position("foobar"),
		Operation: opencdc.OperationSnapshot,
		Key:       opencdc.RawData("bar"),
		Payload: opencdc.Change{
			After: opencdc.RawData("baz"),
		},
	},
}

const (
	clientCertPath = "./test/certs/client.crt"
	clientKeyPath  = "./test/certs/client.key"
	serverCertPath = "./test/certs/server.crt"
	serverKeyPath  = "./test/certs/server.key"
	caCertPath     = "./test/certs/ca.crt"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestConfigure_DisableMTLS(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	dest := NewDestination()
	err := dest.Configure(ctx, map[string]string{
		"url":                  "localhost",
		"mtls.disabled":        "false",
		"mtls.client.certPath": "", // empty path, should fail
		"mtls.client.keyPath":  clientKeyPath,
		"mtls.ca.certPath":     caCertPath,
	})
	is.True(err != nil)
	err = dest.Configure(ctx, map[string]string{
		"url":                  "localhost",
		"mtls.disabled":        "true", // disabled
		"mtls.client.certPath": "",     // should be ok
	})
	is.NoErr(err)
}

func TestWrite_Success(t *testing.T) {
	is := is.New(t)
	dest, ctx := prepareServerAndDestination(t, records)
	n, err := dest.Write(ctx, records)
	is.NoErr(err)
	is.Equal(n, 2)
}

func TestBackoffRetry_MaxDowntime(t *testing.T) {
	is := is.New(t)
	// use in-memory connection
	lis := bufconn.Listen(1024 * 1024)
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	// prepare server
	srv := grpc.NewServer()

	// start gRPC server
	go func() {
		err := srv.Serve(lis)
		is.NoErr(err)
	}()

	// prepare destination (client)
	ctx := context.Background()
	dest := NewDestinationWithDialer(dialer)
	err := dest.Configure(ctx, map[string]string{
		"url":            "passthrough://bufnet",
		"rateLimit":      "0",
		"maxDowntime":    "500ms",
		"reconnectDelay": "200ms",
		"mtls.disabled":  "true",
	})
	is.NoErr(err)
	// Open will start monitoring connection status
	err = dest.Open(ctx)
	is.NoErr(err)
	defer func() {
		err := dest.Teardown(ctx)
		is.NoErr(err)
	}()
	// connection will be lost
	srv.Stop()
	// maxDowntime is 0.5 second, sleep for 1
	time.Sleep(1 * time.Second)
	// attempt to write a record, to get the connection error
	n, err := dest.Write(ctx, []opencdc.Record{
		{Position: opencdc.Position("foo")},
	})
	is.True(errors.Is(err, errMaxDowntimeReached))
	is.Equal(n, 0)
}

func TestBackoffRetry_Reconnect(t *testing.T) {
	is := is.New(t)
	var lisMutex sync.Mutex
	// use in-memory connection
	lis := bufconn.Listen(1024 * 1024)
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		// avoiding a race condition for the listener
		lisMutex.Lock()
		defer lisMutex.Unlock()
		return lis.DialContext(ctx)
	}

	// prepare and start server
	srv := grpc.NewServer()
	go func() {
		err := srv.Serve(lis)
		is.NoErr(err)
	}()

	// prepare destination (client)
	ctx := context.Background()
	dest := NewDestinationWithDialer(dialer)
	err := dest.Configure(ctx, map[string]string{
		"url":            "passthrough://bufnet",
		"rateLimit":      "0",
		"maxDowntime":    "5s",
		"reconnectDelay": "200ms",
		"mtls.disabled":  "true",
	})
	is.NoErr(err)
	// Open will start monitoring connection status
	err = dest.Open(ctx)
	is.NoErr(err)
	defer func() {
		err := dest.Teardown(ctx)
		is.NoErr(err)
	}()
	// stop server, connection will be lost
	srv.Stop()
	// reconnectDelay is 200ms, dest will try to reconnect two times
	time.Sleep(500 * time.Millisecond)
	// start server
	lisMutex.Lock()
	lis = bufconn.Listen(1024 * 1024)
	lisMutex.Unlock()
	startTestServer(t, lis, false, records)
	// reconnection will succeed
	time.Sleep(500 * time.Millisecond)
	// write records normally
	n, err := dest.Write(ctx, records)
	is.NoErr(err)
	is.Equal(n, 2)
}

func prepareServerAndDestination(t *testing.T, expected []opencdc.Record) (sdk.Destination, context.Context) {
	is := is.New(t)
	// use in-memory connection
	lis := bufconn.Listen(1024 * 1024)
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	// prepare server
	startTestServer(t, lis, true, expected)

	// prepare destination (client)
	ctx := context.Background()
	dest := NewDestinationWithDialer(dialer)
	err := dest.Configure(ctx, map[string]string{
		"url":                  "localhost",
		"mtls.client.certPath": clientCertPath,
		"mtls.client.keyPath":  clientKeyPath,
		"mtls.ca.certPath":     caCertPath,
		"rateLimit":            "0",
		"maxDowntime":          "1m",
		"reconnectDelay":       "10s",
	})
	is.NoErr(err)
	err = dest.Open(ctx)
	is.NoErr(err)

	// make sure to teardown destination
	t.Cleanup(func() {
		err := dest.Teardown(ctx)
		is.NoErr(err)
	})

	return dest, ctx
}

func startTestServer(t *testing.T, lis net.Listener, enableMTLS bool, _ []opencdc.Record) { //nolint:funlen // ignore.
	is := is.New(t)
	ctrl := gomock.NewController(t)
	serverOptions := make([]grpc.ServerOption, 0, 1)
	if enableMTLS {
		serverCert, err := tls.LoadX509KeyPair(serverCertPath, serverKeyPath)
		is.NoErr(err)
		caCert, err := os.ReadFile(caCertPath)
		is.NoErr(err)
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		// create TLS credentials with mTLS configuration
		creds := credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{serverCert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    caCertPool,
			MinVersion:   tls.VersionTLS13,
		})
		serverOptions = append(serverOptions, grpc.Creds(creds))
	}
	srv := grpc.NewServer(serverOptions...)

	// create and register simple mock server
	mockServer := pb.NewMockSourceServiceServer(ctrl)
	mockServer.EXPECT().
		Stream(gomock.Any()).
		DoAndReturn(
			func(stream pb.SourceService_StreamServer) error {
				for {
					// read from the stream to simulate receiving data from the client
					rec, err := stream.Recv()
					if err == io.EOF {
						return nil
					}
					if err != nil {
						return err
					}
					// convert the proto record to opencdc.Record to compare with expected records
					_, err = fromproto.Record(rec)
					if err != nil {
						return err
					}
					// Write to the stream to simulate sending data to the client
					resp := &pb.Ack{AckPosition: rec.Position}
					if err := stream.Send(resp); err != nil {
						return err
					}
				}
			},
		)
	pb.RegisterSourceServiceServer(srv, mockServer)

	// start gRPC server
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()
	t.Cleanup(func() {
		srv.Stop()
		wg.Wait()
	})
}
