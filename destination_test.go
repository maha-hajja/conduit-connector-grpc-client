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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"testing"

	"github.com/conduitio-labs/conduit-connector-grpc-client/fromproto"
	pb "github.com/conduitio-labs/conduit-connector-grpc-client/proto/v1"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/test/bufconn"
)

var records = []sdk.Record{
	{
		Position:  sdk.Position("foo"),
		Operation: sdk.OperationSnapshot,
		Key:       sdk.StructuredData{"id1": "6"},
		Payload: sdk.Change{
			After: sdk.StructuredData{
				"foo": "bar",
			},
		},
	},
	{
		Position:  sdk.Position("foobar"),
		Operation: sdk.OperationSnapshot,
		Key:       sdk.RawData("bar"),
		Payload: sdk.Change{
			After: sdk.RawData("baz"),
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

func prepareServerAndDestination(t *testing.T, expected []sdk.Record) (sdk.Destination, context.Context) {
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

func startTestServer(t *testing.T, lis net.Listener, enableMTLS bool, expected []sdk.Record) {
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
				i := 0
				for {
					// read from the stream to simulate receiving data from the client
					rec, err := stream.Recv()
					if err == io.EOF {
						return nil
					}
					if err != nil {
						return err
					}
					// convert the proto record to sdk.Record to compare with expected records
					sdkRec, err := fromproto.Record(rec)
					if err != nil {
						return err
					}
					if !bytes.Equal(sdkRec.Bytes(), expected[i].Bytes()) {
						return fmt.Errorf("received record doesn't match the expected record")
					}
					i++

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
