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
	"io"
	"log"
	"net"
	"sync"
	"testing"

	pb "github.com/conduitio-labs/conduit-connector-grpc-client/proto/v1"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestWrite_Success(t *testing.T) {
	is := is.New(t)
	dest, ctx := prepareServerAndDestination(t)

	records := []sdk.Record{
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
	n, err := dest.Write(ctx, records)
	is.NoErr(err)
	is.Equal(n, 2)
}

func prepareServerAndDestination(t *testing.T) (sdk.Destination, context.Context) {
	is := is.New(t)
	// use in-memory connection
	lis := bufconn.Listen(1024 * 1024)
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	// prepare server
	startTestServer(t, lis)

	// prepare destination (client)
	ctx := context.Background()
	dest := NewDestinationWithDialer(dialer)
	err := dest.Configure(ctx, map[string]string{"url": "bufnet"})
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

func startTestServer(t *testing.T, lis net.Listener) {
	ctrl := gomock.NewController(t)
	srv := grpc.NewServer()

	// create and register simple mock server
	mockServer := pb.NewMockStreamServiceServer(ctrl)
	mockServer.EXPECT().
		Stream(gomock.Any()).
		DoAndReturn(
			func(stream pb.StreamService_StreamServer) error {
				for {
					// read from the stream to simulate receiving data from the client
					req, err := stream.Recv()
					if err == io.EOF {
						return nil
					}
					if err != nil {
						return err
					}

					// Write to the stream to simulate sending data to the client
					resp := &pb.Ack{AckPosition: req.Position}
					if err := stream.Send(resp); err != nil {
						return err
					}
				}
			},
		)
	pb.RegisterStreamServiceServer(srv, mockServer)

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
		srv.GracefulStop()
		wg.Wait()
	})
}
