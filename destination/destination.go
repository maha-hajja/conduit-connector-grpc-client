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

package destination

//go:generate paramgen -output=paramgen_dest.go Config

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/conduitio-labs/conduit-connector-grpc-client/destination/toproto"
	pb "github.com/conduitio-labs/conduit-connector-grpc-client/proto/v1"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/grpc"
)

type Destination struct {
	sdk.UnimplementedDestination

	config Config
	conn   *grpc.ClientConn
	stream pb.StreamService_StreamClient
	client pb.StreamServiceClient
}

type Config struct {
	// url to gRPC server
	URL string `json:"url" validate:"required"`
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{})
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	err := sdk.Util.ParseConfig(cfg, &d.config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	conn, err := grpc.Dial(d.config.URL,
		grpc.WithInsecure(), //nolint:staticcheck // todo: will use mTLS with connection
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("failed to dial server: %w", err)
	}
	d.conn = conn

	// create the client
	d.client = pb.NewStreamServiceClient(conn)
	// call the Stream method to create a bidirectional streaming RPC stream
	d.stream, err = d.client.Stream(ctx)
	if err != nil {
		return fmt.Errorf("failed to create a bidirectional stream: %w", err)
	}
	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for _, r := range records {
		record, err := toproto.Record(r)
		if err != nil {
			return 0, err
		}
		// todo: backoff retries until connection is reestablished and create a new stream
		err = d.stream.Send(record)
		if err == io.EOF {
			return 0, fmt.Errorf("stream was closed: %w", err)
		}
		if err != nil {
			return 0, fmt.Errorf("failed to send record: %w", err)
		}
	}
	// todo: go routine for receiving acks
	for i := range records {
		// block until ack is received
		ack, err := d.stream.Recv()
		if err == io.EOF {
			return i, fmt.Errorf("stream was closed: %w", err)
		}
		if err != nil {
			return i, fmt.Errorf("failed to receive ack: %w", err)
		}
		if !bytes.Equal(ack.AckPosition, records[i].Position) {
			return i, fmt.Errorf("unexpected ack, got: %v, want: %v", ack.AckPosition, records[i].Position)
		}
		sdk.Logger(ctx).Trace().Bytes("position", ack.AckPosition).Msg("ack received")
	}
	return len(records), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	if d.stream != nil {
		err := d.stream.CloseSend()
		if err != nil {
			return err
		}
	}
	if d.conn != nil {
		err := d.conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
