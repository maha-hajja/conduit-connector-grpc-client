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

//go:generate paramgen -output=paramgen_dest.go DestConfig

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"time"

	pb "github.com/conduitio-labs/conduit-connector-grpc-client/proto/v1"
	"github.com/conduitio-labs/conduit-connector-grpc-client/toproto"
	"github.com/conduitio/bwlimit"
	"github.com/conduitio/bwlimit/bwgrpc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Destination struct {
	sdk.UnimplementedDestination

	config DestConfig
	conn   *grpc.ClientConn
	stream pb.SourceService_StreamClient

	// mTLS
	clientCert tls.Certificate
	caCertPool *x509.CertPool

	// for testing: always empty, unless it's a test
	dialer func(ctx context.Context, _ string) (net.Conn, error)
}

type DestConfig struct {
	Config
}

// NewDestinationWithDialer for testing purposes.
func NewDestinationWithDialer(dialer func(ctx context.Context, _ string) (net.Conn, error)) sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{dialer: dialer})
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
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
	if !d.config.MTLS.Disable {
		d.clientCert, d.caCertPool, err = d.config.ParseMTLSFiles()
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(d.dialer),
		grpc.WithBlock(),
	}
	if d.config.RateLimit > 0 {
		dialOptions = append(dialOptions,
			bwgrpc.WithBandwidthLimitedContextDialer(bwlimit.Byte(d.config.RateLimit), bwlimit.Byte(d.config.RateLimit), d.dialer))
	}
	if !d.config.MTLS.Disable {
		// create TLS credentials with mTLS configuration
		creds := credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{d.clientCert},
			RootCAs:      d.caCertPool,
			MinVersion:   tls.VersionTLS13,
		})
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	} else {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Minute) // time.Minute is temporary until backoff retries is merged
	defer cancel()
	conn, err := grpc.DialContext(ctxTimeout,
		d.config.URL,
		dialOptions...,
	)
	if err != nil {
		return fmt.Errorf("failed to dial server: %w", err)
	}
	d.conn = conn

	// create the client
	client := pb.NewSourceServiceClient(conn)
	// call the Stream method to create a bidirectional streaming RPC stream
	d.stream, err = client.Stream(ctx)
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
