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
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"

	"github.com/conduitio-labs/conduit-connector-grpc-client/toproto"
	"github.com/conduitio/bwlimit"
	"github.com/conduitio/bwlimit/bwgrpc"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gopkg.in/tomb.v2"
)

type Destination struct {
	sdk.UnimplementedDestination

	config DestConfig
	conn   *grpc.ClientConn
	index  uint32
	t      *tomb.Tomb

	sm *StreamManager
	am *AckManager

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

func (d *Destination) Parameters() config.Parameters {
	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg config.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	err := sdk.Util.ParseConfig(ctx, cfg, &d.config, NewDestination().Parameters())
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	if !d.config.MTLS.Disabled {
		d.clientCert, d.caCertPool, err = d.config.MTLS.ParseMTLSFiles()
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(d.dialer),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig}),
	}
	if d.config.RateLimit > 0 {
		dialOptions = append(dialOptions,
			bwgrpc.WithBandwidthLimitedContextDialer(bwlimit.Byte(d.config.RateLimit), bwlimit.Byte(d.config.RateLimit), d.dialer))
	}
	if !d.config.MTLS.Disabled {
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
	conn, err := grpc.NewClient(d.config.URL, dialOptions...)
	if err != nil {
		return fmt.Errorf("failed to create grpc client: %w", err)
	}
	d.conn = conn

	// Block until conn is ready.
	conn.Connect()
	ctxTimeout, cancel := context.WithTimeout(ctx, d.config.MaxDowntime)
	defer cancel()
	for {
		s := conn.GetState()
		if s == connectivity.Idle {
			conn.Connect()
		}
		if s == connectivity.Ready {
			break // connection is ready
		}
		if !conn.WaitForStateChange(ctxTimeout, s) {
			// ctx got timeout or canceled.
			return fmt.Errorf("failed to connect to server in time (state: %s)", s)
		}
	}

	d.sm, err = NewStreamManager(ctx, conn, d.config.ReconnectDelay, d.config.MaxDowntime)
	if err != nil {
		return err
	}
	d.am = NewAckManager(d.sm)

	d.t, ctx = tomb.WithContext(ctx)
	d.t.Go(func() error {
		return d.sm.Run(ctx)
	})
	d.t.Go(func() error {
		return d.am.Run(ctx)
	})

	return nil
}

func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	expectedAcks := make([]opencdc.Position, 0, len(records))
	for _, r := range records {
		expectedAcks = append(expectedAcks, r.Position)
	}
	err := d.am.Expect(expectedAcks)
	if err != nil {
		return 0, err
	}
	got := 0
	for {
		err = d.sendRecords(ctx, records[got:])
		if err == io.EOF || status.Code(err) == codes.Unavailable {
			got = d.am.Got()
			continue
		} else if err != nil {
			return d.am.Got(), err
		}
		got, err = d.am.Wait(ctx)
		if err == io.EOF {
			// this error indicates that connection to the sever was lost, and the stream is closed.
			// connector should continue, and will encounter `d.sendRecords` which tries to get a stream, and will
			// block until one is available.
			continue
		} else if err != nil {
			return got, err
		}
		return len(records), nil
	}
}

func (d *Destination) sendRecords(ctx context.Context, records []opencdc.Record) error {
	stream, err := d.sm.Get(ctx)
	if err != nil {
		return err
	}
	for _, r := range records {
		r.Position = AttachPositionIndex(r.Position, d.index)
		d.index++
		record, err := toproto.Record(r)
		if err != nil {
			return err
		}
		err = stream.Send(record)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	if d.sm != nil {
		stream, _ := d.sm.Get(ctx)
		if stream != nil {
			err := stream.CloseSend()
			if err != nil {
				return err
			}
		}
	}
	if d.conn != nil {
		err := d.conn.Close()
		if err != nil {
			return err
		}
	}
	if d.t != nil {
		d.t.Kill(nil)
		_ = d.t.Wait()
	}
	return nil
}
