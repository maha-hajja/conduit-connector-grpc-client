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

//go:generate paramgen -output=paramgen_dest.go Config

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/conduitio-labs/conduit-connector-grpc-client/toproto"
	"github.com/conduitio/bwlimit"
	"github.com/conduitio/bwlimit/bwgrpc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gopkg.in/tomb.v2"
)

type Destination struct {
	sdk.UnimplementedDestination

	config       Config
	conn         *grpc.ClientConn
	index        uint32
	expectedAcks []sdk.Position
	t            *tomb.Tomb

	sm *StreamManager
	am *AckManager

	// for testing: always empty, unless it's a test
	dialer func(ctx context.Context, _ string) (net.Conn, error)
}

type Config struct {
	// url to gRPC server
	URL string `json:"url" validate:"required"`
	// the bandwidth limit in bytes/second, use "0" to disable rate limiting.
	RateLimit int `json:"rateLimit" default:"0" validate:"gt=-1"`
	// delay between each gRPC request retry.
	ReconnectDelay time.Duration `json:"reconnectDelay" default:"5s"`
	// max downtime accepted for the server to be off.
	MaxDowntime time.Duration `json:"maxDowntime" default:"10m"`
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
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(d.dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()), // todo: will use mTLS with connection
		grpc.WithBlock(),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig}),
	}
	if d.config.RateLimit > 0 {
		dialOptions = append(dialOptions,
			bwgrpc.WithBandwidthLimitedContextDialer(bwlimit.Byte(d.config.RateLimit), bwlimit.Byte(d.config.RateLimit), d.dialer))
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, d.config.MaxDowntime)
	defer cancel()
	conn, err := grpc.DialContext(ctxTimeout,
		d.config.URL,
		dialOptions...,
	)
	if err != nil {
		return fmt.Errorf("failed to dial server: %w", err)
	}
	d.conn = conn

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

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	d.expectedAcks = make([]sdk.Position, 0, len(records))
	for _, r := range records {
		d.expectedAcks = append(d.expectedAcks, r.Position)
	}
	err := d.am.Expect(d.expectedAcks)
	if err != nil {
		return 0, err
	}
	got := 0
	for {
		err = d.sendRecords(ctx, got, records)
		if err == io.EOF || status.Code(err) == codes.Unavailable {
			got = d.am.Got()
			continue
		} else if err != nil {
			return d.am.Got(), err
		}
		got, err = d.am.Wait(ctx)
		if err == io.EOF {
			// connection dropped, stream received interrupted
			continue
		} else if err != nil {
			return got, err
		}
		return len(records), nil
	}
}

func (d *Destination) sendRecords(ctx context.Context, startFrom int, records []sdk.Record) error {
	stream, err := d.sm.Get(ctx)
	if err != nil {
		return err
	}
	for i := startFrom; i < len(records); i++ {
		r := records[i]
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
