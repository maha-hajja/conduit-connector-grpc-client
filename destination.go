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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	pb "github.com/conduitio-labs/conduit-connector-grpc-client/proto/v1"
	"github.com/conduitio-labs/conduit-connector-grpc-client/toproto"
	"github.com/conduitio/bwlimit"
	"github.com/conduitio/bwlimit/bwgrpc"
	opencdcv1 "github.com/conduitio/conduit-connector-protocol/proto/opencdc/v1"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gopkg.in/tomb.v2"
)

type Destination struct {
	sdk.UnimplementedDestination

	config      Config
	conn        *grpc.ClientConn
	stream      pb.SourceService_StreamClient
	streamMutex sync.Mutex
	t           *tomb.Tomb

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

var (
	openContextCanceledErr  = errors.New("open context is canceled")
	writeContextCanceledErr = errors.New("write context is canceled")
	maxDowntimeReachedErr   = errors.New("maxDowntime is reached while waiting for server to reconnect")
)

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

	// create the client
	client := pb.NewSourceServiceClient(conn)
	// call the Stream method to create a bidirectional streaming RPC stream
	d.stream, err = client.Stream(ctx)
	if err != nil {
		return fmt.Errorf("failed to create a bidirectional stream: %w", err)
	}
	// spawn a go routine to monitor the connection status
	d.t, ctx = tomb.WithContext(ctx)
	d.t.Go(func() error {
		return d.monitorConnectionStatus(ctx, client)
	})
	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for _, r := range records {
		record, err := toproto.Record(r)
		if err != nil {
			return 0, err
		}
		err = d.sendRecordWithRetries(ctx, record)
		if err != nil {
			return 0, fmt.Errorf("failed to send record: %w", err)
		}
	}
	for i := range records {
		ack, err := d.recvAckWithRetries(ctx)
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

func (d *Destination) sendRecordWithRetries(ctx context.Context, record *opencdcv1.Record) error {
	d.streamMutex.Lock()
	select {
	case <-d.t.Dying():
		return d.t.Err()
	default:
	}
	err := d.stream.Send(record)
	if err == io.EOF || status.Code(err) == codes.Unavailable {
		d.streamMutex.Unlock()
		err := d.waitForReadyState(ctx, d.conn.GetState())
		if err != nil {
			return err
		}
		return d.sendRecordWithRetries(ctx, record)
	}
	d.streamMutex.Unlock()
	return err
}

func (d *Destination) recvAckWithRetries(ctx context.Context) (*pb.Ack, error) {
	d.streamMutex.Lock()
	select {
	case <-d.t.Dying():
		return nil, d.t.Err()
	default:
	}
	ack, err := d.stream.Recv()
	if err == io.EOF || status.Code(err) == codes.Unavailable {
		d.streamMutex.Unlock()
		err := d.waitForReadyState(ctx, d.conn.GetState())
		if err != nil {
			return nil, err
		}
		return d.recvAckWithRetries(ctx)
	}
	d.streamMutex.Unlock()
	return ack, err
}

// waitForReadyState waits for the connection state to change into the ready state, returns an error if `maxDowntime` is reached.
func (d *Destination) waitForReadyState(ctx context.Context, currentState connectivity.State) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, d.config.MaxDowntime)
	defer cancel()
	// loop until state is Ready, or timeout is reached
	for {
		// wait for the state to change
		ok := d.conn.WaitForStateChange(timeoutCtx, currentState)
		if !ok {
			return fmt.Errorf("maxDowntime time reached while waiting for server to reconnect")
		}
		currentState = d.conn.GetState()
		// break loop if state is Ready
		if currentState == connectivity.Ready {
			break
		}
		select {
		case <-d.t.Dying():
			return d.t.Err()
		case <-ctx.Done():
			return writeContextCanceledErr
		default:
		}
	}
	return nil
}

// monitorConnectionStatus checks the status of the connection each `ReconnectDelay`, if connection is lost, the stream
// is locked and reconnect method is called. lock will be released once connection is restored or `maxDowntime` is reached.
func (d *Destination) monitorConnectionStatus(ctx context.Context, client pb.SourceServiceClient) error {
	ticker := time.NewTicker(d.config.ReconnectDelay)
	for {
		select {
		case <-ticker.C:
			state := d.conn.GetState()
			if state != connectivity.Ready {
				sdk.Logger(ctx).Warn().Msgf("connection to the server is lost, will try and reconnect every %s", d.config.ReconnectDelay.String())
				// lock stream until connection is restored, or an error occurred
				d.streamMutex.Lock()
				err := d.reconnect(ctx, client)
				if err != nil {
					d.streamMutex.Unlock()
					return err
				}
				d.streamMutex.Unlock()
			}
		case <-ctx.Done():
			return openContextCanceledErr
		}
	}
}

// reconnect calls an RPC every `ReconnectDelay` to try and reconnect until stream is created successfully, if
// `MaxDowntime` is reached it returns an error.
func (d *Destination) reconnect(ctx context.Context, client pb.SourceServiceClient) error {
	ticker := time.NewTicker(d.config.ReconnectDelay)
	var err error
	timeoutCtx, cancel := context.WithTimeout(ctx, d.config.MaxDowntime)
	defer cancel()
	for {
		select {
		case <-ticker.C:
			d.stream, err = client.Stream(ctx)
			if err == nil {
				sdk.Logger(ctx).Info().Msg("connection to the server is restored")
				return nil
			}
			sdk.Logger(ctx).Warn().Msgf("failed reconnection attempt to the server: %s", err.Error())
		case <-timeoutCtx.Done():
			return maxDowntimeReachedErr
		case <-ctx.Done():
			return openContextCanceledErr
		}
	}
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
	if d.t != nil {
		d.t.Kill(nil)
		_ = d.t.Wait()
	}
	return nil
}
