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
	"errors"
	"fmt"
	"time"

	pb "github.com/conduitio-labs/conduit-connector-grpc-client/proto/v1"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

var (
	errOpenContextCanceled = errors.New("open context is canceled")
	errMaxDowntimeReached  = errors.New("maxDowntime is reached while waiting for server to reconnect")
)

type StreamManager struct {
	conn           *grpc.ClientConn
	reconnectDelay time.Duration
	maxDowntime    time.Duration

	stream     pb.SourceService_StreamClient
	client     pb.SourceServiceClient
	m          chan struct{} // buffered channel used as a mutex to guard access to stream
	streamDone chan struct{} // signals that the connection to the server was lost, by closing the channel

	// stores the reason for exiting Run
	reason error
}

func NewStreamManager(ctx context.Context, conn *grpc.ClientConn, reconnectDelay, maxDowntime time.Duration) (*StreamManager, error) {
	client := pb.NewSourceServiceClient(conn)
	stream, err := client.Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create a bidirectional stream: %w", err)
	}
	return &StreamManager{
		conn:           conn,
		client:         client,
		stream:         stream,
		reconnectDelay: reconnectDelay,
		maxDowntime:    maxDowntime,
		m:              make(chan struct{}, 1),
		streamDone:     make(chan struct{}),
	}, nil
}

// Run a blocking method that monitors the status of the stream connection, If the stream
// gets closed it waits for the connection to come up again and reestablishes the stream again.
func (sm *StreamManager) Run(ctx context.Context) (err error) {
	defer func() {
		_ = sm.lock(context.Background())
		defer sm.unlock()
		sm.stream = nil
		sm.reason = err
	}()
	return sm.monitorConnectionStatus(ctx)
}

// Get blocks until a stream is available. If the context gets closed it
// returns the context error.
func (sm *StreamManager) Get(ctx context.Context) (pb.SourceService_StreamClient, error) {
	err := sm.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer sm.unlock()
	if sm.stream == nil {
		return nil, sm.reason
	}
	return sm.stream, nil
}

// monitorConnectionStatus monitors the connection status when it changes from
// the Ready state, when connection is lost, reconnecting will be attempted
// every `ReconnectDelay`, an error will be returned if connection is not
// successful after `maxDowntime`.
func (sm *StreamManager) monitorConnectionStatus(ctx context.Context) error {
	for {
		ok := sm.conn.WaitForStateChange(ctx, connectivity.Ready)
		if !ok {
			return ctx.Err()
		}
		sdk.Logger(ctx).Warn().Msgf("connection to the server is lost, will try and reconnect every %s", sm.reconnectDelay.String())
		// lock stream until connection is restored, or an error occurred
		err := sm.lock(ctx)
		if err != nil {
			return err
		}
		// in case reconnect fails and stream is nil
		sm.stream = nil
		close(sm.streamDone)
		err = sm.reconnect(ctx)
		if err != nil {
			sm.reason = err
			sm.unlock()
			return err
		}
		sm.unlock()
	}
}

// reconnect calls an RPC every `ReconnectDelay` until stream is created successfully,
// if `MaxDowntime` is reached it returns an error.
func (sm *StreamManager) reconnect(ctx context.Context) error {
	ticker := time.NewTicker(sm.reconnectDelay)
	timeoutCtx, cancel := context.WithTimeout(ctx, sm.maxDowntime)
	defer cancel()
	for {
		select {
		case <-ticker.C:
			stream, err := sm.client.Stream(ctx)
			if err != nil {
				sdk.Logger(ctx).Warn().Msgf("failed reconnection attempt to the server: %s", err.Error())
				continue
			}
			sdk.Logger(ctx).Info().Msg("connection to the server is restored")
			sm.stream = stream
			sm.streamDone = make(chan struct{})
			return nil
		case <-timeoutCtx.Done():
			return errMaxDowntimeReached
		case <-ctx.Done():
			return errOpenContextCanceled
		}
	}
}

// StreamDone returns a channel that will be closed when the connection to the server is lost.
func (sm *StreamManager) StreamDone(ctx context.Context) (chan struct{}, error) {
	err := sm.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer sm.unlock()
	return sm.streamDone, nil
}

func (sm *StreamManager) lock(ctx context.Context) error {
	select {
	case sm.m <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sm *StreamManager) unlock() {
	<-sm.m
}
