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
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AckManager struct {
	sm *StreamManager

	expected  []opencdc.Position
	received  map[string]bool
	m         sync.Mutex    // guards access to expected and received
	batchDone chan struct{} // this channel is closed when the whole batch acks are received
	lastIndex uint32
	got       int
}

func NewAckManager(sm *StreamManager) *AckManager {
	return &AckManager{
		sm: sm,
	}
}

// Run is a blocking method that listen to acks and validates them,
// It returns an error if the context is cancelled or if an unrecoverable
// error happens.
func (am *AckManager) Run(ctx context.Context) error {
	return am.recvAcks(ctx)
}

// Expect lets the ack manager know what acks to expect in the next batch,
// If there are still open acks to be received from the previous batch,
// Expect returns an error.
// has to be called after Run and before Wait.
func (am *AckManager) Expect(expected []opencdc.Position) error {
	am.m.Lock()
	defer am.m.Unlock()
	got := am.got
	if got != len(am.expected) {
		return fmt.Errorf("some records from the last batch were not acked, received: %d, expected acks: %d", got, len(am.expected))
	}

	am.expected = expected
	am.received = make(map[string]bool, len(expected))
	for _, pos := range am.expected {
		am.received[string(pos)] = false
	}
	am.batchDone = make(chan struct{})
	am.got = 0

	return nil
}

// Wait blocks until all acks are received or the connection drops while
// waiting for acks, or the context was canceled.
// if the connection drops it returns io.EOF, if the context gets closed it returns
// the context error, otherwise it returns nil.
func (am *AckManager) Wait(ctx context.Context) (int, error) {
	streamDone, err := am.sm.StreamDone(ctx)
	if err != nil {
		return am.Got(), err
	}
	select {
	case <-am.batchDone:
		return len(am.expected), nil
	case <-ctx.Done():
		return am.Got(), ctx.Err()
	case <-streamDone:
		return am.Got(), io.EOF
	}
}

// Got returns the deduplicated acknowledgments we have received so far.
func (am *AckManager) Got() int {
	am.m.Lock()
	defer am.m.Unlock()
	return am.got
}

func (am *AckManager) recvAcks(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		stream, err := am.sm.Get(ctx)
		if err != nil {
			return err
		}
		ack, err := stream.Recv()
		if err == io.EOF || status.Code(err) == codes.Unavailable {
			continue
		} else if err != nil {
			return err
		}
		am.m.Lock()
		pos := ToRecordPosition(ack.AckPosition)
		err = am.validateAck(pos)
		if err != nil {
			am.m.Unlock()
			return err
		}
		// update the map and the number of unique acks received
		if val := am.received[string(pos.Original)]; !val {
			am.got++
			am.received[string(pos.Original)] = true
		} else {
			sdk.Logger(ctx).Debug().Bytes("position", pos.Original).Msg("a duplicated ack was received")
		}
		am.lastIndex = pos.Index
		// check if the whole batch acks were received
		if am.got == len(am.expected) {
			select {
			case <-am.batchDone:
				// channel is already closed
			default:
				close(am.batchDone)
			}
		}
		am.m.Unlock()
	}
}

func (am *AckManager) validateAck(pos Position) error {
	if len(am.expected) == 0 {
		return fmt.Errorf("expected acks need to be set, call the method Expect([]opencdc.Position) to set them after calling Run()")
	}
	if _, ok := am.received[string(pos.Original)]; !ok {
		return fmt.Errorf("received an unexpected ack: %s", string(pos.Original))
	}
	// check that no acks were skipped
	for _, p := range am.expected {
		if bytes.Equal(pos.Original, p) {
			break
		}
		if !am.received[string(p)] {
			return fmt.Errorf("acks received out of order, skipped ack: %s", string(p))
		}
	}
	// to handle the uint32 overflow case, if the last index we received is in the interval [MaxUint32-100, mMxUint32]
	// then we allow the received index to be in the interval [0-100]
	if am.lastIndex >= math.MaxUint32-100 && pos.Index <= 100 {
		return nil
	}
	// check that index is increasing
	if pos.Index < am.lastIndex {
		return fmt.Errorf("received an ack out of order, received: %d, last received index: %d", pos.Index, am.lastIndex)
	}
	return nil
}
