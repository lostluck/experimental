// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package harness

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	fnpb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/fnexecution_v1"
	"github.com/pkg/errors"
	"golang.org/x/exp/slog"
	"google.golang.org/protobuf/encoding/prototext"
)

type writeTypeEnum int32

const (
	StateWriteAppend writeTypeEnum = 0
	StateWriteClear  writeTypeEnum = 1
)

// ScopedStateManager scopes the global gRPC state manager to a single instruction
// for side input use. The indirection makes it easier to control access.
type ScopedStateManager struct {
	mgr    *StateChannelManager
	instID instructionID
	url    string

	closed bool
	mu     sync.Mutex

	// cache *statecache.SideInputCache
}

// NewScopedStateManager returns a ScopedStateReader for the given instruction.
func NewScopedStateManager(mgr *StateChannelManager, instID instructionID, stateURL string) *ScopedStateManager {
	return &ScopedStateManager{mgr: mgr, instID: instID, url: stateURL}
}

// // GetSideInputCache returns a pointer to the SideInputCache being used by the SDK harness.
// func (s *ScopedStateReader) GetSideInputCache() exec.SideCache {
// 	return s.cache
// }

func (s *ScopedStateManager) OpenReader(ctx context.Context, key *fnpb.StateKey) (NextBuffer, error) {
	ch, err := s.open(ctx)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.Errorf("instruction %v no longer processing", s.instID)
	}
	ret := &stateKeyReader{
		instID: s.instID,
		key:    key,
		ch:     ch,
	}
	s.mu.Unlock()
	return ret, nil
}

func (s *ScopedStateManager) OpenWriter(ctx context.Context, key *fnpb.StateKey, wt writeTypeEnum) (io.Writer, error) {
	ch, err := s.open(ctx)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.Errorf("instruction %v no longer processing", s.instID)
	}
	ret := &stateKeyWriter{
		instID:    s.instID,
		key:       key,
		ch:        ch,
		writeType: wt,
	}
	s.mu.Unlock()
	return ret, nil
}

func (s *ScopedStateManager) open(ctx context.Context) (*StateChannel, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.Errorf("instruction %v no longer processing", s.instID)
	}
	localMgr := s.mgr
	s.mu.Unlock()

	return localMgr.Open(ctx, s.url) // don't hold lock over potentially slow operation
}

// Close closes all open readers.
func (s *ScopedStateManager) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mgr = nil
	s.mu.Unlock()
	return nil
}

type stateKeyReader struct {
	instID instructionID
	key    *fnpb.StateKey

	token []byte
	buf   []byte
	eof   bool

	ch     *StateChannel
	closed bool
	mu     sync.Mutex
}

type stateKeyWriter struct {
	instID    instructionID
	key       *fnpb.StateKey
	writeType writeTypeEnum

	ch *StateChannel
	mu sync.Mutex
}

// TODO: Enable State Backed Iterables, and User State API.
//
// func newRunnerReader(ch *StateChannel, instID instructionID, k []byte) *stateKeyReader {
// 	key := &fnpb.StateKey{
// 		Type: &fnpb.StateKey_Runner_{
// 			Runner: &fnpb.StateKey_Runner{
// 				Key: k,
// 			},
// 		},
// 	}
// 	return &stateKeyReader{
// 		instID: instID,
// 		key:    key,
// 		ch:     ch,
// 	}
// }

// func newBagUserStateReader(ch *StateChannel, id StreamID, instID instructionID, userStateID string, k []byte, w []byte) *stateKeyReader {
// 	key := &fnpb.StateKey{
// 		Type: &fnpb.StateKey_BagUserState_{
// 			BagUserState: &fnpb.StateKey_BagUserState{
// 				TransformId: id.PtransformID,
// 				UserStateId: userStateID,
// 				Window:      w,
// 				Key:         k,
// 			},
// 		},
// 	}
// 	return &stateKeyReader{
// 		instID: instID,
// 		key:    key,
// 		ch:     ch,
// 	}
// }

// func newBagUserStateWriter(ch *StateChannel, id StreamID, instID instructionID, userStateID string, k []byte, w []byte, wt writeTypeEnum) *stateKeyWriter {
// 	key := &fnpb.StateKey{
// 		Type: &fnpb.StateKey_BagUserState_{
// 			BagUserState: &fnpb.StateKey_BagUserState{
// 				TransformId: id.PtransformID,
// 				UserStateId: userStateID,
// 				Window:      w,
// 				Key:         k,
// 			},
// 		},
// 	}
// 	return &stateKeyWriter{
// 		instID:    instID,
// 		key:       key,
// 		ch:        ch,
// 		writeType: wt,
// 	}
// }

// func newMultimapUserStateReader(ch *StateChannel, id StreamID, instID instructionID, userStateID string, k []byte, w []byte, mk []byte) *stateKeyReader {
// 	key := &fnpb.StateKey{
// 		Type: &fnpb.StateKey_MultimapUserState_{
// 			MultimapUserState: &fnpb.StateKey_MultimapUserState{
// 				TransformId: id.PtransformID,
// 				UserStateId: userStateID,
// 				Window:      w,
// 				Key:         k,
// 				MapKey:      mk,
// 			},
// 		},
// 	}
// 	return &stateKeyReader{
// 		instID: instID,
// 		key:    key,
// 		ch:     ch,
// 	}
// }

// func newMultimapUserStateWriter(ch *StateChannel, id StreamID, instID instructionID, userStateID string, k []byte, w []byte, mk []byte, wt writeTypeEnum) *stateKeyWriter {
// 	key := &fnpb.StateKey{
// 		Type: &fnpb.StateKey_MultimapUserState_{
// 			MultimapUserState: &fnpb.StateKey_MultimapUserState{
// 				TransformId: id.PtransformID,
// 				UserStateId: userStateID,
// 				Window:      w,
// 				Key:         k,
// 				MapKey:      mk,
// 			},
// 		},
// 	}
// 	return &stateKeyWriter{
// 		instID:    instID,
// 		key:       key,
// 		ch:        ch,
// 		writeType: wt,
// 	}
// }

// func newMultimapKeysUserStateReader(ch *StateChannel, id StreamID, instID instructionID, userStateID string, k []byte, w []byte) *stateKeyReader {
// 	key := &fnpb.StateKey{
// 		Type: &fnpb.StateKey_MultimapKeysUserState_{
// 			MultimapKeysUserState: &fnpb.StateKey_MultimapKeysUserState{
// 				TransformId: id.PtransformID,
// 				UserStateId: userStateID,
// 				Window:      w,
// 				Key:         k,
// 			},
// 		},
// 	}
// 	return &stateKeyReader{
// 		instID: instID,
// 		key:    key,
// 		ch:     ch,
// 	}
// }

// func newMultimapKeysUserStateWriter(ch *StateChannel, id StreamID, instID instructionID, userStateID string, k []byte, w []byte, wt writeTypeEnum) *stateKeyWriter {
// 	key := &fnpb.StateKey{
// 		Type: &fnpb.StateKey_MultimapKeysUserState_{
// 			MultimapKeysUserState: &fnpb.StateKey_MultimapKeysUserState{
// 				TransformId: id.PtransformID,
// 				UserStateId: userStateID,
// 				Window:      w,
// 				Key:         k,
// 			},
// 		},
// 	}
// 	return &stateKeyWriter{
// 		instID:    instID,
// 		key:       key,
// 		ch:        ch,
// 		writeType: wt,
// 	}
// }

type NextBuffer interface {
	NextBuf() ([]byte, error)
	Close() error
}

func (r *stateKeyReader) NextBuf() ([]byte, error) {
	if r.eof {
		return nil, io.EOF
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("side input closed")
	}
	localChannel := r.ch
	r.mu.Unlock()

	req := &fnpb.StateRequest{
		// Id: set by StateChannel
		InstructionId: string(r.instID),
		StateKey:      r.key,
		Request: &fnpb.StateRequest_Get{
			Get: &fnpb.StateGetRequest{
				ContinuationToken: r.token,
			},
		},
	}
	resp, err := localChannel.Send(req)
	if err != nil {
		r.Close()
		return nil, err
	}
	get := resp.GetGet()
	if get == nil { // no data associated with this segment.
		r.eof = true
		if err := r.Close(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	r.token = get.GetContinuationToken()

	if r.token == nil {
		r.eof = true // no token == this is the last segment.
	}

	return get.GetData(), nil
}

func (r *stateKeyReader) Read(buf []byte) (int, error) {
	if r.buf == nil {
		if r.eof {
			return 0, io.EOF
		}

		// Buffer empty. Get next segment.

		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return 0, errors.New("side input closed")
		}
		localChannel := r.ch
		r.mu.Unlock()

		req := &fnpb.StateRequest{
			// Id: set by StateChannel
			InstructionId: string(r.instID),
			StateKey:      r.key,
			Request: &fnpb.StateRequest_Get{
				Get: &fnpb.StateGetRequest{
					ContinuationToken: r.token,
				},
			},
		}
		resp, err := localChannel.Send(req)
		if err != nil {
			r.Close()
			return 0, err
		}
		get := resp.GetGet()
		if get == nil { // no data associated with this segment.
			r.eof = true
			if err := r.Close(); err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		r.token = get.GetContinuationToken()
		r.buf = get.GetData()

		if r.token == nil {
			r.eof = true // no token == this is the last segment.
		}
	}

	n := copy(buf, r.buf)

	switch {
	case n == 0 && len(buf) != 0 && r.eof:
		// If no data was copied, and this is the last segment anyway, return EOF now.
		// This prevent spurious zero elements.
		r.buf = nil
		if err := r.Close(); err != nil {
			return 0, err
		}
		return 0, io.EOF
	case len(r.buf) == n:
		r.buf = nil
	default:
		r.buf = r.buf[n:]
	}
	return n, nil
}

func (r *stateKeyReader) Close() error {
	r.mu.Lock()
	r.closed = true
	r.ch = nil // StateChannels might be re-used if they're ok, so don't close them here.
	r.mu.Unlock()
	return nil
}

func (r *stateKeyWriter) Write(buf []byte) (int, error) {
	r.mu.Lock()
	localChannel := r.ch
	r.mu.Unlock()

	var req *fnpb.StateRequest
	switch r.writeType {
	case StateWriteAppend:
		req = &fnpb.StateRequest{
			// Id: set by StateChannel
			InstructionId: string(r.instID),
			StateKey:      r.key,
			Request: &fnpb.StateRequest_Append{
				Append: &fnpb.StateAppendRequest{
					Data: buf,
				},
			},
		}
	case StateWriteClear:
		req = &fnpb.StateRequest{
			// ID: set by StateChannel
			InstructionId: string(r.instID),
			StateKey:      r.key,
			Request: &fnpb.StateRequest_Clear{
				Clear: &fnpb.StateClearRequest{},
			},
		}
	default:
		return 0, errors.Errorf("Unknown write type %v", r.writeType)
	}

	_, err := localChannel.Send(req)
	if err != nil {
		return 0, err
	}
	return len(buf), nil
}

// StateChannelManager manages data channels over the State API. A fixed number of channels
// are generally used, each managing multiple logical byte streams. Thread-safe.
type StateChannelManager struct {
	ports map[string]*StateChannel
	mu    sync.Mutex
}

// Open opens a R/W StateChannel over the given port.
func (m *StateChannelManager) Open(ctx context.Context, url string) (*StateChannel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ports == nil {
		m.ports = make(map[string]*StateChannel)
	}
	if con, ok := m.ports[url]; ok {
		return con, nil
	}

	ch, err := newStateChannel(ctx, url)
	if err != nil {
		return nil, err
	}
	m.ports[url] = ch
	return ch, nil
}

type stateClient interface {
	Send(*fnpb.StateRequest) error
	Recv() (*fnpb.StateResponse, error)
}

// StateChannel manages state transactions over a single gRPC connection.
// It does not need to track readers and writers as carefully as the
// DataChannel, because the state protocol is request-based.
type StateChannel struct {
	id     string
	client stateClient

	requests      chan *fnpb.StateRequest
	nextRequestNo int32

	responses map[string]chan<- *fnpb.StateResponse
	mu        sync.Mutex

	cancelFn  context.CancelFunc
	closedErr error
	DoneCh    <-chan struct{}
}

func (c *StateChannel) terminateStreamOnError(err error) {
	c.mu.Lock()
	c.closedErr = err
	// Cancelling context after forcing recreation to ensure closedErr is set.
	c.cancelFn()
	c.mu.Unlock()
}

func newStateChannel(ctx context.Context, url string) (*StateChannel, error) {
	ctx, cancelFn := context.WithCancel(ctx)
	cc, err := Dial(ctx, url, 15*time.Second)
	if err != nil {
		cancelFn()
		return nil, errors.Wrapf(err, "failed to connect to state service %v", url)
	}
	client, err := fnpb.NewBeamFnStateClient(cc).State(ctx)
	if err != nil {
		cc.Close()
		cancelFn()
		return nil, errors.Wrapf(err, "failed to create state client %v", url)
	}
	return makeStateChannel(ctx, url, client, func() {
		cc.Close()
		cancelFn()
	}), nil
}

func makeStateChannel(ctx context.Context, id string, client stateClient, cancelFn context.CancelFunc) *StateChannel {
	ret := &StateChannel{
		id:        id,
		client:    client,
		requests:  make(chan *fnpb.StateRequest, 10),
		responses: make(map[string]chan<- *fnpb.StateResponse),
		cancelFn:  cancelFn,
		DoneCh:    ctx.Done(),
	}
	go ret.read(ctx)
	go ret.write(ctx)

	return ret
}

func (c *StateChannel) read(ctx context.Context) {
	for {
		// Closing the context will have an error return from this call.
		msg, err := c.client.Recv()
		if err != nil {
			c.terminateStreamOnError(err)
			if err == io.EOF {
				slog.WarnContext(ctx, "StateChannel.read: closed", "id", c.id)
				return
			}
			slog.ErrorContext(ctx, "StateChannel.read bad: %v", "id", c.id, "error", err)
			return
		}

		c.mu.Lock()
		ch, ok := c.responses[msg.Id]
		delete(c.responses, msg.Id)
		c.mu.Unlock()
		if !ok {
			// This can happen if Send returns an error that write handles, but
			// the message was actually sent.
			slog.ErrorContext(ctx, "StateChannel[%v].read: no consumer for state response: %v", c.id, prototext.Format(msg))
			continue
		}

		select {
		case ch <- msg:
			// ok
		default:
			panic(fmt.Sprintf("StateChannel[%v].read: failed to consume state response: %v", c.id, prototext.Format(msg)))
		}
	}
}

func (c *StateChannel) write(ctx context.Context) {
	var err error
	var id string
	for {
		var req *fnpb.StateRequest
		select {
		case req = <-c.requests:
		case <-c.DoneCh: // Close the goroutine on context cancel.
			return
		}
		err = c.client.Send(req)
		if err != nil {
			id = req.Id
			break // non-nil errors mean the stream is broken and can't be re-used.
		}
	}

	if err == io.EOF {
		slog.WarnContext(ctx, "StateChannel.write EOF on send; fetching real error", "id", c.id)
		err = nil
		for err == nil {
			// Per GRPC stream documentation, if there's an EOF, we must call Recv
			// until a non-nil error is returned, to ensure resources are cleaned up.
			// https://pkg.go.dev/google.golang.org/grpc#ClientConn.NewStream
			_, err = c.client.Recv()
		}
	}
	slog.ErrorContext(ctx, "StateChannel.write error on send", "id", c.id, "error", err)

	// Failed to send. Return error & unblock Send.
	c.mu.Lock()
	ch, ok := c.responses[id]
	delete(c.responses, id)
	c.mu.Unlock()
	// Clean up everything else, this stream is done.
	c.terminateStreamOnError(err)

	if ok {
		ch <- &fnpb.StateResponse{Id: id, Error: fmt.Sprintf("StateChannel[%v].write failed to send: %v", c.id, err)}
	}
}

// Send sends a state request and returns the response.
func (c *StateChannel) Send(req *fnpb.StateRequest) (*fnpb.StateResponse, error) {
	id := fmt.Sprintf("r%v", atomic.AddInt32(&c.nextRequestNo, 1))
	req.Id = id

	ch := make(chan *fnpb.StateResponse, 1)
	c.mu.Lock()
	if c.closedErr != nil {
		defer c.mu.Unlock()
		return nil, errors.Wrapf(c.closedErr, "StateChannel[%v].Send(%v): channel closed due to: %v", c.id, id, c.closedErr)
	}
	c.responses[id] = ch
	c.mu.Unlock()

	c.requests <- req

	var resp *fnpb.StateResponse
	select {
	case resp = <-ch:
	case <-c.DoneCh:
		c.mu.Lock()
		defer c.mu.Unlock()
		return nil, errors.Wrapf(c.closedErr, "StateChannel[%v].Send(%v): context canceled", c.id, id)
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return resp, nil
}
