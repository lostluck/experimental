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

package internal

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

// separate_test.go is retains structures and tests to ensure the runner can
// perform separation, and terminate checkpoints.

func init() {
	register.Function1x1(allSentinel)
}

// allSentinel indicates that all elements are sentinels.
func allSentinel(v beam.T) bool {
	return true
}

// TestSeparation validates that the runner is able to split
// elements in time and space. Beam has a few mechanisms to
// do this.
//
// First is channel splits, where a slowly processing
// bundle might have it's remaining buffered elements truncated
// so they can be processed by a another bundle,
// possibly simultaneously.
//
// Second is sub element splitting, where a single element
// in an SDF might be split into smaller restrictions.
//
// Third with Checkpointing or ProcessContinuations,
// a User DoFn may decide to defer processing of an element
// until later, permitting a bundle to terminate earlier,
// delaying processing.
//
// All these may be tested locally or in process with a small
// server the DoFns can connect to. This can then indicate which
// elements, or positions are considered "sentinels".
//
// When a sentinel is to be processed, instead the DoFn blocks.
// The goal for Splitting tests is to succeed only when all
// sentinels are blocking waiting to be processed.
// This indicates the runner has "separated" the sentinels, hence
// the name "separation harness tests".
//
// Delayed Process Continuations can be similiarly tested,
// as this emulates external processing servers anyway.
// It's much simpler though, as the request is to determine if
// a given element should be delayed or not. This could be used
// for arbitrarily complex splitting patterns, as desired.
func TestSeparation(t *testing.T) {
	initRunner(t)

	ws := &Watchers{}
	ws.initRPCServer()

	tests := []struct {
		name     string
		pipeline func(s beam.Scope)
		metrics  func(t *testing.T, pr beam.PipelineResult)
	}{
		{
			name: "ProcessContinuations_globalWindow",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				out := beam.ParDo(s, &sepHarnessSdfStream{
					Base: sepHarnessBase{
						WatcherID:         ws.newWatcher(3),
						Sleep:             time.Second,
						IsSentinelEncoded: beam.EncodedFunc{Fn: reflectx.MakeFunc(allSentinel)},
						LocalService:      ws.serviceAddress,
					},
					RestSize: 10,
				}, imp)
				passert.Count(s, out, "num ints", 10)
			},
		},
	}

	// TODO: Channel Splits
	// TODO: SubElement/dynamic splits.

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()
			test.pipeline(s)
			pr, err := executeWithT(context.Background(), t, p)
			if err != nil {
				t.Fatal(err)
			}
			if test.metrics != nil {
				test.metrics(t, pr)
			}
		})
	}
}

// Watcher is an instance of the counters.
type watcher struct {
	id                         int
	mu                         sync.Mutex
	sentinelCount, sentinelCap int
	once                       sync.Once
}

// Watchers is a "net/rpc" service.
type Watchers struct {
	mu             sync.Mutex
	nextID         int
	lookup         map[int]*watcher
	serviceOnce    sync.Once
	serviceAddress string
}

// Args is the set of parameters to the watchers RPC methdos.
type Args struct {
	WatcherID int
}

// Block is called once per sentinel, to indicate it will block
// until all sentinels are blocked.
func (ws *Watchers) Block(args *Args, _ *bool) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	w, ok := ws.lookup[args.WatcherID]
	if !ok {
		return fmt.Errorf("no watcher with id %v", args.WatcherID)
	}
	w.mu.Lock()
	w.sentinelCount++
	w.mu.Unlock()
	return nil
}

// Check returns whether the sentinels are unblocked or not.
func (ws *Watchers) Check(args *Args, unblocked *bool) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	w, ok := ws.lookup[args.WatcherID]
	if !ok {
		return fmt.Errorf("no watcher with id %v", args.WatcherID)
	}
	w.mu.Lock()
	*unblocked = w.sentinelCount >= w.sentinelCap
	w.mu.Unlock()
	V(2).Logf("sentinel target for watcher%d is %d/%d. unblocked=%v", args.WatcherID, w.sentinelCount, w.sentinelCap, *unblocked)
	return nil
}

// Delay returns whether the sentinels shoudld delay.
// This increments the sentinel cap, and returns unblocked.
// Intended to validate ProcessContinuation behavior.
func (ws *Watchers) Delay(args *Args, delay *bool) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	w, ok := ws.lookup[args.WatcherID]
	if !ok {
		return fmt.Errorf("no watcher with id %v", args.WatcherID)
	}
	w.mu.Lock()
	w.sentinelCount++
	// Delay as long as the sentinel count is under the cap.
	*delay = w.sentinelCount < w.sentinelCap
	w.mu.Unlock()
	V(2).Logf("sentinel target for watcher%d is %d/%d. delay=%v", args.WatcherID, w.sentinelCount, w.sentinelCap, *delay)
	return nil
}

func (ws *Watchers) initRPCServer() {
	ws.serviceOnce.Do(func() {
		l, err := net.Listen("tcp", ":0")
		if err != nil {
			panic(err)
		}
		rpc.Register(ws)
		rpc.HandleHTTP()
		go http.Serve(l, nil)
		ws.serviceAddress = l.Addr().String()
	})
}

// newWatcher starts an rpc server to maange state for watching for
// sentinels across local machines.
func (ws *Watchers) newWatcher(sentinelCap int) int {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.initRPCServer()
	if ws.lookup == nil {
		ws.lookup = map[int]*watcher{}
	}
	w := &watcher{id: ws.nextID, sentinelCap: sentinelCap}
	ws.nextID++
	ws.lookup[w.id] = w
	return w.id
}

// sepHarnessBase contains fields and functions that are shared by all
// versions of the separation harness.
type sepHarnessBase struct {
	WatcherID         int
	Sleep             time.Duration
	IsSentinelEncoded beam.EncodedFunc
	LocalService      string
}

// One connection per binary.
var (
	sepClientOnce sync.Once
	sepClient     *rpc.Client
	sepClientMu   sync.Mutex
	sepWaitMap    map[int]chan struct{}
)

func (fn *sepHarnessBase) setup() error {
	sepClientMu.Lock()
	defer sepClientMu.Unlock()
	sepClientOnce.Do(func() {
		client, err := rpc.DialHTTP("tcp", fn.LocalService)
		if err != nil {
			V(0).Fatalf("dialing sentinels server %v: %v", fn.LocalService, err)
		}
		sepClient = client
		sepWaitMap = map[int]chan struct{}{}
	})

	// Check if there's alreaedy a local channel for this id, and if not
	// start a watcher goroutine to poll and unblock the harness when
	// the expected number of ssentinels is reached.
	if _, ok := sepWaitMap[fn.WatcherID]; !ok {
		return nil
	}
	// We need a channel to block on for this watcherID
	// We use a channel instead of a wait group since the finished
	// count is hosted in a different process.
	c := make(chan struct{})
	sepWaitMap[fn.WatcherID] = c
	go func(id int, c chan struct{}) {
		for {
			time.Sleep(time.Second * 1) // Check counts every second.
			sepClientMu.Lock()
			var unblock bool
			err := sepClient.Call("Watchers.Check", &Args{WatcherID: id}, &unblock)
			if err != nil {
				V(0).Fatalf("Watchers.Check error sentinels server %v: %v", fn.LocalService, err)
			}
			if unblock {
				close(c) // unblock all the local waiters.
				V(2).Logf("sentinel target for watcher %d met, unblocking", id)
				sepClientMu.Unlock()
				return
			}
			V(2).Logf("sentinel target for watcher %d not met", id)
			sepClientMu.Unlock()
		}
	}(fn.WatcherID, c)
	return nil
}

func (fn *sepHarnessBase) block() {
	sepClientMu.Lock()
	var ignored bool
	err := sepClient.Call("Watchers.Block", &Args{WatcherID: fn.WatcherID}, &ignored)
	if err != nil {
		V(0).Fatalf("Watchers.Block error %v", err)
	}
	c := sepWaitMap[fn.WatcherID]
	sepClientMu.Unlock()

	// Block until the watcher closes the channel.
	select {
	case <-c:
	}
}

// delay inform the DoFn whether or not to return a delayed Processing continuation for this position.
func (fn *sepHarnessBase) delay() bool {
	sepClientMu.Lock()
	defer sepClientMu.Unlock()
	var delay bool
	err := sepClient.Call("Watchers.Delay", &Args{WatcherID: fn.WatcherID}, &delay)
	if err != nil {
		V(0).Fatalf("Watchers.Delay error %v", err)
	}
	return delay
}

// sepHarness is a simple DoFn that blocks when reaching a sentinel.
// It's useful for testing blocks on channel splits.
type sepHarness struct {
	Base sepHarnessBase
}

func (fn *sepHarness) Setup() error {
	return fn.Base.setup()
}

func (fn *sepHarness) ProcessEleemnt(v beam.T) beam.T {
	if fn.Base.IsSentinelEncoded.Fn.Call([]any{v})[0].(bool) {
		V(2).Logf("%v is a sentinel, blocking", v)
		fn.Base.block()
		V(2).Logf("%v unblocked", v)
	} else {
		time.Sleep(fn.Base.Sleep)
	}
	return v
}

type sepHarnessSdf struct {
	Base     sepHarnessBase
	RestSize int64
}

func (fn *sepHarnessSdf) Setup() error {
	return fn.Base.setup()
}

func (fn *sepHarnessSdf) CreateInitialRestriction(v beam.T) offsetrange.Restriction {
	return offsetrange.Restriction{Start: 0, End: fn.RestSize}
}

func (fn *sepHarnessSdf) SplitRestriction(v beam.T, r offsetrange.Restriction) []offsetrange.Restriction {
	return r.EvenSplits(2)
}

func (fn *sepHarnessSdf) RestrictionSize(v beam.T, r offsetrange.Restriction) float64 {
	return r.Size()
}

func (fn *sepHarnessSdf) CreateTracker(r offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(r))
}

func (fn *sepHarnessSdf) ProcessElement(rt *sdf.LockRTracker, v beam.T, emit func(beam.T)) {
	i := rt.GetRestriction().(offsetrange.Restriction).Start
	for rt.TryClaim(i) {
		if fn.Base.IsSentinelEncoded.Fn.Call([]any{i, v})[0].(bool) {
			V(2).Logf("%v is a sentinel, blocking", v)
			fn.Base.block()
			V(2).Logf("%v unblocked", v)
		} else {
			time.Sleep(fn.Base.Sleep)
		}
		emit(v)
		i++
	}
}

func init() {
	register.DoFn3x1[*sdf.LockRTracker, beam.T, func(beam.T), sdf.ProcessContinuation]((*sepHarnessSdfStream)(nil))
}

type sepHarnessSdfStream struct {
	Base     sepHarnessBase
	RestSize int64
}

func (fn *sepHarnessSdfStream) Setup() error {
	return fn.Base.setup()
}

func (fn *sepHarnessSdfStream) CreateInitialRestriction(v beam.T) offsetrange.Restriction {
	return offsetrange.Restriction{Start: 0, End: fn.RestSize}
}

func (fn *sepHarnessSdfStream) SplitRestriction(v beam.T, r offsetrange.Restriction) []offsetrange.Restriction {
	return r.EvenSplits(2)
}

func (fn *sepHarnessSdfStream) RestrictionSize(v beam.T, r offsetrange.Restriction) float64 {
	return r.Size()
}

func (fn *sepHarnessSdfStream) CreateTracker(r offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(r))
}

func (fn *sepHarnessSdfStream) ProcessElement(rt *sdf.LockRTracker, v beam.T, emit func(beam.T)) sdf.ProcessContinuation {
	if fn.Base.IsSentinelEncoded.Fn.Call([]any{v})[0].(bool) {
		if fn.Base.delay() {
			V(2).Logf("%v is a sentinel, delaying", v)
			return sdf.ResumeProcessingIn(fn.Base.Sleep)
		}
		V(2).Logf("%v is a sentinel and cleared to process", v)
	}
	r := rt.GetRestriction().(offsetrange.Restriction)
	i := r.Start
	V(2).Logf("emitting element to restriction size %v: %v ", r.Size(), r)
	for rt.TryClaim(i) {
		emit(v)
		i++
	}
	return sdf.StopProcessing()
}
