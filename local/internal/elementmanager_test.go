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
	"container/heap"
	"fmt"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
)

func TestElementHeap(t *testing.T) {
	elements := elementHeap{
		element{timestamp: mtime.EndOfGlobalWindowTime},
		element{timestamp: mtime.MaxTimestamp},
		element{timestamp: 3},
		element{timestamp: mtime.MinTimestamp},
		element{timestamp: 2},
		element{timestamp: mtime.ZeroTimestamp},
		element{timestamp: 1},
	}
	heap.Init(&elements)

	if got, want := elements.Len(), len(elements); got != want {
		t.Errorf("elements.Len() = %v, want %v", got, want)
	}
	if got, want := elements[0].timestamp, mtime.MinTimestamp; got != want {
		t.Errorf("elements[0].timestamp = %v, want %v", got, want)
	}

	wanted := []mtime.Time{mtime.MinTimestamp, mtime.ZeroTimestamp, 1, 2, 3, mtime.EndOfGlobalWindowTime, mtime.MaxTimestamp}
	for i, want := range wanted {
		if got := heap.Pop(&elements).(element).timestamp; got != want {
			t.Errorf("[%d] heap.Pop(&elements).(element).timestamp = %v, want %v", i, got, want)
		}
	}
}

func TestStageState_minPendingTimestamp(t *testing.T) {

	newState := func() *stageState {
		return makeStageState("test", []string{"testInput"}, nil, []string{"testOutput"})
	}
	t.Run("noElements", func(t *testing.T) {
		ss := newState()
		got := ss.minPendingTimestamp()
		want := mtime.MaxTimestamp
		if got != want {
			t.Errorf("ss.minPendingTimestamp() = %v, want %v", got, want)
		}
	})

	want := mtime.ZeroTimestamp - 20
	t.Run("onlyPending", func(t *testing.T) {
		ss := newState()
		ss.pending = elementHeap{
			element{timestamp: mtime.EndOfGlobalWindowTime},
			element{timestamp: mtime.MaxTimestamp},
			element{timestamp: 3},
			element{timestamp: want},
			element{timestamp: 2},
			element{timestamp: mtime.ZeroTimestamp},
			element{timestamp: 1},
		}
		heap.Init(&ss.pending)

		got := ss.minPendingTimestamp()
		if got != want {
			t.Errorf("ss.minPendingTimestamp() = %v, want %v", got, want)
		}
	})

	t.Run("onlyInProgress", func(t *testing.T) {
		ss := newState()
		ss.inprogress = map[string]elements{
			"a": {
				es: []element{
					{timestamp: mtime.EndOfGlobalWindowTime},
					{timestamp: mtime.MaxTimestamp},
				},
				minTimestamp: mtime.EndOfGlobalWindowTime,
			},
			"b": {
				es: []element{
					{timestamp: 3},
					{timestamp: want},
					{timestamp: 2},
					{timestamp: 1},
				},
				minTimestamp: want,
			},
			"c": {
				es: []element{
					{timestamp: mtime.ZeroTimestamp},
				},
				minTimestamp: mtime.ZeroTimestamp,
			},
		}

		got := ss.minPendingTimestamp()
		if got != want {
			t.Errorf("ss.minPendingTimestamp() = %v, want %v", got, want)
		}
	})

	t.Run("minInPending", func(t *testing.T) {
		ss := newState()
		ss.pending = elementHeap{
			{timestamp: 3},
			{timestamp: want},
			{timestamp: 2},
			{timestamp: 1},
		}
		heap.Init(&ss.pending)
		ss.inprogress = map[string]elements{
			"a": {
				es: []element{
					{timestamp: mtime.EndOfGlobalWindowTime},
					{timestamp: mtime.MaxTimestamp},
				},
				minTimestamp: mtime.EndOfGlobalWindowTime,
			},
			"c": {
				es: []element{
					{timestamp: mtime.ZeroTimestamp},
				},
				minTimestamp: mtime.ZeroTimestamp,
			},
		}

		got := ss.minPendingTimestamp()
		if got != want {
			t.Errorf("ss.minPendingTimestamp() = %v, want %v", got, want)
		}
	})
	t.Run("minInProgress", func(t *testing.T) {
		ss := newState()
		ss.pending = elementHeap{
			{timestamp: 3},
			{timestamp: 2},
			{timestamp: 1},
		}
		heap.Init(&ss.pending)
		ss.inprogress = map[string]elements{
			"a": {
				es: []element{
					{timestamp: want},
					{timestamp: mtime.EndOfGlobalWindowTime},
					{timestamp: mtime.MaxTimestamp},
				},
				minTimestamp: want,
			},
			"c": {
				es: []element{
					{timestamp: mtime.ZeroTimestamp},
				},
				minTimestamp: mtime.ZeroTimestamp,
			},
		}

		got := ss.minPendingTimestamp()
		if got != want {
			t.Errorf("ss.minPendingTimestamp() = %v, want %v", got, want)
		}
	})
}

func TestStageState_getUpstreamWatermark(t *testing.T) {
	impulse := makeStageState("impulse", nil, nil, []string{"output"})
	_, up := impulse.UpstreamWatermark()
	if got, want := up, mtime.MaxTimestamp; got != want {
		t.Errorf("impulse.getUpstreamWatermark() = %v, want %v", got, want)
	}

	dofn := makeStageState("dofn", []string{"input"}, nil, []string{"output"})
	dofn.updateUpstreamWatermark("input", 42)

	_, up = dofn.UpstreamWatermark()
	if got, want := up, mtime.Time(42); got != want {
		t.Errorf("dofn.getUpstreamWatermark() = %v, want %v", got, want)
	}

	flatten := makeStageState("flatten", []string{"a", "b", "c"}, nil, []string{"output"})
	flatten.updateUpstreamWatermark("a", 50)
	flatten.updateUpstreamWatermark("b", 42)
	flatten.updateUpstreamWatermark("c", 101)
	_, up = flatten.UpstreamWatermark()
	if got, want := up, mtime.Time(42); got != want {
		t.Errorf("flatten.getUpstreamWatermark() = %v, want %v", got, want)
	}
}

func TestStageState_updateWatermarks(t *testing.T) {
	inputCol := "testInput"
	outputCol := "testOutput"
	newState := func() (*stageState, *stageState, *elementManager) {
		underTest := makeStageState("underTest", []string{inputCol}, nil, []string{outputCol})
		outStage := makeStageState("outStage", []string{outputCol}, nil, nil)
		em := &elementManager{
			consumers: map[string][]string{
				inputCol:  {underTest.ID},
				outputCol: {outStage.ID},
			},
			stages: map[string]*stageState{
				outStage.ID:  outStage,
				underTest.ID: underTest,
			},
		}
		return underTest, outStage, em
	}

	tests := []struct {
		name                                  string
		initInput, initOutput                 mtime.Time
		upstream, minPending, minStateHold    mtime.Time
		wantInput, wantOutput, wantDownstream mtime.Time
	}{
		{
			name:           "initialized",
			initInput:      mtime.MinTimestamp,
			initOutput:     mtime.MinTimestamp,
			upstream:       mtime.MinTimestamp,
			minPending:     mtime.EndOfGlobalWindowTime,
			minStateHold:   mtime.EndOfGlobalWindowTime,
			wantInput:      mtime.MinTimestamp, // match default
			wantOutput:     mtime.MinTimestamp, // match upstream
			wantDownstream: mtime.MinTimestamp, // match upstream
		}, {
			name:           "upstream",
			initInput:      mtime.MinTimestamp,
			initOutput:     mtime.MinTimestamp,
			upstream:       mtime.ZeroTimestamp,
			minPending:     mtime.EndOfGlobalWindowTime,
			minStateHold:   mtime.EndOfGlobalWindowTime,
			wantInput:      mtime.ZeroTimestamp, // match upstream
			wantOutput:     mtime.ZeroTimestamp, // match upstream
			wantDownstream: mtime.ZeroTimestamp, // match upstream
		}, {
			name:           "useMinPending",
			initInput:      mtime.MinTimestamp,
			initOutput:     mtime.MinTimestamp,
			upstream:       mtime.ZeroTimestamp,
			minPending:     -20,
			minStateHold:   mtime.EndOfGlobalWindowTime,
			wantInput:      -20, // match minPending
			wantOutput:     -20, // match minPending
			wantDownstream: -20, // match minPending
		}, {
			name:           "useStateHold",
			initInput:      mtime.MinTimestamp,
			initOutput:     mtime.MinTimestamp,
			upstream:       mtime.ZeroTimestamp,
			minPending:     -20,
			minStateHold:   -30,
			wantInput:      -20, // match minPending
			wantOutput:     -30, // match state hold
			wantDownstream: -30, // match state hold
		}, {
			name:           "noAdvance",
			initInput:      20,
			initOutput:     30,
			upstream:       mtime.MinTimestamp,
			wantInput:      20,                 // match original input
			wantOutput:     30,                 // match original output
			wantDownstream: mtime.MinTimestamp, // not propagated
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ss, outStage, em := newState()
			ss.input = test.initInput
			ss.output = test.initOutput
			ss.updateUpstreamWatermark(inputCol, test.upstream)
			ss.updateWatermarks(test.minPending, test.minStateHold, em)
			if got, want := ss.input, test.wantInput; got != want {
				pcol, up := ss.UpstreamWatermark()
				t.Errorf("ss.updateWatermarks(%v,%v); ss.input = %v, want %v (upstream %v %v)", test.minPending, test.minStateHold, got, want, pcol, up)
			}
			if got, want := ss.output, test.wantOutput; got != want {
				pcol, up := ss.UpstreamWatermark()
				t.Errorf("ss.updateWatermarks(%v,%v); ss.output = %v, want %v (upstream %v %v)", test.minPending, test.minStateHold, got, want, pcol, up)
			}
			_, up := outStage.UpstreamWatermark()
			if got, want := up, test.wantDownstream; got != want {
				t.Errorf("outStage.getUpstreamWatermark() = %v, want %v", got, want)
			}
		})
	}

}

func TestElementManager(t *testing.T) {
	t.Run("impulse", func(t *testing.T) {
		em := newElementManager()
		em.addStage("impulse", nil, nil, []string{"output"})
		em.addStage("dofn", []string{"output"}, nil, nil)

		em.Impulse("impulse")

		if got, want := em.stages["impulse"].OutputWatermark(), mtime.MaxTimestamp; got != want {
			t.Fatalf("impulse.OutputWatermark() = %v, want %v", got, want)
		}

		var i int
		ch := em.Bundles(func() string {
			defer func() { i++ }()
			return fmt.Sprintf("%v", i)
		})
		b, ok := <-ch
		if !ok {
			t.Error("Bundles channel unexpectedly closed")
		}
		em.PersistBundle("dofn", b.bundleID, nil, tentativeData{}, PColInfo{}, nil)

		_, ok = <-ch
		if ok {
			t.Error("Bundles channel expected to be closed")
		}
		// for b := range ch {
		// 	t.Log("bundle received!", b)
		// 	em.PersistBundle("dofn", b.bundleID, nil, tentativeData{})
		// }
		// if i > 1 {
		// 	t.Errorf("got %v bundles, want 1", i)
		// }

		// select {
		// case b, ok := <-ch:
		// 	if ok {
		// 		t.Fatalf("got bundle %v, want closed channel", b)
		// 	}
		// default:
		// 	t.Fatalf("bundles channel not closed")
		// }
	})
}

// TODO: add tests for persisting bundles
// TODO: add tests for producing bundles.
