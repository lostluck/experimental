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
	"bytes"
	"container/heap"
	"io"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// There comes a time in every Beam runner's life that they develop a type
// called a Watermark Manager, that becomes the core of the runner.
// Given that handling the watermark is the core of the Beam model, this
// is not super surprising.
//
// Essentially, it needs to track the current watermarks for each pcollection
// and transform/stage. But it's tricky, since the watermarks for the
// PCollections are always relative to transforms/stages.
//
// Key parts:
//
//   - The parallel input's PCollection's watermark is relative to committed consumed
//     elements. That is, the input elements consumed by the transform after a successful
//     bundle, can advance the watermark, based on the minimum of their elements.
//   - An output PCollection's watermark is relative to its producing transform,
//     which relates to *all of it's outputs*.
//
// This means that a PCollection's watermark is the minimum of all it's consuming transforms.
//
// So, the watermark manager needs to track:
// Pending Elements for each stage, along with their windows and timestamps.
// Each transform's view of the watermarks for the PCollections.
//
// Watermarks are always advanced based on consumed input.
type elementManager struct {
	stages map[string]*stageState

	consumers map[string][]string // Map from pcollectionID to stageIDs that consumes them.
}

type stageState struct {
	ID        string
	inputID   string   // PCollection ID of the parallel input
	outputIDs []string // PCollection IDs of outputs to update consumers.

	mu                 sync.Mutex
	upstreamWatermarks sync.Map   // watermark set from inputPCollection's parent.
	input              mtime.Time // input watermark for the parallel input.
	output             mtime.Time // Output watermark for the whole stage

	pending    elementHeap          // pending input elements for this stage that are to be processesd
	inprogress map[string][]element // inprogress elements by active bundles, keyed by bundle
}

// makeStageState produces an initialized stage stage.
func makeStageState(ID string, inputIDs, outputIDs []string) *stageState {
	ss := &stageState{
		ID:        ID,
		outputIDs: outputIDs,

		input:  mtime.MinTimestamp,
		output: mtime.MinTimestamp,
	}

	// Initialize the upstream watermaps to minTime.
	for _, pcol := range inputIDs {
		ss.upstreamWatermarks.Store(pcol, mtime.MinTimestamp)
	}
	if len(inputIDs) == 1 {
		ss.inputID = inputIDs[0]
	}
	// If this is zero, then it's an impulse, and will sort itself out.
	// TODO, add initial pendings here?

	// If this has more than one input, it's a flatten, and we fake the propagation
	// as needed.

	return ss
}

func (ss *stageState) AddPending(newPending []element) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.pending = append(ss.pending, newPending...)
	heap.Init(&ss.pending)
}

// updateUpstreamWatermark is for the parent of the input pcollection
// to call, to update downstream stages with it's current watermark.
// This avoids downstream stages inverting lock orderings from
// calling their parent stage to get their input pcollection's watermark.
func (ss *stageState) updateUpstreamWatermark(pcol string, upstream mtime.Time) {
	// A stage will only have a single upstream watermark, so
	// we simply set this.
	ss.upstreamWatermarks.Store(pcol, &upstream)
}

// getUpstreamWatermark get's the minimum value of all upstream watermarks.
func (ss *stageState) getUpstreamWatermark() mtime.Time {
	upstream := mtime.MaxTimestamp
	ss.upstreamWatermarks.Range(func(_, val any) bool {
		upstream = mtime.Min(upstream, val.(mtime.Time))
		return true
	})
	return upstream
}

type element struct {
	windows   []typex.Window
	timestamp mtime.Time
	pane      typex.PaneInfo

	rawbytes []byte
}

// elementHeap orders elements based on their timestamps
// so we can always find the minimum timestamp of pending elements.
type elementHeap []element

func (h elementHeap) Len() int           { return len(h) }
func (h elementHeap) Less(i, j int) bool { return h[i].timestamp < h[j].timestamp }
func (h elementHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *elementHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(element))
}

func (h *elementHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// PersistBundle updates the watermarks for the stage. Each stage has two
// monotonically increasing watermarks, the input watermark, and the output
// watermark.
//
// MAX(CurrentInputWatermark, MIN(PendingElements, InputPCollectionWatermarks)
// MAX(CurrentOutputWatermark, MIN(InputWatermark, WatermarkHolds))
//
// PersistBundle takes in the stage ID, ID of the bundle associated with the pending
// input elements, and the committed output elements.
func (em *elementManager) PersistBundle(stageID string, bundID string, col2Coders map[string]PColInfo, d tentativeData) {
	stage := em.stages[stageID]
	for output, data := range d.raw {
		info := col2Coders[output]
		var newPending []element
		for _, datum := range data {
			buf := bytes.NewBuffer(datum)
			if len(datum) == 0 {
				logger.Fatalf("zero length data for %v: ", output)
			}
			for {
				var rawBytes bytes.Buffer
				tee := io.TeeReader(buf, &rawBytes)
				ws, et, pn, err := exec.DecodeWindowedValueHeader(info.wDec, tee)
				if err != nil {
					if err == io.EOF {
						break
					}
					logger.Fatalf("error decoding watermarks for %v: %v", output, err)
				}
				// TODO: Optimize unnecessary copies. This is doubleteeing.
				info.eDec(tee)
				newPending = append(newPending,
					element{
						windows:   ws,
						timestamp: et,
						pane:      pn,
						rawbytes:  rawBytes.Bytes(),
					})
			}
		}
		consumers := em.consumers[output]
		for _, sID := range consumers {
			consumer := em.stages[sID]
			consumer.AddPending(newPending)
		}
	}
	// TODO support state/timer watermark holds.
	dummyMinStateHold := mtime.MaxTimestamp
	stage.updateWatermarks(bundID, dummyMinStateHold, em)
}

// updateWatermarks performs the following operations:
//
// Watermark_In'  = MAX(Watermark_In, MIN(U(TS_Pending), U(Watermark_InputPCollection)))
// Watermark_Out' = MAX(Watermark_Out, MIN(Watermark_In', U(StateHold)))
// Watermark_PCollection = Watermark_Out_ProducingPTransform
func (ss *stageState) updateWatermarks(bundID string, minStateHold mtime.Time, em *elementManager) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	// Clear out the inprogress elements associated with the completed bundle.
	delete(ss.inprogress, bundID)

	// PCollection watermarks are based on their parents's output watermark.
	newIn := ss.getUpstreamWatermark()

	// Set the input watermark based on the minimum pending elements,
	// and the current input pcollection watermark.
	minPending := ss.pending[0].timestamp
	if minPending < newIn {
		newIn = minPending
	}

	// If bigger, advance the input watermark.
	if newIn > ss.input {
		// TODO, trigger generation of new bundles?
		ss.input = newIn
	}
	// The output starts with the new input as the basis.
	newOut := ss.input
	if minStateHold < newOut {
		newOut = minStateHold
	}
	// If bigger, advance the output watermark
	if newOut > ss.output {
		// TODO, trigger watermark advancement of consuming stages
		ss.output = newOut
		for _, outputCol := range ss.outputIDs {
			for _, sID := range em.consumers[outputCol] {
				em.stages[sID].updateUpstreamWatermark(outputCol, ss.output)
			}
		}
	}
}
