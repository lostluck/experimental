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

// Package engine handles the operational components of a runner, to
// track elements, watermarks, timers, triggers etc
package engine

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

type Config struct {
	// MaxBundleSize caps the number of elements permitted in a bundle.
	// 0 or less means this is ignored.
	MaxBundleSize int
}

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
type ElementManager struct {
	config Config

	stages map[string]*stageState // The state for each stage.

	consumers     map[string][]string // Map from pcollectionID to stageIDs that consumes them as primary input.
	sideConsumers map[string][]string // Map from pcollectionID to stageIDs that consumes them as side input.

	pcolParents map[string]string // Map from pcollectionID to stageIDs that produce the pcollection.

	refreshCond        sync.Cond   // refreshCond protects the following fields with it's lock, and unblocks bundle scheduling.
	inprogressBundles  set[string] // Active bundleIDs
	watermarkRefreshes set[string] // Scheduled stageID watermark refreshes

	pendingElements sync.WaitGroup // pendingElements counts all unprocessed elements in a job. Jobs with no pending elements terminate successfully.
}

func NewElementManager(config Config) *ElementManager {
	return &ElementManager{
		config:             config,
		stages:             map[string]*stageState{},
		consumers:          map[string][]string{},
		sideConsumers:      map[string][]string{},
		pcolParents:        map[string]string{},
		watermarkRefreshes: set[string]{},
		inprogressBundles:  set[string]{},
		refreshCond:        sync.Cond{L: &sync.Mutex{}},
	}
}

// AddStage adds a stage to this element manager, connecting it's pcollections and
// nodes to the watermark propagation graph.
func (em *ElementManager) AddStage(ID string, inputIDs, sides, outputIDs []string) {
	// logger.Logf("AddStage: %v inputs: %v sides: %v outputs: %v", ID, inputIDs, sides, outputIDs)
	ss := makeStageState(ID, inputIDs, sides, outputIDs)

	em.stages[ss.ID] = ss
	for _, outputIDs := range ss.outputIDs {
		em.pcolParents[outputIDs] = ss.ID
	}
	for _, input := range inputIDs {
		em.consumers[input] = append(em.consumers[input], ss.ID)
	}
	for _, side := range ss.sides {
		em.sideConsumers[side] = append(em.sideConsumers[side], ss.ID)
	}
}

type element struct {
	window    typex.Window
	timestamp mtime.Time
	pane      typex.PaneInfo

	elmBytes []byte
}

type elements struct {
	es           []element
	minTimestamp mtime.Time
}

type PColInfo struct {
	GlobalID string
	WDec     exec.WindowDecoder
	WEnc     exec.WindowEncoder
	EDec     func(io.Reader) []byte
}

// ToData recodes the elements with their approprate windowed value header.
func (es elements) ToData(info PColInfo) [][]byte {
	var ret [][]byte
	for _, e := range es.es {
		var buf bytes.Buffer
		exec.EncodeWindowedValueHeader(info.WEnc, []typex.Window{e.window}, e.timestamp, e.pane, &buf)
		buf.Write(e.elmBytes)
		ret = append(ret, buf.Bytes())
	}
	return ret
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

func (em *ElementManager) Impulse(stageID string) {
	stage := em.stages[stageID]
	newPending := []element{{
		window:    window.GlobalWindow{},
		timestamp: mtime.MinTimestamp,
		pane:      typex.NoFiringPane(),
		elmBytes:  []byte{0}, // Represents an encoded 0 length byte slice.
	}}

	consumers := em.consumers[stage.outputIDs[0]]
	//logger.Log("Impulse:", stageID, stage.outputIDs, consumers)
	em.pendingElements.Add(len(consumers))
	for _, sID := range consumers {
		consumer := em.stages[sID]
		consumer.AddPending(newPending)
		//	logger.Logf("Impulse: %v setting downstream stage %v, with %v new elements", stageID, sID, len(newPending))
	}
	refreshes := stage.updateWatermarks(mtime.MaxTimestamp, mtime.MaxTimestamp, em)
	em.addRefreshes(refreshes)
}

type RunBundle struct {
	StageID   string
	BundleID  string
	Watermark mtime.Time
}

func (rb RunBundle) String() string {
	return fmt.Sprintf("{%v %v %v}", rb.StageID, rb.BundleID, rb.Watermark)
}

func (em *ElementManager) Bundles(ctx context.Context, nextBundID func() string) <-chan RunBundle {
	runStageCh := make(chan RunBundle)
	ctx, cancelFn := context.WithCancel(ctx)
	go func() {
		em.pendingElements.Wait()
		//logger.Logf("no more pending elements: terminating pipeline")
		cancelFn()
		// Ensure the watermark evaluation goroutine exits.
		em.refreshCond.Broadcast()
	}()
	// Watermark evaluation goroutine.
	go func() {
		defer close(runStageCh)
		for {
			em.refreshCond.L.Lock()
			// If there are no watermark refreshes available, we wait until there are.
			for len(em.watermarkRefreshes) == 0 {
				// Check to see if we must exit
				select {
				case <-ctx.Done():
					em.refreshCond.L.Unlock()
					return
				default:
				}
				em.refreshCond.Wait()
			}

			// We know there is some work we can do that may advance the watermarks,
			// refresh them, and see which stages have advanced.
			advanced := em.refreshWatermarks()

			// Check each advanced stage, to see if it's able to execute based on the watermark.
			for stageID := range advanced {
				ss := em.stages[stageID]
				watermark, ready := ss.bundleReady(em)
				if ready {
					bundleID, ok := ss.startBundle(watermark, nextBundID)
					if !ok {
						continue
					}
					rb := RunBundle{StageID: stageID, BundleID: bundleID, Watermark: watermark}

					em.inprogressBundles.insert(rb.BundleID)
					em.refreshCond.L.Unlock()

					select {
					case <-ctx.Done():
						//		logger.Logf("Bundles: loop canceled (pre pending)")
						return
					case runStageCh <- rb:
					}
					em.refreshCond.L.Lock()
				}
			}
			em.refreshCond.L.Unlock()
		}
	}()
	return runStageCh
}

// InputForBundle returns pre-allocated data for the given bundle, encoding the elements using
// the pcollection's coders.
func (em *ElementManager) InputForBundle(stageID, bundleID string, info PColInfo) [][]byte {
	ss := em.stages[stageID]
	ss.mu.Lock()
	defer ss.mu.Unlock()
	es := ss.inprogress[bundleID]
	return es.ToData(info)
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
func (em *ElementManager) PersistBundle(stageID string, bundID string, col2Coders map[string]PColInfo, d TentativeData, inputInfo PColInfo, residuals [][]byte) {
	stage := em.stages[stageID]
	for output, data := range d.Raw {
		info := col2Coders[output]
		var newPending []element
		//	logger.Logf("PersistBundle: processing output for stage %v bundle %v output: %v", stageID, bundID, output)
		for _, datum := range data {
			buf := bytes.NewBuffer(datum)
			if len(datum) == 0 {
				panic(fmt.Sprintf("zero length data for %v: ", output))
			}
			for {
				var rawBytes bytes.Buffer
				tee := io.TeeReader(buf, &rawBytes)
				ws, et, pn, err := exec.DecodeWindowedValueHeader(info.WDec, tee)
				if err != nil {
					if err == io.EOF {
						break
					}
					//			logger.Fatalf("error decoding watermarks for %v: %v", output, err)
				}
				// TODO: Optimize unnecessary copies. This is doubleteeing.
				elmBytes := info.EDec(tee)
				for _, w := range ws {
					newPending = append(newPending,
						element{
							window:    w,
							timestamp: et,
							pane:      pn,
							elmBytes:  elmBytes,
						})
				}
			}
		}
		consumers := em.consumers[output]
		//logger.Logf("PersistBundle: %v has consumers: %v for %v elements", stageID, consumers, len(newPending))
		for _, sID := range consumers {
			em.pendingElements.Add(len(newPending))
			consumer := em.stages[sID]
			consumer.AddPending(newPending)

			//		logger.Logf("PersistBundle: setting downstream stage %v, with %v new elements", sID, len(newPending))
		}
	}

	// Return unprocessed to this stage's pending
	var unprocessedElements []element
	for _, residual := range residuals {
		buf := bytes.NewBuffer(residual)
		ws, et, pn, err := exec.DecodeWindowedValueHeader(inputInfo.WDec, buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			//	logger.Fatalf("error decoding header for residual  for %v: %v", stage.ID, err)
		}

		// TODO use a default output watermark estimator, since we should have watermark estimates
		// coming in most times.
		for _, w := range ws {
			unprocessedElements = append(unprocessedElements,
				element{
					window:    w,
					timestamp: et,
					pane:      pn,
					elmBytes:  buf.Bytes(),
				})
		}
	}
	// Add unprocessed back to the pending stack.
	if len(unprocessedElements) > 0 {
		em.pendingElements.Add(len(unprocessedElements))
		stage.AddPending(unprocessedElements)
	}
	// Clear out the inprogress elements associated with the completed bundle.
	// Must be done after adding the new pending elements to avoid an incorrect
	// watermark advancement.
	stage.mu.Lock()
	completed := stage.inprogress[bundID]
	em.pendingElements.Add(-len(completed.es))
	delete(stage.inprogress, bundID)
	stage.mu.Unlock()

	//logger.Logf("PersistBundle: removed pending bundle %v, with %v processed elements", bundID, -len(completed.es))

	// TODO support state/timer watermark holds.
	em.addRefreshAndClearBundle(stage.ID, bundID)
}

func (em *ElementManager) addRefreshes(stages set[string]) {
	em.refreshCond.L.Lock()
	defer em.refreshCond.L.Unlock()
	em.watermarkRefreshes.merge(stages)
	em.refreshCond.Broadcast()
}

func (em *ElementManager) addRefreshAndClearBundle(stageID, bundID string) {
	em.refreshCond.L.Lock()
	defer em.refreshCond.L.Unlock()
	delete(em.inprogressBundles, bundID)
	em.watermarkRefreshes.insert(stageID)
	em.refreshCond.Broadcast()
}

// refreshWatermarks incrementally refreshes the watermarks, and returns the set of stages where the
// the watermark may have advanced.
// Must be called while holding em.refreshCond.L
func (em *ElementManager) refreshWatermarks() set[string] {
	// Need to have at least one refresh signal.
	nextUpdates := set[string]{}
	refreshed := set[string]{}
	var i int
	for stageID := range em.watermarkRefreshes {
		// clear out old one.
		em.watermarkRefreshes.remove(stageID)
		ss := em.stages[stageID]
		refreshed.insert(stageID)

		dummyStateHold := mtime.MaxTimestamp

		refreshes := ss.updateWatermarks(ss.minPendingTimestamp(), dummyStateHold, em)
		nextUpdates.merge(refreshes)
		// cap refreshes incrementally.
		if i < 10 {
			i++
		} else {
			break
		}
	}
	em.watermarkRefreshes.merge(nextUpdates)
	return refreshed
}

type set[K comparable] map[K]struct{}

func (s set[K]) remove(k K) {
	delete(s, k)
}

func (s set[K]) insert(k K) {
	s[k] = struct{}{}
}

func (s set[K]) merge(o set[K]) {
	for k := range o {
		s.insert(k)
	}
}

type stageState struct {
	ID        string
	inputID   string   // PCollection ID of the parallel input
	outputIDs []string // PCollection IDs of outputs to update consumers.
	sides     []string // PCollection IDs of side inputs that can block execution.
	strat     winStrat

	mu                 sync.Mutex
	upstreamWatermarks sync.Map   // watermark set from inputPCollection's parent.
	input              mtime.Time // input watermark for the parallel input.
	output             mtime.Time // Output watermark for the whole stage

	pending    elementHeap         // pending input elements for this stage that are to be processesd
	inprogress map[string]elements // inprogress elements by active bundles, keyed by bundle
}

// makeStageState produces an initialized stage stage.
func makeStageState(ID string, inputIDs, sides, outputIDs []string) *stageState {
	ss := &stageState{
		ID:        ID,
		outputIDs: outputIDs,
		sides:     sides,
		strat:     defaultStrat{},

		input:  mtime.MinTimestamp,
		output: mtime.MinTimestamp,
	}

	// Initialize the upstream watermarks to minTime.
	for _, pcol := range inputIDs {
		ss.upstreamWatermarks.Store(pcol, mtime.MinTimestamp)
	}
	if len(inputIDs) == 1 {
		ss.inputID = inputIDs[0]
	}
	//logger.Logf("makeStageState: %v input: %v sides: %v outputs: %v", ID, ss.inputID, ss.sides, ss.outputIDs)
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
	ss.upstreamWatermarks.Store(pcol, upstream)
}

// UpstreamWatermark get's the minimum value of all upstream watermarks.
func (ss *stageState) UpstreamWatermark() (string, mtime.Time) {
	upstream := mtime.MaxTimestamp
	var name string
	ss.upstreamWatermarks.Range(func(key, val any) bool {
		if val.(mtime.Time) < upstream {
			upstream = val.(mtime.Time)
			name = key.(string)
		}
		return true
	})
	return name, upstream
}

// InputWatermark gets the current input watermark for the stage.
func (ss *stageState) InputWatermark() mtime.Time {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.input
}

// OutputWatermark gets the current output watermark for the stage.
func (ss *stageState) OutputWatermark() mtime.Time {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.output
}

func (ss *stageState) startBundle(watermark mtime.Time, genBundID func() string) (string, bool) {
	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Sprintf("generating bundle for stage %v at %v panicked\n%v", ss.ID, watermark, e))
		}
	}()
	ss.mu.Lock()
	defer ss.mu.Unlock()
	//	logger.Logf("startBundle: %v %v - %v pending elements at %v", ss.ID, rb.bundleID, len(ss.pending), rb.watermark)

	var toProcess, notYet []element
	for _, e := range ss.pending {
		if e.window.MaxTimestamp() <= watermark {
			toProcess = append(toProcess, e)
		} else {
			notYet = append(notYet, e)
		}
	}
	ss.pending = notYet
	heap.Init(&ss.pending)

	if len(toProcess) == 0 {
		//	logger.Logf("startBundle: %v %v - no elements at %v", ss.ID, rb.bundleID, rb.watermark)
		return "", false
	}
	// Is THIS is where basic splits should happen/per element processing?
	es := elements{
		es:           toProcess,
		minTimestamp: toProcess[0].timestamp,
	}
	if ss.inprogress == nil {
		ss.inprogress = make(map[string]elements)
	}
	bundID := genBundID()
	ss.inprogress[bundID] = es
	//logger.Logf("startBundle: %v %v - %v elements at %v", ss.ID, rb.bundleID, len(es.es), rb.watermark)
	return bundID, true
}

// minimumPendingTimestamp returns the minimum pending timestamp from all pending elements,
// including in progress ones.
//
// Assumes that the pending heap is initialized if it's not empty.
func (ss *stageState) minPendingTimestamp() mtime.Time {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	minPending := mtime.MaxTimestamp
	if len(ss.pending) != 0 {
		minPending = ss.pending[0].timestamp
	}
	for _, es := range ss.inprogress {
		minPending = mtime.Min(minPending, es.minTimestamp)
	}
	return minPending
}

func (ss *stageState) String() string {
	pcol, up := ss.UpstreamWatermark()
	return fmt.Sprintf("[%v] IN: %v OUT: %v UP: %q %v", ss.ID, ss.input, ss.output, pcol, up)
}

// updateWatermarks performs the following operations:
//
// Watermark_In'  = MAX(Watermark_In, MIN(U(TS_Pending), U(Watermark_InputPCollection)))
// Watermark_Out' = MAX(Watermark_Out, MIN(Watermark_In', U(StateHold)))
// Watermark_PCollection = Watermark_Out_ProducingPTransform
func (ss *stageState) updateWatermarks(minPending, minStateHold mtime.Time, em *ElementManager) set[string] {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	// PCollection watermarks are based on their parents's output watermark.
	_, newIn := ss.UpstreamWatermark()

	//	logger.Logf("updateWatermarks[%v]: upstream %v %v, minPending %v, minStateHold %v input %v output %v", ss.ID, pcol, newIn, minPending, minStateHold, ss.input, ss.output)

	// Set the input watermark based on the minimum pending elements,
	// and the current input pcollection watermark.
	if minPending < newIn {
		newIn = minPending
	}

	// If bigger, advance the input watermark.
	if newIn > ss.input {
		//	logger.Logf("updateWatermarks[%v]: advancing input watermark from %v to %v", ss.ID, ss.input, newIn)
		ss.input = newIn
	}
	// The output starts with the new input as the basis.
	newOut := ss.input
	if minStateHold < newOut {
		newOut = minStateHold
	}
	refreshes := set[string]{}
	// If bigger, advance the output watermark
	if newOut > ss.output {
		//	logger.Logf("updateWatermarks[%v]: advancing output watermark from %v to %v", ss.ID, ss.output, newOut)
		ss.output = newOut
		for _, outputCol := range ss.outputIDs {
			consumers := em.consumers[outputCol]
			// if len(consumers) > 0 {
			// 	//	logger.Logf("updateWatermarks[%v]: setting downstream consumer of %v to %v for %v", ss.ID, outputCol, ss.output, consumers)
			// }
			for _, sID := range consumers {
				em.stages[sID].updateUpstreamWatermark(outputCol, ss.output)
				refreshes.insert(sID)
			}
			// Inform side input consumers, but don't update the upstream watermark.
			for _, sID := range em.sideConsumers[outputCol] {
				refreshes.insert(sID)
			}
		}
	}
	return refreshes
}

// bundleReady returns the maximum allowed watermark for this stage, and whether
// it's permitted to execute by side inputs.
func (ss *stageState) bundleReady(em *ElementManager) (mtime.Time, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	// If the upstream watermark and the input watermark are the same,
	// then we can't yet process this stage.
	inputW := ss.input
	_, upstreamW := ss.UpstreamWatermark()
	if inputW == upstreamW {
		//	logger.Logf("Bundles: stage %v has insufficient upstream watermark; requires %q %v > %v", ss.ID, upstreamPcol, inputW, upstreamW)
		return mtime.MinTimestamp, false
	} else if inputW > upstreamW {
		//	logger.Fatalf("%v has invariance violation, input watermark greater than upstream: %v", ss.ID, inputW, upstreamW)
		return mtime.MinTimestamp, false
	}

	// We know now that inputW < upstreamW, so we should be able to process any available inputs.
	// if len(ss.sides) > 0 {
	// 	//	logger.Logf("Bundles: stage %v has side inputs %v", ss.ID, ss.sides)
	// }
	ready := true
	for _, side := range ss.sides {
		pID := em.pcolParents[side]
		parent := em.stages[pID]
		ow := parent.OutputWatermark()
		//	logger.Logf("Bundles: stage %v parent[%v] side input %v watermark; requires %v >= %v", ss.ID, pID, side, ow, upstreamW)

		if upstreamW > ow {
			//		logger.Logf("Bundles: stage %v side input %v from %v insufficient watermark; requires %v >= %v", ss.ID, side, pID, ow, upstreamW)
			ready = false
		}
	}
	return upstreamW, ready
}
