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
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"golang.org/x/exp/maps"
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

	consumers     map[string][]string // Map from pcollectionID to stageIDs that consumes them as primary input.
	sideConsumers map[string][]string // Map from pcollectionID to stageIDs that consumes them as side input.

	pcolParents map[string]string // Map from pcollectionID to stageIDs that produce the pcollection.

	stageMu           sync.Mutex
	pendingStages     runBundleHeap
	readyStages       runBundleHeap
	inprogressBundles set[string] // Active bundleIDs

	refreshMu          sync.Mutex
	watermarkRefreshes set[string] // Scheduled stageID watermark refreshes

	pendingElements sync.WaitGroup
}

func newElementManager() *elementManager {
	return &elementManager{
		stages:             map[string]*stageState{},
		consumers:          map[string][]string{},
		sideConsumers:      map[string][]string{},
		pcolParents:        map[string]string{},
		watermarkRefreshes: set[string]{},
		inprogressBundles:  set[string]{},
	}
}

func (em *elementManager) addStage(ID string, inputIDs, sides, outputIDs []string) {
	logger.Logf("addStage: %v inputs: %v sides: %v outputs: %v", ID, inputIDs, sides, outputIDs)
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

type stageState struct {
	ID        string
	inputID   string   // PCollection ID of the parallel input
	outputIDs []string // PCollection IDs of outputs to update consumers.
	sides     []string // PCollection IDs of side inputs that can block execution.

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
	logger.Logf("makeStageState: %v input: %v sides: %v outputs: %v", ID, ss.inputID, ss.sides, ss.outputIDs)
	// If this is zero, then it's an impulse, and will sort itself out.
	// TODO, add initial pendings here?
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

func (es elements) ToData() [][]byte {
	var ret [][]byte
	for _, e := range es.es {
		ret = append(ret, e.elmBytes)
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

// runBundle orders elements based on their watermark timestamps, so we
// can pull pending bundles off the cue in processing order.
type runBundleHeap []runBundle

func (h runBundleHeap) Len() int           { return len(h) }
func (h runBundleHeap) Less(i, j int) bool { return h[i].watermark < h[j].watermark }
func (h runBundleHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *runBundleHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(runBundle))
}

func (h *runBundleHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (em *elementManager) Impulse(stageID string) {
	stage := em.stages[stageID]
	newPending := []element{{
		window:    window.GlobalWindow{},
		timestamp: mtime.Now(),
		pane:      typex.NoFiringPane(),
		elmBytes:  impulseBytes(),
	}}

	consumers := em.consumers[stage.outputIDs[0]]
	logger.Log("Impulse:", stageID, stage.outputIDs, consumers)
	em.pendingElements.Add(len(consumers))
	for _, sID := range consumers {
		consumer := em.stages[sID]
		consumer.AddPending(newPending)
		logger.Logf("Impulse: %v setting downstream stage %v, with %v new elements", stageID, sID, len(newPending))
		em.addPendingStage(runBundle{stageID: sID, watermark: mtime.EndOfGlobalWindowTime})
	}
	refreshes := stage.updateWatermarks(mtime.MaxTimestamp, mtime.MaxTimestamp, em)
	em.addRefreshes(refreshes)
}

func (em *elementManager) addPendingStage(rb runBundle) {
	em.stageMu.Lock()
	defer em.stageMu.Unlock()
	heap.Push(&em.pendingStages, rb)
}

type runBundle struct {
	stageID   string
	bundleID  string
	watermark mtime.Time
}

func (rb runBundle) String() string {
	return fmt.Sprintf("{%v %v %v}", rb.stageID, rb.bundleID, rb.watermark)
}

func (ss *stageState) startBundle(rb runBundle) bool {
	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Sprintf("stage %v bundle %v at %v panicked\n%v", ss.ID, rb.bundleID, rb.watermark, e))
		}
	}()
	ss.mu.Lock()
	defer ss.mu.Unlock()
	logger.Logf("startBundle: %v %v - %v pending elements at %v", ss.ID, rb.bundleID, len(ss.pending), rb.watermark)

	var toProcess, notYet []element
	for _, e := range ss.pending {
		if e.window.MaxTimestamp() <= rb.watermark {
			toProcess = append(toProcess, e)
		} else {
			notYet = append(notYet, e)
		}
	}
	ss.pending = notYet
	heap.Init(&ss.pending)

	if len(toProcess) == 0 {
		logger.Logf("startBundle: %v %v - no elements at %v", ss.ID, rb.bundleID, rb.watermark)
		return false
	}
	// Is THIS is where basic splits should happen/per element processing?
	es := elements{
		es:           toProcess,
		minTimestamp: toProcess[0].timestamp,
	}
	if ss.inprogress == nil {
		ss.inprogress = make(map[string]elements)
	}
	ss.inprogress[rb.bundleID] = es
	logger.Logf("startBundle: %v %v - %v elements at %v", ss.ID, rb.bundleID, len(es.es), rb.watermark)
	return true
}

func (em *elementManager) processReadyBundles(ctx context.Context, runStageCh chan runBundle, nextBundID func() string) {
	for {
		em.stageMu.Lock()
		if em.readyStages.Len() == 0 {
			em.stageMu.Unlock()
			return // escape from ready loop.
		}
		rb := heap.Pop(&em.readyStages).(runBundle)
		rb.bundleID = nextBundID()
		em.stageMu.Unlock()

		ok := em.stages[rb.stageID].startBundle(rb)
		if !ok {
			continue
		}
		em.stageMu.Lock()
		em.inprogressBundles.insert(rb.bundleID)
		em.stageMu.Unlock()

		logger.Log("Bundles: ready", rb)
		select {
		case <-ctx.Done():
			logger.Logf("Bundles: loop canceled")
			return
		case runStageCh <- rb:
		}
	}
}

func (em *elementManager) Bundles(nextBundID func() string) <-chan runBundle {
	runStageCh := make(chan runBundle)
	ctx, cancelFn := context.WithCancel(context.TODO())
	go func() {
		em.pendingElements.Wait()
		logger.Logf("no more pending elements: terminating pipeline")
		cancelFn()
	}()
	go func() {
		defer func() {
			close(runStageCh)
			logger.Logf("closing stage channel")
		}()
		// repeats is a failsafe to avoid infinite loops in testing.
		// Once we have condition stage scheduling, this shouldn't be
		// necessary.
		repeats := map[string]int{}
		repeatCap := 1000
		for {
			// Process all ready bundles.
			em.processReadyBundles(ctx, runStageCh, nextBundID)

			// We've run out of ready bundles, so lets see if anything pending is ready.
			em.refreshWatermarks()

			var notReadyYet runBundleHeap
			// Process all pending staging, and if they're ready by watermar, add them to the
			// ready queue, otherwise, reset the heap.
			em.stageMu.Lock()
			for {
				select {
				case <-ctx.Done():
					em.stageMu.Unlock()
					logger.Logf("Bundles: loop canceled (pre pending)")
					return
				default:
				}
				if em.pendingStages.Len() == 0 {
					break // escape from pending loop.
				}
				rb := heap.Pop(&em.pendingStages).(runBundle)

				// TODO, check if the bundle is good to run by the watermark
				ready := true
				ss := em.stages[rb.stageID]

				if pcol, wm := ss.UpstreamWatermark(); rb.watermark > wm {
					logger.Logf("Bundles: stage %v insufficient upstream watermark; requires %q %v > %v", rb.stageID, pcol, wm, rb.watermark)
					ready = false
				}
				if len(ss.sides) > 0 {
					logger.Logf("Bundles: stage %v has side inputs %v", rb.stageID, ss.sides)
				}
				var waitingOnSides []string
				for _, side := range ss.sides {
					pID := em.pcolParents[side]
					parent := em.stages[pID]
					// Possible lock inversion?
					ow := parent.OutputWatermark()
					logger.Logf("Bundles: stage %v parent[%v] side input %v watermark; requires %v >= %v", rb.stageID, pID, side, ow, rb.watermark)

					if rb.watermark > ow {
						logger.Logf("Bundles: stage %v side input %v from %v insufficient watermark; requires %v >= %v", rb.stageID, side, pID, ow, rb.watermark)
						ready = false
						waitingOnSides = append(waitingOnSides, side)
					}
				}
				repeats[rb.stageID] = repeats[rb.stageID] + 1
				if ready {
					repeats[rb.stageID] = 0
					logger.Logf("Bundles: %v is ready", rb)
					heap.Push(&em.readyStages, rb)
				} else {
					em.refreshMu.Lock()
					refs := maps.Keys(em.watermarkRefreshes)
					em.refreshMu.Unlock()
					sort.Strings(refs)
					sort.Strings(waitingOnSides)
					bundles := maps.Keys(em.inprogressBundles)
					sort.Strings(bundles)
					if len(bundles) > 0 {
						// reset the cap while there are extant bundles.
						repeats[rb.stageID] = 1
					}

					if repeats[rb.stageID] > repeatCap {
						var b strings.Builder
						b.WriteString("TOO MANY ATTEMPTS for ")
						b.WriteString(rb.String())
						keys := maps.Keys(em.stages)
						sort.Strings(keys)
						for _, key := range keys {
							b.WriteRune('\n')
							ss := em.stages[key]
							b.WriteString(ss.String())
						}
						b.WriteString("\nrefreshes pending: ")
						b.WriteString(strings.Join(refs, ","))

						b.WriteString("\nbundles inProgress: ")
						b.WriteString(strings.Join(bundles, ","))
						panic(b.String())
					}

					logger.Logf("Bundles:%v is still pending: refreshes %v; bundles %v; sides %v", rb, refs, bundles, waitingOnSides)
					heap.Push(&notReadyYet, rb)
				}
			}
			// Reset the pendingStages queue with the not yet ready stages.
			em.pendingStages = notReadyYet
			em.stageMu.Unlock()
			select {
			case <-ctx.Done():
				logger.Logf("Bundles: loop canceled (post pending)")
				return
			default:
			}
		}
	}()
	return runStageCh
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
func (em *elementManager) PersistBundle(stageID string, bundID string, col2Coders map[string]PColInfo, d tentativeData, inputInfo PColInfo, residuals [][]byte) {
	stage := em.stages[stageID]
	for output, data := range d.raw {
		// Can't use set[typex.Window] until go1.20
		uniqueWindows := map[typex.Window]struct{}{}
		info := col2Coders[output]
		var newPending []element
		logger.Logf("PersistBundle: processing output for stage %v bundle %v output: %v", stageID, bundID, output)
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
				elmBytes := info.eDec(tee)
				for _, w := range ws {
					var buf bytes.Buffer
					if err := exec.EncodeWindowedValueHeader(info.wEnc, []typex.Window{w}, et, typex.NoFiringPane(), &buf); err != nil {
						logger.Fatalf("error decoding watermarks for %v: %v", output, err)
					}
					buf.Write(elmBytes)
					newPending = append(newPending,
						element{
							window:    w,
							timestamp: et,
							pane:      pn,
							elmBytes:  buf.Bytes(),
						})
					uniqueWindows[w] = struct{}{}
				}
			}
		}
		consumers := em.consumers[output]
		logger.Logf("PersistBundle: %v has consumers: %v for %v elements", stageID, consumers, len(newPending))
		for _, sID := range consumers {
			em.pendingElements.Add(len(newPending))
			consumer := em.stages[sID]
			consumer.AddPending(newPending)

			logger.Logf("PersistBundle: setting downstream stage %v, with %v new elements", sID, len(newPending))
			// Schedule downstream for each unique window in the pending elements.
			for w := range uniqueWindows {
				em.addPendingStage(runBundle{stageID: sID, watermark: w.MaxTimestamp()})
			}
		}
	}

	// Return unprocessed to this stage's pending
	var unprocessedElements []element
	uniqueWindows := map[typex.Window]struct{}{}
	var buf bytes.Buffer
	for _, residual := range residuals {
		buf.Reset()
		buf.Write(residual)
		ws, et, pn, err := exec.DecodeWindowedValueHeader(inputInfo.wDec, &buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			logger.Fatalf("error decoding header for residual  for %v: %v", stage.ID, err)
		}

		// TODO recode for each window.
		for _, w := range ws {
			unprocessedElements = append(unprocessedElements,
				element{
					window:    w,
					timestamp: et,
					pane:      pn,
					elmBytes:  residual,
				})
			uniqueWindows[w] = struct{}{}
		}
	}
	// Add unprocessed back to the pending stack.
	if len(unprocessedElements) > 0 {
		em.pendingElements.Add(len(unprocessedElements))
		stage.AddPending(unprocessedElements)

		for w := range uniqueWindows {
			em.addPendingStage(runBundle{stageID: stageID, watermark: w.MaxTimestamp()})
		}
	}
	// Clear out the inprogress elements associated with the completed bundle.
	// Must be done after adding the new pending elements to avoid an incorrect
	// watermark advancement.
	stage.mu.Lock()
	completed := stage.inprogress[bundID]
	em.pendingElements.Add(-len(completed.es))
	delete(stage.inprogress, bundID)
	stage.mu.Unlock()
	em.stageMu.Lock()
	em.inprogressBundles.remove(bundID)
	em.stageMu.Unlock()

	logger.Logf("PersistBundle: removed pending bundle %v, with %v processed elements", bundID, -len(completed.es))

	// TODO support state/timer watermark holds.
	em.addRefresh(stage.ID)
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

func (em *elementManager) addRefreshes(stages set[string]) {
	em.refreshMu.Lock()
	defer em.refreshMu.Unlock()
	em.watermarkRefreshes.merge(stages)
}

func (em *elementManager) addRefresh(stageID string) {
	em.refreshMu.Lock()
	defer em.refreshMu.Unlock()
	em.watermarkRefreshes.insert(stageID)
}

func (em *elementManager) refreshWatermarks() {
	// Need to have at least one refresh signal.
	em.refreshMu.Lock()
	defer em.refreshMu.Unlock()
	nextUpdates := set[string]{}
	for stageID := range em.watermarkRefreshes {
		// clear out old one.
		em.watermarkRefreshes.remove(stageID)
		ss := em.stages[stageID]

		dummyStateHold := mtime.MaxTimestamp

		refreshes := ss.updateWatermarks(ss.minPendingTimestamp(), dummyStateHold, em)
		nextUpdates.merge(refreshes)
	}
	em.watermarkRefreshes.merge(nextUpdates)
}

// updateWatermarks performs the following operations:
//
// Watermark_In'  = MAX(Watermark_In, MIN(U(TS_Pending), U(Watermark_InputPCollection)))
// Watermark_Out' = MAX(Watermark_Out, MIN(Watermark_In', U(StateHold)))
// Watermark_PCollection = Watermark_Out_ProducingPTransform
func (ss *stageState) updateWatermarks(minPending, minStateHold mtime.Time, em *elementManager) set[string] {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	// PCollection watermarks are based on their parents's output watermark.
	pcol, newIn := ss.UpstreamWatermark()

	logger.Logf("updateWatermarks[%v]: upstream %v %v, minPending %v, minStateHold %v input %v output %v", ss.ID, pcol, newIn, minPending, minStateHold, ss.input, ss.output)

	// Set the input watermark based on the minimum pending elements,
	// and the current input pcollection watermark.
	if minPending < newIn {
		newIn = minPending
	}

	// If bigger, advance the input watermark.
	if newIn > ss.input {
		logger.Logf("updateWatermarks[%v]: advancing input watermark from %v to %v", ss.ID, ss.input, newIn)
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
		logger.Logf("updateWatermarks[%v]: advancing output watermark from %v to %v", ss.ID, ss.output, newOut)
		// TODO, trigger watermark advancement of consuming stages
		ss.output = newOut
		for _, outputCol := range ss.outputIDs {
			consumers := em.consumers[outputCol]
			if len(consumers) > 0 {
				logger.Logf("updateWatermarks[%v]: setting downstream consumer of %v to %v for %v", ss.ID, outputCol, ss.output, consumers)
			}
			for _, sID := range em.consumers[outputCol] {
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
