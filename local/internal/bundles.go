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
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/proto"
)

func executePipeline(wk *worker, j *job) {
	pipeline := j.pipeline
	comps := proto.Clone(pipeline.GetComponents()).(*pipepb.Components)

	// TODO, configure the preprocessor from pipeline options.
	// Maybe change these returns to a single struct for convenience and further
	// annotation?

	handlers := []any{
		Combine(CombineCharacteristic{EnableLifting: true}),
		ParDo(ParDoCharacteristic{DisableSDF: true}),
		Runner(RunnerCharacteristic{}),
	}

	prepro := &preprocessor{
		transformPreparers: map[string]transformPreparer{},
	}

	proc := processor{
		transformExecuters: map[string]transformExecuter{},
	}

	for _, h := range handlers {
		if th, ok := h.(transformPreparer); ok {
			for _, urn := range th.PrepareUrns() {
				prepro.transformPreparers[urn] = th
			}
		}
		if th, ok := h.(transformExecuter); ok {
			for _, urn := range th.ExecuteUrns() {
				proc.transformExecuters[urn] = th
			}
		}
	}

	// We're going to do something fun with channels for this,
	// and do a "Generator" pattern to get the bundles we need
	// to process.
	// Ultimately, we'd want some pre-processing of the pipeline
	// which will determine dependency relationshion ships, and
	// when it's safe to GC bundle results.
	// The key idea is that we have a goroutine that will block on
	// channels, and emit the next bundle to process.
	toProcess, processed := make(chan *bundle), make(chan *bundle)

	// Goroutine for executing bundles on the worker.
	go func() {
		// Send nil to start, Impulses won't require parental translation.
		processed <- nil
		for b := range toProcess {
			V(1).Logf("processing %v", b.PBDID)
			b.ProcessOn(wk) // Blocks until finished.

			resp := <-b.Resp
			V(1).Logf("got response for %v", b.PBDID)
			// Tentative Data is ready, commit it to the main datastore.
			wk.data.Commit(b.Generation, b.OutputData)
			b.OutputData = tentativeData{} // Clear the data.
			j.metrics.contributeMetrics(resp)

			// Basic attempt at Process Continuations.
			// Goal 1: Process applications to completion, then start the next bundle.
			if len(resp.GetResidualRoots()) > 0 {
				var data [][]byte
				for _, rr := range resp.GetResidualRoots() {
					ba := rr.GetApplication()
					data = append(data, ba.GetElement())

					if len(ba.GetElement()) == 0 {
						logger.Fatalf("bundle %v returned empty residual application", b.PBDID)
					}
				}
				// Technically we also grab the read index,
				// then we use that against the original data set by input elements for the remaining residual,
				// to avoid the extra round trip back from the SDK when we already have the data.

				b.InputData = data
				if len(b.InputData) == 0 {
					logger.Fatalf("bundle %v returned empty residual application", b.PBDID)
				}

				b.DataWait.Add(b.OutputCount)
				go func() {
					toProcess <- b
					V(1).Logf("residuals %v sent", b.PBDID)
				}()

				continue
			}

			// Send back for dependency handling afterwards.
			processed <- b
		}
		close(processed)
	}()

	topo := prepro.preProcessGraph(comps)
	ts := comps.GetTransforms()

	// This is where the Batch -> Streaming tension exists.
	// We don't *pre* do this, and we need a different mechanism
	// to sort out processing order.
	// Prepare stage here.
	for i, stage := range topo {
		if len(stage.transforms) != 1 {
			V(0).Fatalf("unsupported stage[%d]: contains multiple transforms: %v", i, stage.transforms)
		}
		tid := stage.transforms[0]
		t := ts[tid]
		urn := t.GetSpec().GetUrn()
		exe := proc.transformExecuters[urn]

		// Stopgap until everythinng's moved to handlers.
		envID := t.GetEnvironmentId()
		if exe != nil {
			envID = exe.ExecuteWith(t)
		}

		switch envID {
		case "": // Runner Transforms
		case wk.ID:
			// Great! this is for this environment. // Broken abstraction.
			buildStage(stage, tid, t, comps, wk)
		default:
			logger.Fatalf("unknown environment[%v]", t.GetEnvironmentId())
		}
	}

	// Execute stages here
	for i, stage := range topo {
		// Block until the previous bundle is done.
		prevBundle := <-processed
		var gen int
		if prevBundle != nil {
			gen = prevBundle.Generation
		}
		// Stopgap until everythinng's moved to handlers.
		if len(stage.transforms) != 1 {
			V(0).Fatalf("unsupported stage[%d]: contains multiple transforms: %v", i, stage.transforms)
		}
		tid := stage.transforms[0]
		t := ts[tid]
		urn := t.GetSpec().GetUrn()
		exe := proc.transformExecuters[urn]
		envID := t.GetEnvironmentId()
		if exe != nil {
			envID = exe.ExecuteWith(t)
		}

		var sendBundle func()
		switch envID {
		case "": // Runner Transforms
			// Runner transforms are processed immeadiately.
			b := exe.ExecuteTransform(tid, t, comps, wk, gen)
			sendBundle = func() {
				go func() {
					processed <- b
				}()
			}
		case wk.ID:
			bs := stage.makeBundles(wk.data, gen)
			// FnAPI instructions need to be sent to the SDK.
			sendBundle = func() {
				toProcess <- bs[0]
			}
		default:
			logger.Fatalf("unknown environment[%v]", t.GetEnvironmentId())
		}
		sendBundle()
	}

	// We're done with the pipeline!
	close(toProcess)
	b := <-processed // Drain the final bundle.
	V(1).Logf("pipeline done! Final Bundle: %v", b.InstID)
}

func buildStage(s *stage, tid string, t *pipepb.PTransform, comps *pipepb.Components, wk *worker) {
	pbdID := wk.nextBund()

	s.inputTransformID = tid + "_source"

	coders := map[string]*pipepb.Coder{}
	transforms := map[string]*pipepb.PTransform{
		tid: t, // The Transform to Execute!
	}

	sis, err := getSideInputs(t)
	if err != nil {
		logger.Fatalf("for transform %v: %v", tid, err)
	}
	var mainInputPCol string
	for local, global := range t.GetInputs() {
		// This id is directly used for the source, but this also copies
		// coders used by side inputs to the coders map for the bundle, so
		// needs to be run for every ID.
		wInCid := makeWindowedValueCoder(t, global, comps, coders)
		_, ok := sis[local]
		if !ok {
			// this is the main input
			mainInputPCol = global
			transforms[s.inputTransformID] = sourceTransform(s.inputTransformID, portFor(wInCid, wk), mainInputPCol)
		}
		// We need to process all inputs to ensure we have all input coders, so we must continue.
	}

	prepareSides, err := handleSideInputs(t, comps, coders, wk)
	if err != nil {
		logger.Fatalf("for transform %v: %v", tid, err)
	}

	// TODO: We need a new logical PCollection to represent the source
	// so we can avoid double counting PCollection metrics later.
	// But this also means replacing the ID for the input in the bundle.
	sink2Col := map[string]string{}
	for local, global := range t.GetOutputs() {
		wOutCid := makeWindowedValueCoder(t, global, comps, coders)
		sinkID := tid + "_" + local
		sink2Col[sinkID] = global
		transforms[sinkID] = sinkTransform(sinkID, portFor(wOutCid, wk), global)
	}

	reconcileCoders(coders, comps.GetCoders())

	desc := &fnpb.ProcessBundleDescriptor{
		Id:                  pbdID,
		Transforms:          transforms,
		WindowingStrategies: comps.GetWindowingStrategies(),
		Pcollections:        comps.GetPcollections(),
		Coders:              coders,
		StateApiServiceDescriptor: &pipepb.ApiServiceDescriptor{
			Url: wk.Endpoint(),
		},
	}

	s.ID = pbdID
	s.desc = desc
	s.outputCount = len(t.Outputs)
	s.prepareSides = prepareSides
	s.SinkToPCollection = sink2Col
	s.mainInputPCol = mainInputPCol

	wk.stages[s.ID] = s
}

func getSideInputs(t *pipepb.PTransform) (map[string]*pipepb.SideInput, error) {
	if t.GetSpec().GetUrn() != urnTransformParDo {
		return nil, nil
	}
	pardo := &pipepb.ParDoPayload{}
	if err := (proto.UnmarshalOptions{}).Unmarshal(t.GetSpec().GetPayload(), pardo); err != nil {
		return nil, fmt.Errorf("unable to decode ParDoPayload")
	}
	return pardo.GetSideInputs(), nil
}

// handleSideInputs ensures appropriate coders are available to the bundle, and prepares a function to stage the data.
func handleSideInputs(t *pipepb.PTransform, comps *pipepb.Components, coders map[string]*pipepb.Coder, wk *worker) (func(b *bundle, tid string, gen int), error) {
	sis, err := getSideInputs(t)
	if err != nil {
		return nil, err
	}
	var prepSides []func(b *bundle, tid string, gen int)

	// Get WindowedValue Coders for the transform's input and output PCollections.
	for local, global := range t.GetInputs() {
		si, ok := sis[local]
		if !ok {
			continue // This is the main input.
		}

		// this is a side input
		switch si.GetAccessPattern().GetUrn() {
		case urnSideInputIterable:
			V(2).Logf("urnSideInputIterable key? src %v, local %v, global %v", local, global)
			col := comps.GetPcollections()[global]
			cID := lpUnknownCoders(col.GetCoderId(), coders, comps.GetCoders())
			ec := coders[cID]
			ed := pullDecoder(ec, coders)

			ws := comps.GetWindowingStrategies()[col.GetWindowingStrategyId()]
			wcID := lpUnknownCoders(ws.GetWindowCoderId(), coders, comps.GetCoders())
			wDec, wEnc := makeWindowCoders(coders[wcID])
			// May be of zero length, but that's OK. Side inputs can be empty.

			global, local := global, local
			prepSides = append(prepSides, func(b *bundle, tid string, gen int) {
				data := wk.data.GetData(global, gen)

				if b.IterableSideInputData == nil {
					b.IterableSideInputData = map[string]map[string]map[string][][]byte{}
				}
				if _, ok := b.IterableSideInputData[tid]; !ok {
					b.IterableSideInputData[tid] = map[string]map[string][][]byte{}
				}
				b.IterableSideInputData[tid][local] = collateByWindows(data, wDec, wEnc,
					func(r io.Reader) [][]byte {
						return [][]byte{ed(r)}
					}, func(a, b [][]byte) [][]byte {
						return append(a, b...)
					})
			})

		case urnSideInputMultiMap:
			V(2).Logf("urnSideInputMultiMap key? %v, %v", local, global)
			col := comps.GetPcollections()[global]

			kvc := comps.GetCoders()[col.GetCoderId()]
			if kvc.GetSpec().GetUrn() != urnCoderKV {
				return nil, fmt.Errorf("multimap side inputs needs KV coder, got %v", kvc.GetSpec().GetUrn())
			}
			kcID := lpUnknownCoders(kvc.GetComponentCoderIds()[0], coders, comps.GetCoders())
			vcID := lpUnknownCoders(kvc.GetComponentCoderIds()[1], coders, comps.GetCoders())

			reconcileCoders(coders, comps.GetCoders())

			kc := coders[kcID]
			vc := coders[vcID]

			kd := pullDecoder(kc, coders)
			vd := pullDecoder(vc, coders)

			ws := comps.GetWindowingStrategies()[col.GetWindowingStrategyId()]
			wcID := lpUnknownCoders(ws.GetWindowCoderId(), coders, comps.GetCoders())
			wDec, wEnc := makeWindowCoders(coders[wcID])

			global, local := global, local
			prepSides = append(prepSides, func(b *bundle, tid string, gen int) {
				// May be of zero length, but that's OK. Side inputs can be empty.
				data := wk.data.GetData(global, gen)
				if b.MultiMapSideInputData == nil {
					b.MultiMapSideInputData = map[string]map[string]map[string]map[string][][]byte{}
				}
				if _, ok := b.MultiMapSideInputData[tid]; !ok {
					b.MultiMapSideInputData[tid] = map[string]map[string]map[string][][]byte{}
				}
				b.MultiMapSideInputData[tid][local] = collateByWindows(data, wDec, wEnc,
					func(r io.Reader) map[string][][]byte {
						kb := kd(r)
						return map[string][][]byte{
							string(kb): {vd(r)},
						}
					}, func(a, b map[string][][]byte) map[string][][]byte {
						if len(a) == 0 {
							return b
						}
						for k, vs := range b {
							a[k] = append(a[k], vs...)
						}
						return a
					})
			})
		default:
			return nil, fmt.Errorf("local input %v (global %v) uses accesspattern %v", local, global, si.GetAccessPattern().GetUrn())
		}
	}
	return func(b *bundle, tid string, gen int) {
		for _, prep := range prepSides {
			prep(b, tid, gen)
		}
	}, nil
}

func sourceTransform(parentID string, sourcePortBytes []byte, outPID string) *pipepb.PTransform {
	source := &pipepb.PTransform{
		UniqueName: parentID,
		Spec: &pipepb.FunctionSpec{
			Urn:     urnTransformSource,
			Payload: sourcePortBytes,
		},
		Outputs: map[string]string{
			"i0": outPID,
		},
	}
	return source
}

func sinkTransform(sinkID string, sinkPortBytes []byte, inPID string) *pipepb.PTransform {
	source := &pipepb.PTransform{
		UniqueName: sinkID,
		Spec: &pipepb.FunctionSpec{
			Urn:     urnTransformSink,
			Payload: sinkPortBytes,
		},
		Inputs: map[string]string{
			"i0": inPID,
		},
	}
	return source
}

func portFor(wInCid string, wk *worker) []byte {
	sourcePort := &fnpb.RemoteGrpcPort{
		CoderId: wInCid,
		ApiServiceDescriptor: &pipepb.ApiServiceDescriptor{
			Url: wk.Endpoint(),
		},
	}
	sourcePortBytes, err := proto.Marshal(sourcePort)
	if err != nil {
		logger.Fatalf("bad port: %v", err)
	}
	return sourcePortBytes
}

type transformExecuter interface {
	ExecuteUrns() []string
	ExecuteWith(t *pipepb.PTransform) string
	ExecuteTransform(tid string, t *pipepb.PTransform, comps *pipepb.Components, wk *worker, gen int) *bundle
}

type processor struct {
	transformExecuters map[string]transformExecuter
}

// bundle represents an extant ProcessBundle instruction sent to an SDK worker.
type bundle struct {
	InstID     string // ID for the instruction processing this bundle.
	PBDID      string // ID for the ProcessBundleDescriptor
	Generation int    // Which generation this is related to.

	// InputTransformID is data being sent to the SDK.
	InputTransformID string
	InputData        [][]byte // Data specifically for this bundle.

	// TODO change to a single map[tid] -> map[input] -> map[window] -> struct { Iter data, MultiMap data } instead of all maps.
	// IterableSideInputData is a map from transformID, to inputID, to window, to data.
	IterableSideInputData map[string]map[string]map[string][][]byte
	// MultiMapSideInputData is a map from transformID, to inputID, to window, to data key, to data values.
	MultiMapSideInputData map[string]map[string]map[string]map[string][][]byte

	// OutputCount is the number of data outputs this bundle has.
	// We need to see this many closed data channels before the bundle is complete.
	OutputCount int
	// DataWait is how we determine if a bundle is finished, by waiting for each of
	// a Bundle's DataSinks to produce their last output.
	// After this point we can "commit" the bundle's output for downstream use.
	DataWait   sync.WaitGroup
	OutputData tentativeData
	Resp       chan *fnpb.ProcessBundleResponse

	SinkToPCollection map[string]string

	// TODO: Metrics for this bundle, can be handled after the fact.
}

// ProcessOn executes the given bundle on the given worker.
//
// Assumes the bundle is initialized (all maps are non-nil, and data waitgroup is set.)
// Assumes the bundle descriptor is already registered.
func (b *bundle) ProcessOn(wk *worker) {
	wk.mu.Lock()
	b.InstID = wk.nextInst()
	wk.bundles[b.InstID] = b
	wk.mu.Unlock()

	V(2).Logf("processing %v %v %v on %v", b.InstID, b.PBDID, b.Generation, wk)

	// Tell the SDK to start processing the bundle.
	wk.InstReqs <- &fnpb.InstructionRequest{
		InstructionId: b.InstID,
		Request: &fnpb.InstructionRequest_ProcessBundle{
			ProcessBundle: &fnpb.ProcessBundleRequest{
				ProcessBundleDescriptorId: b.PBDID,
			},
		},
	}

	// Send the data one at a time, rather than batching.
	// TODO: Batch Data.
	for i, d := range b.InputData {
		V(3).Logf("XXX adding data to channel for %v", b.InstID)
		wk.DataReqs <- &fnpb.Elements{
			Data: []*fnpb.Elements_Data{
				{
					InstructionId: b.InstID,
					TransformId:   b.InputTransformID,
					Data:          d,
					IsLast:        i+1 == len(b.InputData),
				},
			},
		}
	}

	V(3).Logf("XXX waiting on data from %v", b.InstID)
	b.DataWait.Wait() // Wait until data is ready.
}

// collateByWindows takes the data and collates them into string keyed window maps.
// Uses generics to consolidate the repetitive window loops.
func collateByWindows[T any](data [][]byte, wDec exec.WindowDecoder, wEnc exec.WindowEncoder, ed func(io.Reader) T, join func(T, T) T) map[string]T {
	windowed := map[typex.Window]T{}
	for _, datum := range data {
		inBuf := bytes.NewBuffer(datum)
		for {
			ws, _, _, err := exec.DecodeWindowedValueHeader(wDec, inBuf)
			if err == io.EOF {
				break
			}
			// Get the element out, and window them properly.
			e := ed(inBuf)
			for _, w := range ws {
				windowed[w] = join(windowed[w], e)
			}
		}
	}
	output := make(map[string]T, len(windowed))
	var buf strings.Builder
	for w, v := range windowed {
		wEnc.EncodeSingle(w, &buf)
		output[buf.String()] = v
		buf.Reset()
	}
	return output
}

// stage represents a fused subgraph.
//
// TODO: do we guarantee that they are all
// the same environment at this point, or
// should that be handled later?
type stage struct {
	ID         string
	transforms []string

	outputCount      int
	inputTransformID string
	mainInputPCol    string
	desc             *fnpb.ProcessBundleDescriptor
	prepareSides     func(b *bundle, tid string, gen int)

	SinkToPCollection map[string]string
}

// makeBundles handles initial splitting among bundles.
func (s *stage) makeBundles(data *dataService, gen int) []*bundle {
	b := &bundle{
		PBDID:      s.ID,
		Generation: gen,

		InputTransformID: s.inputTransformID,

		// TODO Here's where we can split data for processing in multiple bundles.
		InputData: data.GetData(s.mainInputPCol, gen),
		Resp:      make(chan *fnpb.ProcessBundleResponse, 1),

		SinkToPCollection: s.SinkToPCollection,
		OutputCount:       s.outputCount,
	}
	b.DataWait.Add(b.OutputCount)
	s.prepareSides(b, s.transforms[0], gen)

	return []*bundle{b}
}

type dataDep struct {
	Gen int
}

// HandleStage must be run as a goroutine.
//
// It prepares and sends bundles for the stage to the worker.
// In particular it needs to handle all side inputs for the current stage of the current generation of data.
//
// TODO If it's unable to determine how to project the primary PCollection to the SideInput PCollection, it can
// ask the SDK with a map windows transform.
func (s *stage) HandleStage(wk *worker, toProcess <-chan dataDep, processed chan<- dataDep) {

}
