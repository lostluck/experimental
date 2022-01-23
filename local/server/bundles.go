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

package server

import (
	"bytes"
	"io"
	"strings"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/ioutilx"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

var (
	impOnce  sync.Once
	impBytes []byte
)

func impulseBytes() []byte {
	impOnce.Do(func() {
		var buf bytes.Buffer
		byt, _ := exec.EncodeElement(exec.MakeElementEncoder(coder.NewBytes()), []byte("lostluck"))

		exec.EncodeWindowedValueHeader(
			exec.MakeWindowEncoder(coder.NewGlobalWindow()),
			window.SingleGlobalWindow,
			mtime.Now(),
			typex.NoFiringPane(),
			&buf,
		)
		buf.Write(byt)
		impBytes = buf.Bytes()
	})
	return impBytes
}

func executePipeline(wk *worker, pipeline *pipepb.Pipeline) {
	// logger.Print("Pipeline:", prototext.Format(pipeline))

	ts := pipeline.GetComponents().GetTransforms()

	// Basically need to figure out the "plan" at this point.
	// Long term: Take RootTransforms & TOPO sort them. (technically not necessary, but paranoid)
	//  - Recurse through composites from there: TOPO sort subtransforms
	//  - Eventually we have our ordered list of transforms.
	//  - Then we can figure out how to fuse transforms into bundles
	//    (producer-consumer & sibling fusion & environment awareness)
	//
	// But for now we figure out how to connect bundles together
	// First: default "impulse" bundle. "executed" by the runner.
	// Second: Derive bundle for a DoFn for the environment.

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
		processed <- nil
		logger.Printf("nil bundle sent")
		for b := range toProcess {
			b.ProcessOn(wk) // Blocks until finished.
			// Send back for dependency handling afterwards.
			processed <- b
		}
		close(processed)
	}()

	// TODO - Recurse down composite transforms.
	topo := pipelinex.TopologicalSort(ts, pipeline.GetRootTransformIds())
	for _, tid := range topo {
		// Block until the parent bundle is ready.
		parent := <-processed
		logger.Printf("making bundle for %v", tid)
		// TODO stash the parent bundle results in a map for
		// later access for non-linear pipelines.

		t := ts[tid]
		switch t.GetEnvironmentId() {
		case "": // Runner Transforms
			// Runner transforms are processed immeadiately.
			runnerTransform(t, tid, processed, parent, pipeline)
			continue
		case wk.ID:
			// Great! this is for this environment. // Broken abstraction.
			urn := t.GetSpec().GetUrn()
			switch urn {
			case "beam:transform:pardo:v1":
				// and design the consuming bundle.
				instID, bundID := wk.nextInst(), wk.nextBund()

				// Get WindowedValue Coders for the transform's input and output PCollections.
				inPID, wInCid, wInC := makeWindowedValueCoder(t, (*pipepb.PTransform).GetInputs, pipeline)
				outPID, wOutCid, wOutC := makeWindowedValueCoder(t, (*pipepb.PTransform).GetOutputs, pipeline)

				coders := map[string]*pipepb.Coder{
					wInCid:  wInC,
					wOutCid: wOutC,
				}

				reconcileCoders(coders, pipeline.GetComponents().GetCoders())

				// Extract the only data from the parent bundle.
				// Only one set of data.
				dataParentID, parentID := sourceIDs(parent)

				// TODO generate as many sinks as there are output transforms for the bundle
				// indicated by their local outputIDs not "_sink"
				sinkID := tid + "_sink"
				desc := &fnpb.ProcessBundleDescriptor{
					Id: bundID,
					Transforms: map[string]*pipepb.PTransform{
						parentID: sourceTransform(parentID, portFor(wInCid, wk), inPID),
						tid:      t, // The Transform to Execute!
						sinkID:   sinkTransform(sinkID, portFor(wOutCid, wk), outPID),
					},
					WindowingStrategies: pipeline.GetComponents().GetWindowingStrategies(),
					Pcollections:        pipeline.GetComponents().GetPcollections(),
					Coders:              coders,
				}

				logger.Printf("registering %v with %v:", desc.GetId(), instID) // , prototext.Format(desc))

				wk.InstReqs <- &fnpb.InstructionRequest{
					InstructionId: instID,
					Request: &fnpb.InstructionRequest_Register{
						Register: &fnpb.RegisterRequest{
							ProcessBundleDescriptor: []*fnpb.ProcessBundleDescriptor{
								desc,
							},
						},
					},
				}

				b := &bundle{
					BundID: bundID,

					TransformID: parentID,
					InputData:   parent.DataReceived[dataParentID],
					Resp:        make(chan *fnpb.ProcessBundleResponse, 1),

					DataReceived: make(map[string][][]byte),
				}
				b.DataWait.Add(1) // Only one sink.
				toProcess <- b
				continue
			default:
				logger.Fatalf("unimplemented worker transform[%v]: %v", urn, prototext.Format(t))
			}
		default:
			logger.Fatalf("unknown environment[%v]", t.GetEnvironmentId())
		}
	}
	// We're done with the pipeline!
	close(toProcess)
	b := <-processed // Drain the final bundle.

	logger.Printf("pipeline done! Final Bundle: %+v", b)
}

func sourceIDs(parent *bundle) (string, string) {
	var dataParentID, parentID string
	for k := range parent.DataReceived {
		dataParentID = k
		parentID = strings.TrimSuffix(dataParentID, "_sink")
	}
	return dataParentID, parentID
}

func makeWindowedValueCoder(t *pipepb.PTransform, puts func(*pipepb.PTransform) map[string]string, pipeline *pipepb.Pipeline) (string, string, *pipepb.Coder) {
	if len(puts(t)) > 1 {
		logger.Fatalf("side inputs/outputs not implemented, can't build bundle for: %v", prototext.Format(t))
	}

	var pID string
	for _, c := range puts(t) {
		pID = c
	}
	col := pipeline.GetComponents().GetPcollections()[pID]
	cID := col.GetCoderId()
	wcID := pipeline.GetComponents().GetWindowingStrategies()[col.GetWindowingStrategyId()].GetWindowCoderId()

	//pID, cID, wcID := extractOnlyPut(t, (*pipepb.PTransform).GetInputs, pipeline)
	// Produce ID for the Windowed Value Coder
	wvcID := "cwv_" + pID
	wInC := &pipepb.Coder{
		Spec: &pipepb.FunctionSpec{
			Urn: "beam:coder:windowed_value:v1",
		},
		ComponentCoderIds: []string{cID, wcID},
	}
	return pID, wvcID, wInC
}

func sourceTransform(parentID string, sourcePortBytes []byte, inPID string) *pipepb.PTransform {
	source := &pipepb.PTransform{
		UniqueName: parentID,
		Spec: &pipepb.FunctionSpec{
			Urn:     "beam:runner:source:v1",
			Payload: sourcePortBytes,
		},
		Outputs: map[string]string{
			"o1": inPID,
		},
	}
	return source
}

func sinkTransform(sinkID string, sinkPortBytes []byte, outPID string) *pipepb.PTransform {
	source := &pipepb.PTransform{
		UniqueName: sinkID,
		Spec: &pipepb.FunctionSpec{
			Urn:     "beam:runner:sink:v1",
			Payload: sinkPortBytes,
		},
		Inputs: map[string]string{
			"i1": outPID,
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

func runnerTransform(t *pipepb.PTransform, tid string, processed chan *bundle, parent *bundle, pipeline *pipepb.Pipeline) {
	urn := t.GetSpec().GetUrn()
	switch urn {
	case "beam:transform:impulse:v1":
		// To avoid conflicts with these single transform
		// bundles, we suffix the transform IDs.
		// These will be subbed out by the pardo stage.

		fakeTid := tid + "_sink"
		b := &bundle{
			TransformID: fakeTid,
			DataReceived: map[string][][]byte{
				fakeTid: {impulseBytes()},
			},
		}
		// Get back to the start of the bundle generation loop since the worker
		// doesn't need to do anything for this.
		go func() {
			processed <- b
		}()
	case "beam:transform:group_by_key:v1":
		dataParentID, _ := sourceIDs(parent)
		fakeTid := tid + "_sink" // The ID from which the consumer will read from.

		ws := windowingStrategy(pipeline, tid)
		kvc := kvcoder(pipeline, tid)

		wc := exec.MakeWindowDecoder(coder.NewGlobalWindow())
		kc := pipeline.GetComponents().GetCoders()[kvc.GetComponentCoderIds()[0]]
		ec := pipeline.GetComponents().GetCoders()[kvc.GetComponentCoderIds()[1]]

		b := &bundle{
			TransformID: fakeTid,
			DataReceived: map[string][][]byte{
				fakeTid: {gbkBytes(ws, wc, kc, ec, parent.DataReceived[dataParentID])},
			},
		}
		go func() {
			processed <- b
		}()
	default:
		logger.Fatalf("unimplemented runner transform[%v]", urn)
	}
}

func kvcoder(pipeline *pipepb.Pipeline, tid string) *pipepb.Coder {
	comp := pipeline.GetComponents()
	t := comp.GetTransforms()[tid]
	var inputPColID string
	for _, pcolID := range t.GetInputs() {
		inputPColID = pcolID
	}
	pcol := comp.GetPcollections()[inputPColID]
	return comp.GetCoders()[pcol.GetCoderId()]
}

// Gets the windowing strategy for the given transform ID.
func windowingStrategy(pipeline *pipepb.Pipeline, tid string) *pipepb.WindowingStrategy {
	comp := pipeline.GetComponents()
	t := comp.GetTransforms()[tid]
	var inputPColID string
	for _, pcolID := range t.GetInputs() {
		inputPColID = pcolID
	}
	pcol := comp.GetPcollections()[inputPColID]
	return comp.GetWindowingStrategies()[pcol.GetWindowingStrategyId()]
}

func gbkBytes(ws *pipepb.WindowingStrategy, wc exec.WindowDecoder, kc, vc *pipepb.Coder, toAggregate [][]byte) []byte {
	var outputTime func(typex.Window, mtime.Time) mtime.Time
	switch ws.GetOutputTime() {
	case pipepb.OutputTime_END_OF_WINDOW:
		outputTime = func(w typex.Window, et mtime.Time) mtime.Time {
			return w.MaxTimestamp()
		}
	default:
		logger.Fatalf("unsupported OutputTime behavior: %v", ws.GetOutputTime())
	}

	type keyTime struct {
		key    string
		w      typex.Window
		time   mtime.Time
		values [][]byte
	}
	// Map windows to a map of keys to a map of keys to time.
	// We ultimately emit the window, the key, the time, and the iterable of elements,
	// all contained in the final value.
	windows := map[typex.Window]map[string]keyTime{}
	pulldecoder := func(urn string) func(io.Reader) []byte {
		switch urn {
		case "beam:coder:bytes:v1", "beam:coder:string_utf8:v1", "beam:coder:length_prefix:v1":
			return func(r io.Reader) []byte {
				l, _ := coder.DecodeVarInt(r)
				b, _ := ioutilx.ReadN(r, int(l))
				return b
			}
		case "beam:coder:varint:v1":
			return func(r io.Reader) []byte {
				// Not efficient. We basically just want to pull the bytes
				// but not actually decode them.
				i, _ := coder.DecodeVarInt(r)
				var buf bytes.Buffer
				coder.EncodeVarInt(i, &buf)
				return buf.Bytes()
			}
		default:
			logger.Fatalf("unknown coder urn key: %v", urn)

		}
		return nil
	}
	kd := pulldecoder(kc.GetSpec().GetUrn())
	vd := pulldecoder(vc.GetSpec().GetUrn())

	// Right, need to get the key coder, and the element coder.
	// Cus I'll need to pull out anything the runner knows how to deal with.
	// And repeat.
	for _, data := range toAggregate {
		// Parse out each element's data, and repeat.
		buf := bytes.NewBuffer(data)
		for {
			ws, tm, _, err := exec.DecodeWindowedValueHeader(wc, buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				logger.Fatalf("can't decode windowed value header with %v: %v", wc, err)
			}

			key := string(kd(buf))
			value := vd(buf)
			for _, w := range ws {
				ft := outputTime(w, tm)
				wk, ok := windows[w]
				if !ok {
					wk = make(map[string]keyTime)
					windows[w] = wk
				}
				kt := wk[key]
				kt.time = ft
				kt.key = key
				kt.w = w
				kt.values = append(kt.values, value)
				wk[key] = kt
			}
		}
	}

	// Everything's aggregated!
	// Time to turn things into a windowed KV<K, Iterable<V>>

	var buf bytes.Buffer
	for _, w := range windows {
		for _, kt := range w {
			exec.EncodeWindowedValueHeader(
				exec.MakeWindowEncoder(coder.NewGlobalWindow()),
				[]typex.Window{kt.w},
				kt.time,
				typex.NoFiringPane(),
				&buf,
			)
			coder.EncodeStringUTF8(kt.key, &buf)
			coder.EncodeInt32(int32(len(kt.values)), &buf)
			for _, value := range kt.values {
				buf.Write(value)
			}
		}
	}
	return buf.Bytes()
}

// reconcileCoders, has coders is primed with initial coders.
func reconcileCoders(coders, base map[string]*pipepb.Coder) {
	var comps []string
	for _, c := range coders {
		for _, ccid := range c.GetComponentCoderIds() {
			if _, ok := coders[ccid]; !ok {
				// We don't have the coder yet, so in we go.
				comps = append(comps, ccid)
			}
		}
	}
	if len(comps) == 0 {
		return
	}
	for _, ccid := range comps {
		c, ok := base[ccid]
		if !ok {
			logger.Fatalf("unknown coder id during reconciliation")
		}
		coders[ccid] = c
	}
	reconcileCoders(coders, base)
}

// bundle represents an extant ProcessBundle instruction sent to an SDK worker.
type bundle struct {
	InstID string // ID for the instruction processing this bundle.
	BundID string // ID for the ProcessBundleDescriptor

	// InputData is data being sent to the SDK.
	TransformID string
	InputData   [][]byte

	// DataReceived is where all the data received from the SDK is put
	// separated by their TransformIDs.
	DataReceived map[string][][]byte
	DataWait     sync.WaitGroup
	Resp         chan *fnpb.ProcessBundleResponse

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

	logger.Printf("processing %v %v on %v", b.InstID, b.BundID, wk)

	// Tell the SDK to start processing the bundle.
	wk.InstReqs <- &fnpb.InstructionRequest{
		InstructionId: b.InstID,
		Request: &fnpb.InstructionRequest_ProcessBundle{
			ProcessBundle: &fnpb.ProcessBundleRequest{
				ProcessBundleDescriptorId: b.BundID,
			},
		},
	}

	// Send the data one at a time, rather than batching.
	// TODO: Batch Data.
	for i, d := range b.InputData {
		wk.DataReqs <- &fnpb.Elements{
			Data: []*fnpb.Elements_Data{
				{
					InstructionId: b.InstID,
					TransformId:   b.TransformID,
					Data:          d,
					IsLast:        i+1 == len(b.InputData),
				},
			},
		}
	}

	b.DataWait.Wait() // Wait until data is done.

	// logger.Printf("ProcessBundle %v done! Resp: %v", b.InstID, prototext.Format(<-b.Resp))
	// logger.Printf("ProcessBundle %v output: %v", b.InstID, b.DataReceived)

	// TODO figure out how to kick off the next Bundle with the data from here.
}
