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
	"strings"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
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
			runnerTransform(t, processed, tid)
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
				var dataParentID, parentID string
				// Only one set of data.
				for k := range parent.DataReceived {
					// The matches what the output bundle had.
					dataParentID = k
					parentID = strings.TrimSuffix(dataParentID, "_sink")
				}

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
				logger.Fatalf("unimplemented worker transform[%v]", urn)
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

func runnerTransform(t *pipepb.PTransform, processed chan *bundle, tid string) {
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
			logger.Printf("bundle for impulse %v sent", tid)
		}()
	default:
		logger.Fatalf("unimplemented runner transform[%v]", urn)
	}
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
	logger.Printf("ProcessBundle %v output: %v", b.InstID, b.DataReceived)

	// TODO figure out how to kick off the next Bundle with the data from here.
}
