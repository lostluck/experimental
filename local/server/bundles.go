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
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

func dummyBundle(wk *worker, pipeline *pipepb.Pipeline) {
	// None of this should be in the "worker", it should be assigned to the
	// worker later when the worker is the correct environment.

	logger.Print("Pipeline:", prototext.Format(pipeline))

	ts := pipeline.GetComponents().GetTransforms()
	topo := pipelinex.TopologicalSort(ts, []string{"e1", "e2"})

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

	// lets see how we do?
	var impulseBuf bytes.Buffer
	byt, _ := exec.EncodeElement(exec.MakeElementEncoder(coder.NewBytes()), []byte("lostluck"))
	exec.EncodeWindowedValueHeader(
		exec.MakeWindowEncoder(coder.NewGlobalWindow()),
		window.SingleGlobalWindow,
		typex.EventTime(0),
		typex.NoFiringPane(),
		&impulseBuf,
	)
	impulseBuf.Write(byt)

	// This is probably something passed in instead.
	var parent, b *bundle
	for _, tid := range topo {
		t := ts[tid]
		switch t.GetEnvironmentId() {
		case "": // Runner Transforms
			urn := t.GetSpec().GetUrn()
			switch urn {
			case "beam:transform:impulse:v1":
				parent = &bundle{
					TransformID: tid, // TODO Fix properly for impulse.
					DataReceived: map[string][][]byte{
						tid: {impulseBuf.Bytes()},
					},
				}
			default:
				logger.Fatalf("unimplemented runner transform[%v]", urn)
			}

		case wk.ID:
			// Great! this is for this environment.
			urn := t.GetSpec().GetUrn()
			switch urn {
			case "beam:transform:pardo:v1":
				// This is where we take any parent, and turn it into input data.
				// and design the consuming bundle.
				instID, bundID := wk.nextInst(), wk.nextBund()

				// Get InputCoderID  (This is where we'd need to wrap things as windowed_value or not)
				// Get the input Pcollection for this transform
				if len(t.GetInputs()) > 1 {
					logger.Fatalf("side inputs not implemented, can't build bundle for: %v", prototext.Format(t))
				}
				// Extract the one PCollection ID from the inputs.
				var inPID string
				for _, c := range t.GetInputs() {
					inPID = c
				}
				inCol := pipeline.GetComponents().GetPcollections()[inPID]
				inCid := inCol.GetCoderId()
				inWCid := pipeline.GetComponents().GetWindowingStrategies()[inCol.GetWindowingStrategyId()].GetWindowCoderId()

				// Get OutputCoderId  (This is where we'd need to wrap things as windowed_value or not)
				// Get the input Pcollection for this transform
				if len(t.GetOutputs()) > 1 {
					logger.Fatalf("multiple outputs not implemented, can't build bundle for: %v", prototext.Format(t))
				}
				// Extract the one PCollection ID from the inputs.
				var outPID string
				for _, c := range t.GetOutputs() {
					outPID = c
				}
				outCol := pipeline.GetComponents().GetPcollections()[outPID]
				outCid := outCol.GetCoderId()
				outWCid := pipeline.GetComponents().GetWindowingStrategies()[outCol.GetWindowingStrategyId()].GetWindowCoderId()

				wInCid := "cwv_" + inPID
				wOutCid := "cwv_" + outPID

				coders := map[string]*pipepb.Coder{
					wInCid: {
						Spec: &pipepb.FunctionSpec{
							Urn: "beam:coder:windowed_value:v1",
						},
						ComponentCoderIds: []string{inCid, inWCid},
					},
					wOutCid: {
						Spec: &pipepb.FunctionSpec{
							Urn: "beam:coder:windowed_value:v1",
						},
						ComponentCoderIds: []string{outCid, outWCid},
					},
				}

				reconcileCoders(coders, pipeline.GetComponents().GetCoders())

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

				sinkPort := &fnpb.RemoteGrpcPort{
					CoderId: wOutCid,
					ApiServiceDescriptor: &pipepb.ApiServiceDescriptor{
						Url: wk.Endpoint(),
					},
				}
				sinkPortBytes, err := proto.Marshal(sinkPort)
				if err != nil {
					logger.Fatalf("bad port: %v", err)
				}

				desc := &fnpb.ProcessBundleDescriptor{
					Id: bundID,
					Transforms: map[string]*pipepb.PTransform{
						parent.TransformID: {
							Spec: &pipepb.FunctionSpec{
								Urn:     "beam:runner:source:v1",
								Payload: sourcePortBytes,
							},
							Outputs: map[string]string{
								"o1": inPID,
							},
						},
						tid: t, // The Transform to Execute!
						tid + "_sink": {
							Spec: &pipepb.FunctionSpec{
								Urn:     "beam:runner:sink:v1",
								Payload: sinkPortBytes,
							},
							Inputs: map[string]string{
								"i1": outPID,
							},
						},
					},
					WindowingStrategies: pipeline.GetComponents().GetWindowingStrategies(),
					Pcollections:        pipeline.GetComponents().GetPcollections(),
					Coders:              coders,
				}

				logger.Print("Desc:", instID, prototext.Format(desc))

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

				b = &bundle{
					BundID: bundID,

					TransformID: parent.TransformID, // Fix for Impulse.
					InputData:   parent.DataReceived[parent.TransformID],
					Resp:        make(chan *fnpb.ProcessBundleResponse, 1),

					DataReceived: make(map[string][][]byte),
				}
				b.DataWait.Add(1) // Only one sink.
			default:
				logger.Fatalf("unimplemented worker transform[%v]", urn)
			}
		default:
			logger.Fatalf("unknown environment[%v]", t.GetEnvironmentId())
		}
	}

	// Usually this should be called in a goroutine, but we're skipping for now.
	b.ProcessOn(wk)
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
func (b *bundle) ProcessOn(env *worker) {
	env.mu.Lock()
	b.InstID = env.nextInst()
	env.bundles[b.InstID] = b
	env.mu.Unlock()

	// Tell the SDK to start processing the bundle.
	env.InstReqs <- &fnpb.InstructionRequest{
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
		env.DataReqs <- &fnpb.Elements{
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
