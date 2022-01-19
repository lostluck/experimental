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
	"fmt"
	"io"
	"net"
	"path"
	"sync"
	"sync/atomic"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// This is where worker orchestration happen.
type worker struct {
	fnpb.UnimplementedBeamFnControlServer
	fnpb.UnimplementedBeamFnDataServer
	fnpb.UnimplementedBeamFnStateServer
	fnpb.UnimplementedBeamFnLoggingServer

	ID string

	// Server management
	lis    net.Listener
	server *grpc.Server

	// These are the ID sources
	inst, bund uint64

	// descs map[string]*fnpb.ProcessBundleDescriptor

	InstReqs chan *fnpb.InstructionRequest
	DataReqs chan *fnpb.Elements

	mu      sync.Mutex
	bundles map[string]*bundle // Bundles keyed by InstructionID
}

// newWorker starts the server components of FnAPI Execution.
func newWorker(job *jobstate, env string) *worker {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	wk := &worker{
		ID:     env,
		lis:    lis,
		server: grpc.NewServer(opts...),

		InstReqs: make(chan *fnpb.InstructionRequest, 10),
		DataReqs: make(chan *fnpb.Elements, 10),

		bundles: make(map[string]*bundle),
	}
	logger.Printf("Serving Worker components on %v\n", wk.Endpoint())
	fnpb.RegisterBeamFnControlServer(wk.server, wk)
	fnpb.RegisterBeamFnDataServer(wk.server, wk)
	fnpb.RegisterBeamFnLoggingServer(wk.server, wk)
	fnpb.RegisterBeamFnStateServer(wk.server, wk)
	return wk
}

func (w *worker) Endpoint() string {
	return w.lis.Addr().String()
}

// Serve serves on the started listener. Blocks.
func (wk *worker) Serve() {
	wk.server.Serve(wk.lis)
}

func (wk *worker) String() string {
	return "worker[" + wk.ID + "]"
}

// Stop the GRPC server.
func (wk *worker) Stop() {
	close(wk.InstReqs)
	logger.Printf("stopping %v", wk)
	wk.server.Stop()
}

// TODO set logging level.
var minsev = fnpb.LogEntry_Severity_DEBUG

func (wk *worker) Logging(stream fnpb.BeamFnLogging_LoggingServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		for _, l := range in.GetLogEntries() {
			if l.Severity > minsev {
				logger.Printf("%v [%v]: %v", l.GetSeverity(), path.Base(l.GetLogLocation()), l.GetMessage())
			}
		}
	}
}

func (wk *worker) nextInst() string {
	return fmt.Sprintf("inst%05d", atomic.AddUint64(&wk.inst, 1))
}

func (wk *worker) nextBund() string {
	return fmt.Sprintf("bundle%05d", atomic.AddUint64(&wk.bund, 1))
}

func (wk *worker) dummyBundle(pipeline *pipepb.Pipeline) {
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

	// lets just see how we do?
	byt, _ := exec.EncodeElement(exec.MakeElementEncoder(coder.NewBytes()), []byte("pants"))
	impulseBuf := bytes.NewBuffer(byt)
	exec.EncodeWindowedValueHeader(
		exec.MakeWindowEncoder(coder.NewGlobalWindow()),
		window.SingleGlobalWindow,
		typex.EventTime(0),
		typex.NoFiringPane(),
		impulseBuf,
	)

	// This is probably something passed in instead.
	var parent, b *bundle
	for _, tid := range topo {
		t := ts[tid]
		env := t.GetEnvironmentId()
		switch env {
		case "": // Runner Transforms
			urn := t.GetSpec().GetUrn()
			switch urn {
			case "beam:transform:impulse:v1":
				parent = &bundle{
					TransformID: tid, // TODO Fix properly for impulse.
					DataReceived: map[string][][]byte{
						tid: [][]byte{impulseBuf.Bytes()},
					},
				}
			default:
				logger.Fatalf("unimplemented runner transform[%v]", urn)
			}

		case "go":
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
			logger.Fatalf("unknown environment[%v]", env)
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

func (wk *worker) Control(ctrl fnpb.BeamFnControl_ControlServer) error {
	done := make(chan bool)
	go func() {
		for {
			resp, err := ctrl.Recv()
			if err == io.EOF {
				done <- true // means stream is finished
				return
			}
			if err != nil {
				switch status.Code(err) {
				case codes.Canceled: // Might ignore this all the time instead.
					logger.Printf("ctrl.Recv Canceled: %v", err)
					return
				default:
					logger.Fatalf("ctrl.Recv error: %v", err)
				}
			}

			wk.mu.Lock()
			if b, ok := wk.bundles[resp.GetInstructionId()]; ok {
				// TODO. Better pipeline error handling.
				// No retries. likely ever.
				if resp.Error != "" {
					logger.Fatal(resp.Error)
				}
				b.Resp <- resp.GetProcessBundle()
			} else {
				logger.Printf("ctrl.Recv: %v", resp)
			}
			wk.mu.Unlock()
		}
	}()

	for req := range wk.InstReqs {
		ctrl.Send(req)
	}
	logger.Printf("closing control channel")
	return nil
}

func (wk *worker) Data(data fnpb.BeamFnData_DataServer) error {
	go func() {
		for {
			resp, err := data.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				switch status.Code(err) {
				case codes.Canceled:
					logger.Printf("data.Recv Canceled: %v", err)
					return
				default:
					logger.Fatalf("data.Recv error: %v", err)
				}
			}
			for i, d := range resp.GetData() {
				wk.mu.Lock()
				tID := d.GetTransformId()
				b, ok := wk.bundles[d.GetInstructionId()]
				if !ok {
					logger.Printf("data.Recv for unknown bundle: %v", resp)
					continue
				}
				logger.Printf("XXXXX Recv Data[%v]%v", i, d)
				output := b.DataReceived[tID]
				output = append(output, d.GetData())
				b.DataReceived[tID] = output
				if d.GetIsLast() {
					b.DataWait.Done()
				}
				wk.mu.Unlock()
			}
		}
	}()

	for req := range wk.DataReqs {
		logger.Printf("XXXXX Send Data - %v", req)
		if err := data.Send(req); err != nil {
			logger.Printf("data.Send error: %v", err)
		}
	}
	return nil
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

// Notes to myself: 2022-01-18
// Before any of the actual pipeline execution, I think sorting out
// *proper clean* worker shutdown behavior is likely best. As it stands
// things shudwon and tests fail, regardless of what the pipeline is doing.
//
// Also, we can just do an unoptimized runner to start, one DoFn at a time,
// and then work on an optimized version. Much simpler.
//
// By the end of the evening:
// I have a successfully stopping worker & SDK. Turns out shutting down
// properly is the best move. Huzzah.
//
// Next up, handling "Impulse" (which uses a global window non firing pane empty byte.),
// and a DoFn, and getting the DoFn output from the sink.
// (Everything should be sunk, and not discarded.)

// Notes to myself: 2022-01-17
// At this point I have a runner that actuates the SDK harness
// and sends & receives data.
// However it's presently all a lie, with the source and sink hard coded in.
//
// So there are a few things to do:
// 1. Persist and organize ProcessBundle instructions, and make managing them
// a bit easier to handle.
//
// 2. Start Graph Dissection.
// This is the harder ongoing work, since I need to take the pipeline and
// have the code *plan* how it's breaking things into bundles.
// We can't simply take the direct runner code exactly, since it's geared up
// as a single bundle runner. This won't be. It'll have multiple bundles.
// However, it might only ever have/use one per "stage", and only run one
// at a time for the moment.
// But that might be too tricky without...
//
// 3. Targetted graph subsets.
// The ray tracer is certainly too complicated to start with at present.
// Lets instead go with smaller pipelines with purpose.
// As unit tests of a sort.
// Simplest: Impulse -> DoFn -> Sink
// Sequence: Impulse -> DoFn -> DoFn -> Sink
// Split 1: Impulse -> Sink A
//                |--> Sink B
// Split 2: Impulse -> DoFn -> Sink A
//                        |--> Sink B
// Split 2: Impulse -> DoFn -> Sink A
//                        |--> Sink B
// Grouping: Impulse -> DoFn -> GBK -> DoFn -> Sink
//   Probably involves a bit of coder finagling to extract keys and re-provide as inputs and the like.
// Combiner Lifting
//  This requires understanding the combiner components, and their IOs and handling them properly.
//  But vanilla combines are handled by the SDK harness.
// SplittableDoFns
//  This requires similarly understanding the components and calling them out.
//  However also handled by the SDK harness.
// None of the above Sinks are actual sinks since those don't have a representation in the model.
//
// 4. Then of course theres Metrics and progress, which is required for more complex
// pipelines. Need to collect them for all the things.
