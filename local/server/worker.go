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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// This is where worker orchestration happen.
type worker struct {
	fnpb.UnimplementedBeamFnControlServer
	fnpb.UnimplementedBeamFnDataServer
	fnpb.UnimplementedBeamFnStateServer
	fnpb.UnimplementedBeamFnLoggingServer

	// Server management
	lis    net.Listener
	server *grpc.Server

	// BackReference to Job.
	job *jobstate

	// These are the ID sources
	inst, bund uint64

	descs map[string]*fnpb.ProcessBundleDescriptor

	instReqs chan *fnpb.InstructionRequest
	dataReqs chan *fnpb.Elements

	mu      sync.Mutex
	bundles map[string]*bundle // Bundles keyed by InstructionID
}

// newWorker starts the server components of FnAPI Execution.
func newWorker(job *jobstate) *worker {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	wk := &worker{
		lis:    lis,
		server: grpc.NewServer(opts...),
		job:    job,

		instReqs: make(chan *fnpb.InstructionRequest, 10),
		dataReqs: make(chan *fnpb.Elements, 10),

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

// Stop the GRPC server.
func (wk *worker) Stop() {
	close(wk.instReqs)
	//wk.server.Stop()
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

func (wk *worker) dummyBundle() {
	// First instruction, register a descriptor.
	instID, bundID := wk.nextInst(), wk.nextBund()
	// Lets try and get 2 things working.

	//	logger.Print(prototext.Format(wk.job.pipeline.GetComponents()))

	port := &fnpb.RemoteGrpcPort{
		CoderId: "c2",
		ApiServiceDescriptor: &pipepb.ApiServiceDescriptor{
			Url: wk.Endpoint(),
		},
	}
	portBytes, err := proto.Marshal(port)
	if err != nil {
		logger.Fatalf("bad port: %v", err)
	}

	wk.instReqs <- &fnpb.InstructionRequest{
		InstructionId: instID,
		Request: &fnpb.InstructionRequest_Register{
			Register: &fnpb.RegisterRequest{
				ProcessBundleDescriptor: []*fnpb.ProcessBundleDescriptor{
					{
						Id: bundID,
						Transforms: map[string]*pipepb.PTransform{
							"source": {
								Spec: &pipepb.FunctionSpec{
									Urn:     "beam:runner:source:v1",
									Payload: portBytes,
								},
								Outputs: map[string]string{
									"o1": "p0",
								},
							},
							"sink": {
								Spec: &pipepb.FunctionSpec{
									Urn:     "beam:runner:sink:v1",
									Payload: portBytes,
								},
								Inputs: map[string]string{
									"i1": "p0",
								},
							},
						},
						Pcollections: map[string]*pipepb.PCollection{
							"p0": {
								CoderId: "c2",
							},
						},
						Coders: map[string]*pipepb.Coder{
							"c0": {
								Spec: &pipepb.FunctionSpec{
									Urn: "beam:coder:string_utf8:v1",
								},
							},
							"c1": {
								Spec: &pipepb.FunctionSpec{
									Urn: "beam:coder:global_window:v1",
								},
							},
							"c2": {
								Spec: &pipepb.FunctionSpec{
									Urn: "beam:coder:windowed_value:v1",
								},
								ComponentCoderIds: []string{"c0", "c1"},
							},
						},
					},
				},
			},
		},
	}
	// lets just see how we do?
	byt, _ := exec.EncodeElement(exec.MakeElementEncoder(coder.NewString()), "pants")
	buf := bytes.NewBuffer(byt)
	exec.EncodeWindowedValueHeader(
		exec.MakeWindowEncoder(coder.NewGlobalWindow()),
		window.SingleGlobalWindow,
		typex.EventTime(0),
		typex.NoFiringPane(),
		buf,
	)

	instID = wk.nextInst()
	b := &bundle{
		InstID: instID,
		BundID: bundID,

		TransformID: "source",
		InputData: [][]byte{
			buf.Bytes(),
		},
		Resp: make(chan *fnpb.ProcessBundleResponse, 1),

		DataReceived: make(map[string][][]byte),
	}
	b.DataWait.Add(1) // Only one sink.

	wk.mu.Lock()
	wk.bundles[instID] = b
	wk.mu.Unlock()

	// Usually this should be called in a goroutine, but we're skipping for now.
	wk.Process(b)
}

func (wk *worker) Control(ctrl fnpb.BeamFnControl_ControlServer) error {
	done := make(chan bool)
	go func() {
		for {
			resp, err := ctrl.Recv()
			if err == io.EOF {
				done <- true //means stream is finished
				return
			}
			if err != nil {
				logger.Fatalf("cannot receive %v", err)
			}

			wk.mu.Lock()
			if b, ok := wk.bundles[resp.GetInstructionId()]; ok {
				b.Resp <- resp.GetProcessBundle()
			} else {
				logger.Printf("Control Resp received: %v", resp)
			}
			wk.mu.Unlock()
		}
	}()

	for req := range wk.instReqs {
		ctrl.Send(req)
	}
	<-done // we will wait until all response is received
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
				logger.Fatalf("Data cannot receive error: %v", err)
			}
			for _, d := range resp.GetData() {
				wk.mu.Lock()
				tID := d.GetTransformId()
				b, ok := wk.bundles[d.GetInstructionId()]
				if !ok {
					logger.Printf("Data Resp received for unknown bundle: %v", resp)
					continue
				}
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

	for req := range wk.dataReqs {
		data.Send(req)
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

// Execute the given bundle asynchronously.
//
// Assumes the bundle is initialized (all maps are non-nil, and data waitgroup is set.)
// Assumes the bundle descriptor is already registered.
func (wk *worker) Process(b *bundle) {
	// Tell the SDK to start processing the bundle.
	wk.instReqs <- &fnpb.InstructionRequest{
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
		wk.dataReqs <- &fnpb.Elements{
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

	logger.Printf("ProcessBundle %v done! Resp: %v", b.InstID, prototext.Format(<-b.Resp))
	logger.Printf("ProcessBundle %v Data: %v", b.InstID, string(b.DataReceived["sink"][0]))

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
