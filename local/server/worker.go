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
	"fmt"
	"io"
	"net"
	"path"
	"sync/atomic"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/grpc"
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

	inst, bund uint64

	instReqs chan *fnpb.InstructionRequest
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

		instReqs: make(chan *fnpb.InstructionRequest, 100),
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
			logger.Printf("Control Resp received: %v", resp)
		}
	}()

	// First instruction, register a descriptor.
	go func() {
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
										Urn: "beam:coder:bytes:v1",
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
		instID = wk.nextInst()
		wk.instReqs <- &fnpb.InstructionRequest{
			InstructionId: instID,
			Request: &fnpb.InstructionRequest_ProcessBundle{
				ProcessBundle: &fnpb.ProcessBundleRequest{
					ProcessBundleDescriptorId: bundID,
				},
			},
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
	done := make(chan bool)
	go func() {
		for {
			resp, err := data.Recv()
			if err == io.EOF {
				done <- true //means stream is finished
				return
			}
			if err != nil {
				logger.Fatalf("Data cannot receive error: %v", err)
			}
			logger.Printf("Data Resp received: %v", resp)
		}
	}()

	data.Send(&fnpb.Elements{
		Data: []*fnpb.Elements_Data{
			{
				InstructionId: "inst00002",
				TransformId:   "source",
				Data:          []byte{0, 0},
				IsLast:        true,
			},
		},
	})
	<-done //we will wait until all response is received
	return nil
}

// Notes to myself: 2022-01-18
// Before any of the actual pipeline execution, I think sorting out
// *proper clean* worker shutdown behavior is likely best. As it stands
// things shudwon and tests fail, regardless of what the pipeline is doing.

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
