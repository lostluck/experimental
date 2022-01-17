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
	<-done //we will wait until all response is received
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
				logger.Fatalf("Data cannot receive %v", err)
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
