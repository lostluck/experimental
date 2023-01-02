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
	"net"
	"sync"
	"sync/atomic"

	"io"
	"path"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A worker manages environments, sending them work
// that they're able to execute.
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

	data *dataService
}

// newWorker starts the server components of FnAPI Execution.
func newWorker(id string) *worker {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	wk := &worker{
		ID:     id,
		lis:    lis,
		server: grpc.NewServer(opts...),

		InstReqs: make(chan *fnpb.InstructionRequest, 10),
		DataReqs: make(chan *fnpb.Elements, 10),

		bundles: make(map[string]*bundle),

		data: &dataService{},
	}
	V(0).Logf("Serving Worker components on %v\n", wk.Endpoint())
	fnpb.RegisterBeamFnControlServer(wk.server, wk)
	fnpb.RegisterBeamFnDataServer(wk.server, wk)
	fnpb.RegisterBeamFnLoggingServer(wk.server, wk)
	fnpb.RegisterBeamFnStateServer(wk.server, wk)
	return wk
}

func (wk *worker) Endpoint() string {
	return wk.lis.Addr().String()
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
	V(1).Logf("stopping %v", wk)
	close(wk.InstReqs)
	close(wk.DataReqs)
	wk.server.Stop()
	wk.lis.Close()
	V(1).Logf("stopped %v", wk)
}

func (wk *worker) nextInst() string {
	return fmt.Sprintf("inst%05d", atomic.AddUint64(&wk.inst, 1))
}

func (wk *worker) nextBund() string {
	return fmt.Sprintf("bundle%05d", atomic.AddUint64(&wk.bund, 1))
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
			V(2).Logf("logging stream.Recv error: %v", err)
			return err
		}
		for _, l := range in.GetLogEntries() {
			if l.Severity > minsev {
				V(0).Logf("%v [%v]: %v", l.GetSeverity(), path.Base(l.GetLogLocation()), l.GetMessage())
			}
		}
	}
}

func (wk *worker) Control(ctrl fnpb.BeamFnControl_ControlServer) error {
	done := make(chan bool)
	go func() {
		for {
			resp, err := ctrl.Recv()
			if err == io.EOF {
				V(3).Logf("ctrl.Recv finished marking done")
				done <- true // means stream is finished
				return
			}
			if err != nil {
				switch status.Code(err) {
				case codes.Canceled: // Might ignore this all the time instead.
					V(3).Logf("ctrl.Recv Canceled: %v", err)
					done <- true // means stream is finished
					return
				default:
					V(0).Fatalf("ctrl.Recv error: %v", err)
				}
			}

			wk.mu.Lock()
			if b, ok := wk.bundles[resp.GetInstructionId()]; ok {
				// TODO. Better pipeline error handling.
				if resp.Error != "" {
					V(0).Fatalf("ctrl.Recv pipeline error: %v", resp.Error)
				}
				b.Resp <- resp.GetProcessBundle()
			} else {
				V(3).Logf("ctrl.Recv: %v", resp)
			}
			wk.mu.Unlock()
		}
	}()

	for req := range wk.InstReqs {
		ctrl.Send(req)
	}
	V(2).Logf("ctrl.Send finished waiting on done")
	V(2).Logf("Control Done %v", <-done)
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
					V(3).Logf("data.Recv Canceled: %v", err)
					return
				default:
					V(0).Fatalf("data.Recv error: %v", err)
				}
			}
			wk.mu.Lock()
			for _, d := range resp.GetData() {
				tID := d.GetTransformId()
				b, ok := wk.bundles[d.GetInstructionId()]
				if !ok {
					V(3).Logf("data.Recv for unknown bundle: %v", resp)
					continue
				}
				colID := b.SinkToPCollection[d.GetTransformId()]

				// There might not be data, eg. for side inputs, so we need to reconcile this elsewhere for
				// downstream side inputs.
				if len(d.GetData()) > 0 {
					wk.data.WriteData(colID, b.Generation, d.GetData())
				}
				if d.GetIsLast() {
					V(3).Logf("XXX done waiting on data from %v, with tID: %v", b.InstID, tID)
					b.DataWait.Done()
				}
			}
			wk.mu.Unlock()
		}
	}()

	for req := range wk.DataReqs {
		V(3).Logf("XXX data.Send for %v", req.GetData()[0].GetInstructionId())
		if err := data.Send(req); err != nil {
			V(3).Logf("data.Send error: %v", err)
		}
	}
	return nil
}

func (wk *worker) State(state fnpb.BeamFnState_StateServer) error {
	responses := make(chan *fnpb.StateResponse)
	go func() {
		// This go routine creates all responses to state requests from the worker
		// so we want to close the State handler when it's all done.
		defer close(responses)
		for {
			resp, err := state.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				switch status.Code(err) {
				case codes.Canceled:
					V(3).Logf("state.Recv Canceled: %v", err)
					return
				default:
					V(0).Fatalf("state.Recv error: %v", err)
				}
			}
			switch resp.GetRequest().(type) {
			case *fnpb.StateRequest_Get:
				// TODO: move data handling to be pcollection based.
				b := wk.bundles[resp.GetInstructionId()]
				key := resp.GetStateKey()

				var data [][]byte
				switch key.GetType().(type) {
				case *fnpb.StateKey_IterableSideInput_:
					ikey := key.GetIterableSideInput()
					wKey := string(ikey.GetWindow())
					data = b.IterableSideInputData[ikey.GetTransformId()][ikey.GetSideInputId()][wKey]

				case *fnpb.StateKey_MultimapSideInput_:
					mmkey := key.GetMultimapSideInput()
					wKey := string(mmkey.GetWindow())
					dKey := string(mmkey.GetKey())
					data = b.MultiMapSideInputData[mmkey.GetTransformId()][mmkey.GetSideInputId()][wKey][dKey]

				default:
					logger.Fatalf("unsupported StateKey Access type: %T: %v", key.GetType(), prototext.Format(key))
				}

				// Encode the runner iterable (no length, just consecutive elements), and send it out.
				// This is also where we can handle things like State Backed Iterables.
				var buf bytes.Buffer
				for _, value := range data {
					buf.Write(value)
				}

				responses <- &fnpb.StateResponse{
					Id: resp.GetId(),
					Response: &fnpb.StateResponse_Get{
						Get: &fnpb.StateGetResponse{
							Data: buf.Bytes(),
						},
					},
				}
			default:
				logger.Fatalf("unsupported StateRequest kind %T: %v", resp.GetRequest(), prototext.Format(resp))
			}
		}
	}()
	for resp := range responses {
		if err := state.Send(resp); err != nil {
			V(3).Logf("state.Send error: %v", err)
		}
	}
	return nil
}
