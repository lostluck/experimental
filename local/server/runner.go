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
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

var capabilities = map[string]struct{}{
	"beam:requirement:pardo:splittable_dofn:v1": {}, // Not actually implemented yet. Cheating.
}

func isSupported(requirements []string) error {
	var unsupported []string
	for _, req := range requirements {
		if _, ok := capabilities[req]; !ok {
			unsupported = append(unsupported, req)
		}
	}
	if len(unsupported) > 0 {
		sort.Strings(unsupported)
		return fmt.Errorf("local runner doesn't support the following required features: %v", strings.Join(unsupported, ","))
	}
	return nil
}

type jobstate struct {
	key     string
	jobName string

	pipeline *pipepb.Pipeline
	options  *structpb.Struct

	// Management side concerns.
	// done      chan struct{} // closed when done.
	msgChan   chan string
	stateChan chan jobpb.JobState_Enum

	// worker
	wk *worker
}

// Run starts the main thread fo executing this job.
// It's analoguous to the manager side process for a distributed pipeline.
// It will begin "workers"
func (j *jobstate) run(ctx context.Context) {
	j.msgChan = make(chan string, 100)
	j.stateChan = make(chan jobpb.JobState_Enum)

	j.stateChan <- jobpb.JobState_STOPPED
	j.msgChan <- "starting " + j.key + " " + j.jobName
	j.stateChan <- jobpb.JobState_STARTING

	j.wk = newWorker(j)
	go j.wk.Serve()

	// In a "proper" runner, we'd iterate through all the
	// environments, and start up docker containers, but
	// here, we only want and need the go one, operating
	// in loopback mode.
	wkctx, _ := context.WithCancel(context.TODO())
	//defer cancelFn()
	go j.startWorkerEnvironment(wkctx, "go", j.wk.Endpoint())

	j.msgChan <- "running " + j.key + " " + j.jobName
	j.stateChan <- jobpb.JobState_RUNNING

	// Lets see what the worker does.
	time.Sleep(5 * time.Second)
	// cancelFn()
	// Stop the worker.
	j.wk.Stop()

	j.msgChan <- "terminating " + j.key + " " + j.jobName
	j.stateChan <- jobpb.JobState_DONE
}

func (j *jobstate) startWorkerEnvironment(ctx context.Context, env, url string) {
	e := j.pipeline.GetComponents().GetEnvironments()[env]

	ep := &pipepb.ExternalPayload{}
	if err := (proto.UnmarshalOptions{}).Unmarshal(e.GetPayload(), ep); err != nil {
		logger.Printf("unmarshalling environment payload %v: %v", env, err)
	}
	logger.Printf("starting worker for env %v:\n %v", env, prototext.Format(ep))

	conn, err := grpc.Dial(ep.GetEndpoint().GetUrl(), grpc.WithInsecure())
	if err != nil {
		logger.Fatalf("unable to dial sdk worker %v: %v", ep.GetEndpoint().GetUrl(), err)
	}
	defer conn.Close()
	pool := fnpb.NewBeamFnExternalWorkerPoolClient(conn)

	endpoint := &pipepb.ApiServiceDescriptor{
		Url: url,
	}

	pool.StartWorker(ctx, &fnpb.StartWorkerRequest{
		WorkerId:          env,
		ControlEndpoint:   endpoint,
		LoggingEndpoint:   endpoint,
		ArtifactEndpoint:  endpoint,
		ProvisionEndpoint: endpoint,
		Params:            nil,
	})

	// Job processing happens here, but orchestrated by other goroutines?
	select {
	case <-ctx.Done():
		logger.Printf("context canceled! stopping worker")
	}

	// Previous context cancelled so we need a new one.
	pool.StopWorker(context.Background(), &fnpb.StopWorkerRequest{
		WorkerId: env,
	})
	logger.Printf("worker stopped")
}
