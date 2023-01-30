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
	"context"
	"fmt"
	"sort"
	"strings"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/lostluck/experimental/local/internal/urns"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

var capabilities = map[string]struct{}{
	urns.RequirementSplittableDoFn: {},
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

type job struct {
	key     string
	jobName string

	pipeline *pipepb.Pipeline
	options  *structpb.Struct

	// Management side concerns.
	// done      chan struct{} // closed when done.
	msgChan   chan string
	stateChan chan jobpb.JobState_Enum

	// Context used to terminate this job.
	rootCtx  context.Context
	cancelFn context.CancelFunc

	metrics metricsStore
}

func (j *job) String() string {
	return fmt.Sprintf("%v[%v]", j.key, j.jobName)
}

// Run starts the main thread fo executing this job.
// It's analoguous to the manager side process for a distributed pipeline.
// It will begin "workers"
func (j *job) run(ctx context.Context) {
	j.stateChan <- jobpb.JobState_STOPPED
	j.msgChan <- "starting " + j.String()
	j.stateChan <- jobpb.JobState_STARTING

	// In a "proper" runner, we'd iterate through all the
	// environments, and start up docker containers, but
	// here, we only want and need the go one, operating
	// in loopback mode.
	env := "go"
	wk := newWorker(env) // Cheating by having the worker id match the environment id.
	go wk.Serve()

	wkctx, cancelFn := context.WithCancel(ctx)
	defer func() {
		cancelFn()
	}()
	go j.runEnvironment(wkctx, env, wk)

	j.msgChan <- "running " + j.String()
	j.stateChan <- jobpb.JobState_RUNNING

	// Lets see what the worker does.
	executePipeline(wkctx, wk, j)
	j.msgChan <- "pipeline completed " + j.String()

	// Stop the worker.
	wk.Stop()

	j.msgChan <- "terminating " + j.String()
	j.stateChan <- jobpb.JobState_DONE
}

func (j *job) runEnvironment(ctx context.Context, env string, wk *worker) {
	// TODO fix broken abstraction.
	// We're starting a worker pool here, because that's the loopback environment.
	// It's sort of a mess, largely because of loopback, which has
	// a different flow from a provisioned docker container.
	e := j.pipeline.GetComponents().GetEnvironments()[env]
	switch e.GetUrn() {
	case urns.EnvExternal:
		ep := &pipepb.ExternalPayload{}
		if err := (proto.UnmarshalOptions{}).Unmarshal(e.GetPayload(), ep); err != nil {
			V(1).Logf("unmarshalling environment payload %v: %v", wk.ID, err)
		}
		externalEnvironment(ctx, ep, wk)
		V(1).Logf("%v for %v stopped", wk, j)
	default:
		logger.Fatalf("environment %v with urn %v unimplemented", env, e.GetUrn())
	}
}

func externalEnvironment(ctx context.Context, ep *pipepb.ExternalPayload, wk *worker) {
	conn, err := grpc.Dial(ep.GetEndpoint().GetUrl(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatalf("unable to dial sdk worker %v: %v", ep.GetEndpoint().GetUrl(), err)
	}
	defer conn.Close()
	pool := fnpb.NewBeamFnExternalWorkerPoolClient(conn)

	endpoint := &pipepb.ApiServiceDescriptor{
		Url: wk.Endpoint(),
	}

	pool.StartWorker(ctx, &fnpb.StartWorkerRequest{
		WorkerId:          wk.ID,
		ControlEndpoint:   endpoint,
		LoggingEndpoint:   endpoint,
		ArtifactEndpoint:  endpoint,
		ProvisionEndpoint: endpoint,
		Params:            nil,
	})

	// Job processing happens here, but orchestrated by other goroutines
	// This goroutine blocks until the context is cancelled, signalling
	// that the pool runner should stop the worker.
	<-ctx.Done()

	// Previous context cancelled so we need a new one
	// for this request.
	pool.StopWorker(context.Background(), &fnpb.StopWorkerRequest{
		WorkerId: wk.ID,
	})
}
