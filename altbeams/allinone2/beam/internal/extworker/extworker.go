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

// Package extworker provides an external worker service and related utilities.
package extworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"sync"
	"time"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/harness"
	fnpb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/fnexecution_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// StartLoopback initializes a Loopback ExternalWorkerService, at the given port.
func StartLoopback(ctx context.Context, port int) (*Loopback, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, err
	}

	slog.InfoContext(ctx, "starting Loopback server", "endpoint", lis.Addr())
	grpcServer := grpc.NewServer()
	root, cancel := context.WithCancel(ctx)
	s := &Loopback{lis: lis, root: root, rootCancel: cancel, workers: map[string]context.CancelFunc{},
		grpcServer: grpcServer}
	fnpb.RegisterBeamFnExternalWorkerPoolServer(grpcServer, s)
	go grpcServer.Serve(lis)
	return s, nil
}

// Loopback implements fnpb.BeamFnExternalWorkerPoolServer
type Loopback struct {
	fnpb.UnimplementedBeamFnExternalWorkerPoolServer

	lis        net.Listener
	root       context.Context
	rootCancel context.CancelFunc

	mu      sync.Mutex
	workers map[string]context.CancelFunc

	grpcServer *grpc.Server
}

// StartWorker initializes a new worker harness, implementing BeamFnExternalWorkerPoolServer.StartWorker.
func (s *Loopback) StartWorker(ctx context.Context, req *fnpb.StartWorkerRequest) (*fnpb.StartWorkerResponse, error) {
	slog.DebugContext(ctx, "starting worker", "worker_id", req.GetWorkerId())
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workers == nil {
		return &fnpb.StartWorkerResponse{
			Error: "worker pool shutting down",
		}, nil
	}

	if _, ok := s.workers[req.GetWorkerId()]; ok {
		return &fnpb.StartWorkerResponse{
			Error: fmt.Sprintf("worker with ID %q already exists", req.GetWorkerId()),
		}, nil
	}
	if req.GetLoggingEndpoint() == nil {
		return &fnpb.StartWorkerResponse{Error: fmt.Sprintf("Missing logging endpoint for worker %v", req.GetWorkerId())}, nil
	}
	if req.GetControlEndpoint() == nil {
		return &fnpb.StartWorkerResponse{Error: fmt.Sprintf("Missing control endpoint for worker %v", req.GetWorkerId())}, nil
	}
	if req.GetLoggingEndpoint().Authentication != nil || req.GetControlEndpoint().Authentication != nil {
		return &fnpb.StartWorkerResponse{Error: "[BEAM-10610] Secure endpoints not supported."}, nil
	}

	ctx = WriteWorkerID(s.root, req.GetWorkerId())
	ctx, s.workers[req.GetWorkerId()] = context.WithCancel(ctx)

	opts := harnessOptions(ctx, req.GetProvisionEndpoint().GetUrl())

	go harness.Main(ctx, req.GetControlEndpoint().GetUrl(), opts)
	return &fnpb.StartWorkerResponse{}, nil
}

func harnessOptions(ctx context.Context, endpoint string) harness.Options {
	var opts harness.Options
	if endpoint == "" {
		return opts
	}
	info, err := ProvisionInfo(ctx, endpoint)
	if err != nil {
		slog.DebugContext(ctx, "error talking to provision service worker, using defaults", "error", err)
		return opts
	}

	opts.LoggingEndpoint = info.GetLoggingEndpoint().GetUrl()
	opts.StatusEndpoint = info.GetStatusEndpoint().GetUrl()
	opts.RunnerCapabilities = info.GetRunnerCapabilities()
	return opts
}

// StopWorker terminates a worker harness, implementing BeamFnExternalWorkerPoolServer.StopWorker.
func (s *Loopback) StopWorker(ctx context.Context, req *fnpb.StopWorkerRequest) (*fnpb.StopWorkerResponse, error) {
	slog.InfoContext(ctx, "stopping worker", "worker_id", req.GetWorkerId())
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workers == nil {
		// Worker pool is already shutting down, so no action is needed.
		return &fnpb.StopWorkerResponse{}, nil
	}
	if cancelfn, ok := s.workers[req.GetWorkerId()]; ok {
		cancelfn()
		delete(s.workers, req.GetWorkerId())
		return &fnpb.StopWorkerResponse{}, nil
	}
	return &fnpb.StopWorkerResponse{
		Error: fmt.Sprintf("no worker with id %q running", req.GetWorkerId()),
	}, nil

}

// Stop terminates the service and stops all workers.
func (s *Loopback) Stop(ctx context.Context) error {
	s.mu.Lock()

	slog.DebugContext(ctx, "stopping Loopback", "worker_count", len(s.workers))
	s.workers = nil
	s.rootCancel()

	// There can be a deadlock between the StopWorker RPC and GracefulStop
	// which waits for all RPCs to finish, so it must be outside the critical section.
	s.mu.Unlock()

	s.grpcServer.GracefulStop()
	return nil
}

// EnvironmentConfig returns the environment config for this service instance.
func (s *Loopback) EnvironmentConfig(context.Context) string {
	return fmt.Sprintf("localhost:%d", s.lis.Addr().(*net.TCPAddr).Port)
}

const idKey = "worker_id"

// WriteWorkerID write the worker ID to an outgoing gRPC request context. It
// merges the information with any existing gRPC metadata.
func WriteWorkerID(ctx context.Context, id string) context.Context {
	md := metadata.New(map[string]string{
		idKey: id,
	})
	if old, ok := metadata.FromOutgoingContext(ctx); ok {
		md = metadata.Join(md, old)
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// ProvisionInfo returns the runtime provisioning info for the worker.
func ProvisionInfo(ctx context.Context, endpoint string) (*fnpb.ProvisionInfo, error) {
	cc, err := Dial(ctx, endpoint, 2*time.Minute)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	client := fnpb.NewProvisionServiceClient(cc)

	resp, err := client.GetProvisionInfo(ctx, &fnpb.GetProvisionInfoRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}
	if resp.GetInfo() == nil {
		return nil, errors.New("empty manifest")
	}
	return resp.GetInfo(), nil
}

// Dial is a convenience wrapper over grpc.Dial. It can be overridden
// to provide a customized dialing behavior.
var Dial = DefaultDial

// DefaultDial is a dialer that specifies an insecure blocking connection with a timeout.
func DefaultDial(ctx context.Context, endpoint string, timeout time.Duration) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cc, err := grpc.DialContext(ctx, endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)))
	if err != nil {
		return nil, fmt.Errorf("failed to dial server at %v: %w", endpoint, err)
	}
	return cc, nil
}
