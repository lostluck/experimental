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
	"net"
	"sync"

	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"

	"google.golang.org/grpc"
)

type Server struct {
	jobpb.UnimplementedJobServiceServer
	jobpb.UnimplementedArtifactStagingServiceServer

	// Server management
	lis    net.Listener
	server *grpc.Server

	// Job Management
	mu    sync.Mutex
	index uint32
	jobs  map[string]*jobstate
}

// NewServer acquires the indicated port.
func NewServer(port int) *Server {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
	}
	s := &Server{
		lis:  lis,
		jobs: make(map[string]*jobstate),
	}
	logger.Printf("Serving JobManagement on %v\n", s.Endpoint())
	var opts []grpc.ServerOption
	s.server = grpc.NewServer(opts...)
	jobpb.RegisterJobServiceServer(s.server, s)
	jobpb.RegisterArtifactStagingServiceServer(s.server, s)
	return s
}

func (s *Server) Endpoint() string {
	return s.lis.Addr().String()
}

// Serve serves on the started listener. Blocks.
func (s *Server) Serve() {
	s.server.Serve(s.lis)
}

// Stop the GRPC server.
func (s *Server) Stop() {
	s.server.GracefulStop()
}
