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
	"io"

	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
)

func (s *Server) ReverseArtifactRetrievalService(stream jobpb.ArtifactStagingService_ReverseArtifactRetrievalServiceServer) error {
	in, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	job := s.jobs[in.GetStagingToken()]

	envs := job.pipeline.GetComponents().GetEnvironments()
	for _, env := range envs {
		for _, dep := range env.GetDependencies() {
			V(2).Log("start GetArtifact:", dep.GetTypeUrn(), string(dep.GetTypePayload()))
			stream.Send(&jobpb.ArtifactRequestWrapper{
				Request: &jobpb.ArtifactRequestWrapper_GetArtifact{
					GetArtifact: &jobpb.GetArtifactRequest{
						Artifact: dep,
					},
				},
			})
			var count int
			for {
				in, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if in.IsLast {
					V(2).Log("finished GetArtifact:", dep.GetTypeUrn(), string(dep.GetTypePayload()), "received bytes:", count)
					break
				}
				// Here's where we go through each environment's artifacts.
				// We do nothing with them.
				switch req := in.GetResponse().(type) {
				case *jobpb.ArtifactResponseWrapper_GetArtifactResponse:
					count += len(req.GetArtifactResponse.GetData())
				case *jobpb.ArtifactResponseWrapper_ResolveArtifactResponse:
					V(0).Fatalf("Unexpected ResolveArtifactResponse to GetArtifact: %v", in.GetResponse())
				}

			}
		}
	}
	return nil
}
