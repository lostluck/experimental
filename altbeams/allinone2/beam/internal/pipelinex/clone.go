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

package pipelinex

import (
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

func shallowClonePipeline(p *pipepb.Pipeline) *pipepb.Pipeline {
	return &pipepb.Pipeline{
		Components:       shallowCloneComponents(p.GetComponents()),
		Requirements:     slices.Clone(p.GetRequirements()),
		RootTransformIds: slices.Clone(p.GetRootTransformIds()),
		DisplayData:      slices.Clone(p.DisplayData),
	}
}

func shallowCloneComponents(comp *pipepb.Components) *pipepb.Components {
	return &pipepb.Components{
		Transforms:          maps.Clone(comp.GetTransforms()),
		Pcollections:        maps.Clone(comp.GetPcollections()),
		WindowingStrategies: maps.Clone(comp.GetWindowingStrategies()),
		Coders:              maps.Clone(comp.GetCoders()),
		Environments:        maps.Clone(comp.GetEnvironments()),
	}
}

// ShallowClonePTransform makes a shallow copy of the given PTransform.
func ShallowClonePTransform(t *pipepb.PTransform) *pipepb.PTransform {
	if t == nil {
		return nil
	}
	return &pipepb.PTransform{
		UniqueName:    t.GetUniqueName(),
		Spec:          t.GetSpec(),
		DisplayData:   t.GetDisplayData(),
		Annotations:   t.GetAnnotations(),
		EnvironmentId: t.GetEnvironmentId(),
		// We mostly need the transforms and IOs to be cloned.
		Subtransforms: slices.Clone(t.GetSubtransforms()),
		Inputs:        maps.Clone(t.GetInputs()),
		Outputs:       maps.Clone(t.GetOutputs()),
	}
}
