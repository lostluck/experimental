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
	"sort"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"golang.org/x/exp/maps"
)

// Shamelessly derived from the direct runner's processing.
// linkID is an identifier for a PCollection from a DoFn's perspective
// identifying the relative transform, the local identifier for the input
// and what the global pcollection of this link is.
type linkID struct {
	global string // Transform
	local  string // local input/output id
	pcol   string // What PCol, is This?
}

// preprocessor retains configuration for preprocessing the
// graph, such as special handling for lifted combiners or
// other configuration.
type preprocessor struct {
}

// preProcessGraph takes the graph and preprocesses for consumption in bundles.
// The outputs the topological sort of the transform ids, and maps of their successor and input transforms for lookup,
// as well as the parents of any given PCollection.
//
// These are how transforms are related in graph form, but not the specific bundles themselves, which will come later.
//
// Handles awareness of composite transforms and similar. Ultimately, after this point
// the graph stops being a hypergraph, with composite transforms being treated as
// "leaves" downstream as needed.
//
// This is where Combines can become lifted (if it makes sense, or is configured), and
// similar behaviors.
func (*preprocessor) preProcessGraph(ts map[string]*pipepb.PTransform) (topological []string, successors, inputs map[string][]linkID, pcolParents map[string]linkID) {
	// TODO - Recurse down composite transforms.
	// But lets just ignore composites for now. Leaves only.
	// We only need composites to do "smart" things with
	// combiners and SDFs.
	leaves := map[string]struct{}{}
	for tid, t := range ts {
		// Iterating through all the transforms to extract the leaves.
		if len(t.GetSubtransforms()) == 0 {
			leaves[tid] = struct{}{}
		} else {
			spec := t.GetSpec()
			if spec != nil {
				V(0).Logf("composite transform: %v has urn %v", t.GetUniqueName(), spec.GetUrn())
			}
		}
	}

	// Extract URNs for the given transform.

	keptLeaves := maps.Keys(leaves)
	topological = pipelinex.TopologicalSort(ts, keptLeaves)

	inputs = make(map[string][]linkID)     // TransformID -> []linkID (outputs they consume from the parent)
	successors = make(map[string][]linkID) // TransformID -> []linkID (successors inputs they generate for their children)

	// Each PCollection only has one parent, determined by transform outputs.
	// And since we only know pcollections, we need to map from pcol to parent transforms
	// so we can derive the transform successors map.
	pcolParents = make(map[string]linkID)

	// We iterate in the transform's topological order for determinism.
	for _, id := range topological {
		t := ts[id]
		for o, out := range t.GetOutputs() {
			pcolParents[out] = linkID{id, o, out}
		}
	}

	for _, id := range topological {
		t := ts[id]
		ins := t.GetInputs()
		ks := maps.Keys(ins)
		// Sort the inputs by local key id for determinism.
		sort.Strings(ks)
		for _, local := range ks {
			in := ins[local]
			from := pcolParents[in]
			successors[from.global] = append(successors[from.global], linkID{id, local, in})
			inputs[id] = append(inputs[id], from)
		}
	}
	V(2).Logf("TOPO:\n%+v", topological)
	V(2).Logf("SUCCESSORS:\n%+v", successors)
	V(2).Logf("INPUTS:\n%+v", inputs)

	return topological, successors, inputs, pcolParents
}
