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
	transformHandlers map[string]transformHandler
}

type transformHandler interface {
	// TransformUrns returns the Beam URNs that this handler deals with for preprocessing.
	TransformUrns() []string
	// HandleTransform takes a PTransform proto and returns a set of new Components, and a list of
	// transformIDs leaves to remove and ignore from graph processing.
	HandleTransform(tid string, t *pipepb.PTransform, comps *pipepb.Components) (*pipepb.Components, []string)
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
func (p *preprocessor) preProcessGraph(comps *pipepb.Components) (topological []string, successors, inputs map[string][]linkID, pcolParents map[string]linkID) {
	ts := comps.GetTransforms()

	// TODO move this out of this part of the pre-processor?
	leaves := map[string]struct{}{}
	ignore := map[string]struct{}{}
	for tid, t := range ts {
		if _, ok := ignore[tid]; ok {
			continue
		}

		spec := t.GetSpec()
		if spec == nil {
			V(0).Logf("transform %v %v is missing a spec", tid, t.GetUniqueName())
			continue
		}

		// Composite Transforms basically means needing to remove the "leaves" from the
		// handling set, and producing the new sub component transforms. The top level
		// composite should have enough information to produce the new sub transforms.
		// In particular, the inputs and outputs need to all be connected and matched up
		// so the topological sort still works out.
		h := p.transformHandlers[spec.GetUrn()]
		if h == nil {

			// If there's an unknown urn, and it's not composite, simply add it to the leaves.
			if len(t.GetSubtransforms()) == 0 {
				leaves[tid] = struct{}{}
			} else {
				V(0).Logf("composite transform %v has unknown urn %v", t.GetUniqueName(), spec.GetUrn())
			}
			continue
		}

		subs, toRemove := h.HandleTransform(tid, t, comps)

		// Clear out unnecessary leaves from this composite for topological sort handling.
		for _, key := range toRemove {
			ignore[key] = struct{}{}
			delete(leaves, key)
		}

		// ts should be a clone, so we should be able to add new transforms into the map.
		for tid, t := range subs.GetTransforms() {
			leaves[tid] = struct{}{}
			ts[tid] = t
		}
		for cid, c := range subs.GetCoders() {
			comps.GetCoders()[cid] = c
		}
		for nid, n := range subs.GetPcollections() {
			comps.GetPcollections()[nid] = n
		}
		// It's unlikely for these to change, but better to handle them now, to save a headache later.
		for wid, w := range subs.GetWindowingStrategies() {
			comps.GetWindowingStrategies()[wid] = w
		}
		for envid, env := range subs.GetEnvironments() {
			comps.GetEnvironments()[envid] = env
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
