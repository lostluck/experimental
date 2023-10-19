package beam

import (
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
)

// GBK produces an output PCollection of grouped values.
func GBK[K Keys, V Element](s *Scope, input Emitter[KV[K, V]], opts ...Options) Emitter[KV[K, Iter[V]]] {
	if s.g.consumers == nil {
		s.g.consumers = map[nodeIndex][]edgeIndex{}
	}

	var opt beamopts.Struct
	opt.Join(opts...)

	edgeID := s.g.curEdgeIndex()
	nodeID := s.g.curNodeIndex()
	s.g.consumers[input.globalIndex] = append(s.g.consumers[input.globalIndex], edgeID)

	s.g.edges = append(s.g.edges, &edgeGBK[K, V]{index: edgeID, input: input.globalIndex, output: nodeID, opts: opt})
	s.g.nodes = append(s.g.nodes, &typedNode[KV[K, Iter[V]]]{index: nodeID, parentEdge: edgeID})

	// We do all the expected connections here.
	// Side inputs, are put on the side input at the DoFn creation time being passed in.
	return Emitter[KV[K, Iter[V]]]{globalIndex: nodeID}
}

// EdgeGBK represents a Group By Key transform.
type edgeGBK[K Keys, V Element] struct {
	index edgeIndex

	input, output nodeIndex
	opts          beamopts.Struct
}

// inputs for GBKs are one.
func (e *edgeGBK[K, V]) inputs() map[string]nodeIndex {
	return map[string]nodeIndex{"i0": e.input}
}

// inputs for GBKs are one.
func (e *edgeGBK[K, V]) outputs() map[string]nodeIndex {
	return map[string]nodeIndex{"o0": e.output}
}

func (e *edgeGBK[K, V]) toProtoParts(translateParams) (spec *pipepb.FunctionSpec, envID, name string) {
	spec = &pipepb.FunctionSpec{Urn: "beam:transform:group_by_key:v1"}
	envID = "" // Runner transforms are left blank.
	name = "GroupByKey"
	return spec, envID, name
}

var _ protoDescMultiEdge = (*edgeGBK[int, int])(nil)
