package beam

import (
	"context"
	"fmt"

	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
)

// Flatten joins together multiple Emitters of the same type into a single Emitter for
// downstream consumption.
func Flatten[E Element](s *Scope, inputs ...Emitter[E]) Emitter[E] {
	edgeID := s.g.curEdgeIndex()
	nodeID := s.g.curNodeIndex()
	if s.g.consumers == nil {
		s.g.consumers = map[nodeIndex][]edgeIndex{}
	}
	var ins []nodeIndex
	for _, emt := range inputs {
		in := emt.globalIndex
		ins = append(ins, in)
		s.g.consumers[in] = append(s.g.consumers[in], edgeID)
	}
	s.g.edges = append(s.g.edges, &edgeFlatten[E]{index: edgeID, ins: ins, output: nodeID})
	s.g.nodes = append(s.g.nodes, &typedNode[E]{index: nodeID, parentEdge: edgeID})

	// We do all the expected connections here.
	// Side inputs, are put on the side input at the DoFn creation time being passed in.
	return Emitter[E]{globalIndex: nodeID}
}

// edgeFlatten represents a Flatten transform.
type edgeFlatten[E Element] struct {
	index     edgeIndex
	transform string

	ins    []nodeIndex
	output nodeIndex

	// exec build time instances.
	instance *flatten[E]
	procs    []processor
}

// inputs for flattens are plural
func (e *edgeFlatten[E]) inputs() map[string]nodeIndex {
	ins := map[string]nodeIndex{}
	for i, input := range e.ins {
		ins[fmt.Sprintf("i%d", i)] = input
	}
	return ins
}

// outputs for Flattens are one.
func (e *edgeFlatten[E]) outputs() map[string]nodeIndex {
	return map[string]nodeIndex{"Output": e.output}
}

func (e *edgeFlatten[E]) toProtoParts(translateParams) (spec *pipepb.FunctionSpec, envID, name string) {
	spec = &pipepb.FunctionSpec{Urn: "beam:transform:flatten:v1"}
	envID = "" // Runner transforms are left blank.
	name = "Flatten"
	return spec, envID, name
}

func (e *edgeFlatten[E]) flatten() (string, any, []processor, bool) {
	var first bool
	if e.instance == nil {
		first = true
		e.instance = &flatten[E]{
			Output: Emitter[E]{globalIndex: e.output},
		}
		e.procs = []processor{e.instance.Output.newDFC(e.output)}
	}
	return e.transform, e.instance, e.procs, first
}

type flattener interface {
	protoDescMultiEdge
	// Returns the flatten instance, the downstream processors, and if this was the first call to dedup setting downstream consumers
	flatten() (string, any, []processor, bool)
}

var _ flattener = (*edgeFlatten[int])(nil)

// flatten implements an SDK side flatten, being a single point to funnel together
// multiple outputs together.
type flatten[E Element] struct {
	Output Emitter[E]
}

func (fn *flatten[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) error {
		fn.Output.Emit(ec, elm)
		return nil
	})
	return nil
}
