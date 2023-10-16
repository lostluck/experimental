package beam

import "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"

type edgeDoFn[E Element] struct {
	index     edgeIndex
	transform string

	dofn       Transform[E]
	ins, outs  map[string]nodeIndex
	parallelIn nodeIndex
	proc       processor

	opts beamopts.Struct
}

func (e *edgeDoFn[E]) inputs() map[string]nodeIndex {
	return e.ins
}

func (e *edgeDoFn[E]) outputs() map[string]nodeIndex {
	return e.outs
}

type bundleProcer interface {
	multiEdge

	// Make this a reflect.Type and avoid instance aliasing.
	// Then we can keep the graph around, for cheaper startup vs reparsing proto
	actualTransform() any
	dummyProcessor() processor
	options() beamopts.Struct
}

func (e *edgeDoFn[E]) actualTransform() any {
	return e.dofn
}

func (e *edgeDoFn[E]) options() beamopts.Struct {
	return e.opts
}

func (e *edgeDoFn[E]) dummyProcessor() processor {
	return e.proc
}

var _ bundleProcer = (*edgeDoFn[int])(nil)
