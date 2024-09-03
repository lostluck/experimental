package beam

import (
	"fmt"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"
)

type mapper[I, O Element] struct {
	fn  func(I) O
	Key string

	Output Output[O]
}

func (fn *mapper[I, O]) ProcessBundle(dfc *DFC[I]) error {
	return dfc.Process(func(ec ElmC, in I) error {
		out := fn.fn(in)
		fn.Output.Emit(ec, out)
		return nil
	})
}

// lightweightInit is used by reconstruction.
func (fn *mapper[I, O]) lightweightInit(metadata map[string]any) {
	fn.fn = metadata[fn.Key].(func(I) O)
}

func Map[I, O Element](s *Scope, input Output[I], lambda func(I) O, opts ...beamopts.Options) Output[O] {
	ei := s.g.curEdgeIndex()
	// Store the transform in the metadata
	// with an index specific key.
	key := fmt.Sprintf("map%03d", ei)
	out := ParDo(s, input, &mapper[I, O]{fn: lambda, Key: key}, opts...)

	if s.g.edgeMeta == nil {
		s.g.edgeMeta = make(map[string]any)
	}
	s.g.edgeMeta[key] = lambda

	return out.Output
}

type lightweightIniter interface {
	lightweightInit(metadata map[string]any)
}
