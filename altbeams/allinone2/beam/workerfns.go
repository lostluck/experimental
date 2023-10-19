package beam

import (
	"context"
)

// workerfns.go is where SDK side transforms and their abstract graph representations live.
// They provide common utility that pipeline execution needs to correctly implement the beam model.
// Note that they are largely implemented in the same manner as user DoFns.

// multiplex and discard tranforms have no explicit edge, and are implicitly added to
// the execution graph when a PCollection is consumed by more than one transform, and zero
// transforms respectively.

// multiplex is a Transform inserted when a PCollection is used as an input into
// multiple downstream Transforms. The same element is emitted to each
// consuming emitter in order.
type multiplex[E Element] struct {
	Outs []Emitter[E]
}

func (fn *multiplex[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) bool {
		for _, out := range fn.Outs {
			out.Emit(ec, elm)
		}
		return true
	})
	return nil
}

// discard is a Transform inserted when a PCollection is unused by a downstream Transform.
// It performs a no-op. This allows execution graphs to avoid branches and checks whether
// a consumer is valid on each element.
type discard[E Element] struct{}

func (fn *discard[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) bool {
		return true
	})
	return nil
}
