package beam

import (
	"context"
)

// direct.go is where direct runner or SDK side transforms live.
// They provide common utility that pipeline execution needs to correctly implement the beam model.
// Note that they are largely implemented in the same manner as user DoFns.

// multiplex is Transform inserted when a PCollection is used as an input into
// multiple downstream Transforms. The same element is emitted to each
// consumming emitter in order.
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

// discard is Transform inserted when a PCollection is unused by a downstream Transform.
// It performs a no-op. This allows execution graphs to avoid branches and checks whether
// a consumer is valid on each element.
type discard[E Element] struct{}

func (fn *discard[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) bool {
		return true
	})
	return nil
}

// buffer is a Transform inserted in front of a parallel PCollection when its consumer
// also has side inputs. The side inputs will be prepped with a notification signal
type buffer[E Element] struct {
	notify func(context.Context) error

	OnBundleFinish
	Out Emitter[E]
}

func (fn *buffer[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	var buf []E
	var latestEC ElmC
	dfc.Process(func(ec ElmC, elm E) bool {
		buf = append(buf, elm)
		if ec.EventTime().After(latestEC.EventTime()) {
			latestEC = ec
		}
		return true
	})
	fn.OnBundleFinish.Do(dfc, func() error {
		for _, elm := range buf {
			fn.Out.Emit(latestEC, elm)
		}
		return nil
	})
	return nil
}
