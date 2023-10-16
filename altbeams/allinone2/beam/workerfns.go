package beam

import (
	"context"
	"fmt"
	"math"
	"time"
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
	return map[string]nodeIndex{"o0": e.output}
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
	multiEdge
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
	fmt.Println("sdk flatten")
	dfc.Process(func(ec ElmC, elm E) bool {
		fn.Output.Emit(ec, elm)
		return true
	})
	return nil
}

// EdgeGBK represents a Group By Key transform.
type edgeGBK[K Keys, V Element] struct {
	index edgeIndex

	input, output nodeIndex
}

// inputs for GBKs are one.
func (e *edgeGBK[K, V]) inputs() map[string]nodeIndex {
	return map[string]nodeIndex{"i0": e.input}
}

// inputs for GBKs are one.
func (e *edgeGBK[K, V]) outputs() map[string]nodeIndex {
	return map[string]nodeIndex{"o0": e.output}
}

func (e *edgeGBK[K, V]) groupby() {}

type keygrouper interface {
	multiEdge
	groupby()
}

var _ keygrouper = (*edgeGBK[int, int])(nil)

// gbk groups by the key type and value type.
// TODO, remove this.
type gbk[K Keys, V Element] struct {
	Output Emitter[KV[K, Iter[V]]]

	OnBundleFinish
}

var (
	MaxET time.Time = time.UnixMilli(math.MaxInt64 / 1000)
	EOGW            = MaxET.Add(-time.Hour * 24)
)

func (fn *gbk[K, V]) ProcessBundle(ctx context.Context, dfc *DFC[KV[K, V]]) error {
	grouped := map[K][]V{}
	dfc.Process(func(ec ElmC, elm KV[K, V]) bool {
		vs := grouped[elm.Key]
		vs = append(vs, elm.Value)
		grouped[elm.Key] = vs
		return true
	})
	fn.OnBundleFinish.Do(dfc, func() error {
		ec := dfc.ToElmC(EOGW) // TODO pull time, from the window that's been closed.
		for k, vs := range grouped {
			var i int
			out := KV[K, Iter[V]]{Key: k, Value: Iter[V]{
				source: func() (V, bool) {
					var v V
					if i < len(vs) {
						v = vs[i]
						i++
						return v, true
					}
					return v, false
				},
			}}
			fn.Output.Emit(ec, out)
		}
		return nil
	})
	return nil
}
