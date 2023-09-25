// Package beam is an experimental mockup of an Apache Beam Go SDK API that
// leverages generics, and a more opinionated construction method. It exists
// to explore the ergonomics and feasibility of such an approach.
//
// This one in particular is a variant on allinone, which avoids the use of
// separate goroutines and channels to pass around elements.
package beam

import (
	"context"
	"math"
	"reflect"
	"time"
)

type Keys interface {
	comparable
}

type KV[K Keys, V Elements] struct {
	Key   K
	Value V
}

type Elements interface {
	any // Sadly, can't really restrict this without breaking iterators in GBK results.
}

// DFC is the DoFn Context for simple DoFns.
type DFC[E Elements] struct {
	id int

	dofn       BundleProc[E]
	downstream []processor

	perElm       func(ec ElmC, elm E) bool
	finishBundle func() error
}

type elmContext struct {
	eventTime time.Time
	windows   []time.Time
	pane      string
}

func newDFC[E Elements](id int, ds []processor) *DFC[E] {
	return &DFC[E]{
		id:         id,
		downstream: ds,
	}
}

// Process is what the user calls to handle the bundle of elements.
//
//	for ec := range dfc.Process {
//	    // Do some processing with ec.Elm()
//	}
//
// Process can't return a function since we can't reprocess bundle data.
func (c *DFC[E]) Process(perElm func(ec ElmC, elm E) bool) {
	if c.perElm != nil {
		panic("Process called twice")
	}
	c.perElm = perElm
}

// FinishBundle can optionally be called to provide a callback
// for post bundle tasks.
func (c *DFC[E]) FinishBundle(finishBundle func() error) {
	if c.finishBundle != nil {
		panic("FinishBundle called twice")
	}
	c.finishBundle = finishBundle
}

// ToElmC is to get the appropriate element context for elements not derived from a specific
// element directly.
//
// This derives the element windows, and sets a no-firing pane.
func (c *DFC[E]) ToElmC(eventTime time.Time) ElmC {
	return ElmC{
		elmContext: elmContext{
			eventTime: eventTime,
			// TODO windows, pane
		},
		pcollections: c.downstream,
	}
}

// processor allows a uniform type for different generic types.
type processor interface {
	start(ctx context.Context) error
	finish() error
}

func (c *DFC[E]) processE(ec elmContext, elm E) error {
	c.perElm(ElmC{ec, c.downstream}, elm)
	return nil
}

func (c *DFC[E]) start(ctx context.Context) error {
	c.dofn.ProcessBundle(ctx, c)
	for _, proc := range c.downstream {
		if err := proc.start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *DFC[E]) finish() error {
	if c.finishBundle != nil {
		if err := c.finishBundle(); err != nil {
			return err
		}
	}
	for _, proc := range c.downstream {
		if err := proc.finish(); err != nil {
			return err
		}
	}
	return nil
}

// ElmC is the catch all context for the current element.
//
// This includes
// * Key (state and timers)
// * Windows
// * Timestamp
// * Pane
//
// Provides the downstream emission context, so it actually sends data to the next DoFn.
type ElmC struct {
	elmContext

	pcollections []processor
}

func (e *ElmC) EventTime() time.Time {
	return e.eventTime
}

type Emitter[E Elements] struct {
	pcolKey int
}

type emitIface interface {
	setPColKey(id int)
	newDFC(id int) processor
}

func (emt *Emitter[E]) setPColKey(id int) {
	emt.pcolKey = id
}

func (_ *Emitter[E]) newDFC(id int) processor {
	return newDFC[E](id, nil)
}

// Emit the element within the current element's context.
//
// The ElmC value is sourced from the [DFC.Process] method.
func (emt Emitter[E]) Emit(ec ElmC, elm E) {
	// derive the elmContext, and direct the element down to its PCollection handle
	proc := ec.pcollections[emt.pcolKey]

	dfc := proc.(*DFC[E])
	dfc.processE(ec.elmContext, elm)
}

// BundleProc is the only interface that needs to be implemented by most DoFns.
type BundleProc[E Elements] interface {
	ProcessBundle(ctx context.Context, dfc *DFC[E]) error
}

// --------------------------------------------------
// Forward execution construction from sources to sinks.

func Impulse() *DFC[[]byte] {
	return newDFC[[]byte](0, nil)
}

// ParDo initializes and starts the BundleProc, and prepares inputs for downstream consumers.
func ParDo[E Elements](input processor, prod BundleProc[E]) []processor {
	dfc := input.(*DFC[E])
	dfc.dofn = prod
	procs := makeEmitters(prod)
	dfc.downstream = procs
	return procs
}

type Iter[V Elements] struct {
	source func() (V, bool) // source returns true if the element is valid.
}

// All allows a single iteration of its stream of values.
func (it *Iter[V]) All(perElm func(elm V) bool) {
	for {
		v, ok := it.source()
		if !ok {
			return
		}
		if !perElm(v) {
			return
		}
	}
}

// ParDo initializes and starts the BundleProc, and prepares inputs for downstream consumers.
func GBK[K Keys, V Elements](input processor) processor {
	return ParDo(input, &gbk[K, V]{})[0]
}

// gbk groups by the key type and value type.
type gbk[K Keys, V Elements] struct {
	Output Emitter[KV[K, Iter[V]]]
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
	dfc.FinishBundle(func() error {
		ec := dfc.ToElmC(EOGW) // TODO pull, from the window that's been closed.
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

func makeEmitters(prod any) []processor {
	rv := reflect.ValueOf(prod)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	var procs []processor
	rt := rv.Type()
	for i := 0; i < rv.NumField(); i++ {
		fv := rv.Field(i)
		if !fv.CanAddr() || !rt.Field(i).IsExported() {
			continue
		}
		fv = fv.Addr()
		if emt, ok := fv.Interface().(emitIface); ok {
			id := len(procs)
			emt.setPColKey(id)
			proc := emt.newDFC(id)
			procs = append(procs, proc)
		}
	}
	return procs
}

// --------------------------------------------------
// Reverse execution construction from sinks to sources.
func Start(dfc *DFC[[]byte]) error {
	if err := dfc.start(context.TODO()); err != nil {
		return err
	}
	if err := dfc.processE(elmContext{
		eventTime: time.Now(),
	}, []byte{}); err != nil {
		return err
	}
	return dfc.finish()
}
