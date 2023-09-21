// Package beam is an experimental mockup of an Apache Beam Go SDK API that
// leverages generics, and a more opinionated construction method. It exists
// to explore the ergonomics and feasibility of such an approach.
//
// This one in particular is a variant on allinone, which avoids the use of
// separate goroutines and channels to pass around elements.
package beam

import (
	"context"
	"fmt"
	"reflect"
	"time"
)

// DFC is the DoFn Context for simple DoFns.
type DFC[E any] struct {
	id string

	dofn       BundleProc[E]
	downstream map[string]processor

	perElm       func(ec ElmC, elm E) bool
	finishBundle func() error
}

type elmContext struct {
	eventTime time.Time
	windows   []time.Time
	pane      string
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

// processor allows a uniform type for different generic types.
type processor interface {
	start(ctx context.Context) error
	process(elmContext, any) error
	finish() error
}

func (c *DFC[E]) process(ec elmContext, elm any) error {
	c.perElm(ElmC{ec, c.downstream}, elm.(E))
	return nil
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
	if c.finishBundle == nil {
		return nil
	}
	if err := c.finishBundle(); err != nil {
		return err
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

	pcollections map[string]processor
}

func (e *ElmC) EventTime() time.Time {
	return e.eventTime
}

type Emitter[E any] struct {
	pcolKey string
}

func (emt *Emitter[E]) setPColKey(id string) {
	emt.pcolKey = id
}

func (_ *Emitter[E]) newDFC(id string) processor {
	return newDFC[E](id, nil)
}

func newDFC[E any](id string, ds map[string]processor) *DFC[E] {
	return &DFC[E]{
		id:         id,
		downstream: ds,
	}
}

type emitIface interface {
	setPColKey(id string)
	newDFC(id string) processor
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
type BundleProc[E any] interface {
	ProcessBundle(ctx context.Context, dfc *DFC[E]) error
}

// --------------------------------------------------
// Forward execution construction from sources to sinks.

func Impulse() *DFC[[]byte] {
	return newDFC[[]byte]("impulse", nil)
}

// ParDo initializes and starts the BundleProc, and prepares inputs for downstream consumers.
func ParDo[E any](ctx context.Context, input processor, prod BundleProc[E]) []processor {
	dfc := input.(*DFC[E])
	dfc.dofn = prod
	procs, downstream := makeEmitters(prod)
	dfc.downstream = downstream
	return procs
}

func makeEmitters(prod any) ([]processor, map[string]processor) {
	rv := reflect.ValueOf(prod)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	var procs []processor
	downstream := map[string]processor{}
	rt := rv.Type()
	for i := range rv.NumField() {
		fv := rv.Field(i)
		if !fv.CanAddr() || !rt.Field(i).IsExported() {
			continue
		}
		fv = fv.Addr()
		if emt, ok := fv.Interface().(emitIface); ok {
			id := fmt.Sprintf("n%d", len(downstream))
			emt.setPColKey(id)
			proc := emt.newDFC(id)
			downstream[id] = proc
			procs = append(procs, proc)
		}
	}
	if len(procs) != len(downstream) {
		panic(fmt.Sprintf("mistmatch between ids and outputs on %T: %v ids, outs %v", prod, len(procs), len(downstream)))
	}
	return procs, downstream
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
