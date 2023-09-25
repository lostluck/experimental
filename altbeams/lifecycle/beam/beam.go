// Package beam is an experimental mockup of an Apache Beam Go SDK API that
// leverages generics, and a more opinionated construction method. It exists
// to explore the ergonomics and feasibility of such an approach.
//
// Core Ideas:
// * Same Lifecycle method approach as the current SDK.
//   - Build off of the State and Timer "provider" widgets, expanding them to side inputs, and emitters.
//   - The above avoids complexities in Start and Finish Bundle methods.
//   - ProcessElement is the only generic method.
//
// * Generic KV, Iterator, Emitter types.
// * Straight forward bundle execution. Heavy Emitters/core logic.
package beam

import (
	"context"
	"reflect"
	"time"
)

type KV[K, V any] struct {
	key   K
	value V
}

func (kv *KV[K, V]) Key() K {
	return kv.key
}

func (kv *KV[K, V]) Value() V {
	return kv.value
}

type msg struct {
	ec  elmContext
	elm any
}

type elmContext struct {
	eventTime time.Time
	windows   []time.Time
	pane      string
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

type processor interface {
	StartBundle(context.Context) error
	FinishBundle(context.Context) error
}

func (e *ElmC) EventTime() time.Time {
	return e.eventTime
}

// BundC is the context used in bundles, when the element context is
// unknown. It can be used to build an element context for emitting
// values.
type BundC struct {
	pcollections []processor
}

func (bc BundC) WithEventTime(et time.Time) ElmC {
	return ElmC{
		elmContext: elmContext{
			eventTime: et,
			// windows: Use windowing strategy to get the Proper Windows
			// pane: NoFiringPane
		},
		pcollections: bc.pcollections,
	}
}

type Emitter[E any] struct {
	pcolKey int
}

func (emt *Emitter[E]) setPColKey(id int) {
	emt.pcolKey = id
}

func (emt *Emitter[E]) newNode() processor {
	return &node[E]{}
}

type emitIface interface {
	setPColKey(id int)
	newNode() processor
}

func (emt Emitter[E]) Emit(ec ElmC, elm E) {
	// derive the elmContext, and direct the element down to its PCollection handle
	proc := ec.pcollections[emt.pcolKey]
	nd := proc.(*node[E])
	if err := nd.processE(ec.elmContext, elm); err != nil {
		panic(err)
	}
}

type DoFn[E any] interface {
	ProcessElement(ctx context.Context, ec ElmC, elm E) error
}

type StartBundler interface {
	StartBundle(ctx context.Context, bc BundC) error
}

type FinishBundler interface {
	FinishBundle(ctx context.Context, bc BundC) error
}

type Plan struct {
	Root processor
}

func NewPlan() Plan {
	return Plan{
		Root: &node[[]byte]{},
	}
}

func (p *Plan) Process(ctx context.Context) error {
	if err := p.Root.StartBundle(ctx); err != nil {
		return err
	}

	nd := p.Root.(*node[[]byte])
	if err := nd.processE(elmContext{eventTime: time.Now()}, []byte{}); err != nil {
		return err
	}

	if err := p.Root.FinishBundle(ctx); err != nil {
		return err
	}
	return nil
}

type node[E any] struct {
	fn DoFn[E]

	pcollections []processor
}

func (n *node[E]) StartBundle(ctx context.Context) error {
	if sbn, ok := n.fn.(StartBundler); ok {
		bc := BundC{
			pcollections: n.pcollections,
		}
		if err := sbn.StartBundle(ctx, bc); err != nil {
			return err
		}
	}
	for _, node := range n.pcollections {
		if err := node.StartBundle(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (n *node[E]) processE(ecc elmContext, elm E) error {
	ec := ElmC{
		elmContext:   ecc,
		pcollections: n.pcollections,
	}
	return n.fn.ProcessElement(context.TODO(), ec, elm)
}

func (n *node[E]) FinishBundle(ctx context.Context) error {
	if fbn, ok := n.fn.(FinishBundler); ok {
		bc := BundC{
			pcollections: n.pcollections,
		}
		if err := fbn.FinishBundle(ctx, bc); err != nil {
			return err
		}
	}
	for _, node := range n.pcollections {
		if err := node.FinishBundle(ctx); err != nil {
			return err
		}
	}
	return nil
}

func ParDo[E any](ctx context.Context, cur processor, prod DoFn[E]) []processor {
	var proc *node[E]
	if cur == nil {
		proc = &node[E]{}
	} else {
		proc = cur.(*node[E])
	}
	procs := makeEmitters(prod)
	proc.fn = prod
	proc.pcollections = procs
	return procs
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
			proc := emt.newNode()
			procs = append(procs, proc)
		}
	}
	return procs
}
