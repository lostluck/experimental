// Package beam is an experimental mockup of an Apache Beam Go SDK API that
// leverages generics, and a more opinionated construction method. It exists
// to explore the ergonomics and feasibility of such an approach.
//
// Core Ideas:
// * Single Process method, that is passed a special beam context object: DE.
//   - User manage Iteration avoids start and finish bundle.
//   - Isolating certain types to passed in callbacks avoids context errors.
//   - Should also avoid certain issues around sampling, and
//   - Beam context object has specific methods that can accept a function to push iterate over.
//
// * Build off of the State and Timer "provider" widgets, expanding them to side inputs, and emitters.
// * Generic KV, Iterator, Emitter types.
//
// Key things that need to be observable to the framework:
//
//   - Type of the input elements.
//
//   - Side Inputs and their access patterns.
//
//   - Emitter(s) and their types.
//
//   - Whether SplittableDoFns (and restrictions, watermark estimators)
//
//   - Whether window observing
//
//   - Either looks at windows, or has side inputs.
//
//   - Whether stateful
//
//   - State and Timers
//
//     There's some value to trying to keep the "yeild"/ iterate functions
//     to be of the `form func(yeild func(T1, T2) bool) bool` so that
//
// they may be able to take advantage of proposed languages changes around
// user defined iterators. https://github.com/golang/go/issues/61405
//
// ----
// Did I fall into the coroutine trap? Is this stream processing structure possible
// without coroutines? https://research.swtch.com/coro ?
// I don't think it is :/ since we call the DFC to process the stream of values...
// We would need to have DFC.process initialize the next DoFn down the line.
// then the last one needs to request input from it's predicessor, which requests it
// from their predicessor, etc.
//
// That's if the processors are are StartBundled at the same time, instead of only on
// first element...
//
// So to make this approach work, I'm certain coroutine optimizations are required,
// as described in https://github.com/golang/go/discussions/54245's appendix.
// The real trick is the debugging since we end up with a goroutine per DoFn, and
// it's not clear about which stack connects to what, for a given set of transforms.
package beam

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
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

// DFC is the DoFn Context for simple DoFns.
type DFC[E any] struct {
	processStarted bool
	id             string

	upstream   chan msg
	downstream map[string]processor
}

func (dfc DFC[E]) identifier() string {
	return dfc.id
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

// Process is what the user calls to handle the bundle of elements.
//
//	for ec := range dfc.Process {
//	    // Do some processing with ec.Elm()
//	}
//
// Process can't return a function since we can't reprocess bundle data.
func (c *DFC[E]) Process(perElm func(ec ElmC, elm E) bool) {
	c.processStarted = true
	for m := range c.upstream {
		// TODO Explode windows if necessary.
		ec := ElmC{
			elmContext:   m.ec,
			pcollections: c.downstream,
		}
		if !perElm(ec, m.elm.(E)) {
			break
		}
	}
}

func (c DFC[E]) process(ec elmContext, elm any) {
	c.upstream <- msg{ec, elm}
}

func (c DFC[E]) stop() {
	close(c.upstream)
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

type processor interface {
	identifier() string
	process(elmContext, any)
	stop()
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

var numBuf atomic.Uint32

func init() { numBuf.Store(100) } // See BenchmarkPipe.

func newDFC[E any](id string, ds map[string]processor) DFC[E] {
	return DFC[E]{
		id:         id,
		upstream:   make(chan msg, numBuf.Load()),
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

	proc.process(ec.elmContext, elm)
}

// BundleProc is the only interface that needs to be implemented by most DoFns.
type BundleProc[E any] interface {
	ProcessBundle(ctx context.Context, dfc DFC[E]) error
}

// --------------------------------------------------
// Forward execution construction from sources to sinks.

func Start() DFC[[]byte] {
	dfc := newDFC[[]byte]("impulse", nil)
	dfc.processStarted = true
	go func() {
		dfc.upstream <- msg{
			ec: elmContext{
				eventTime: time.Now(),
			},
			elm: []byte{},
		}
		close(dfc.upstream)
	}()
	return dfc
}

// RunDoFn initializes and starts the BundleProc, and prepares inputs for downstream consumers.
func RunDoFn[E any](ctx context.Context, wg *sync.WaitGroup, input processor, prod BundleProc[E]) []processor {
	dfc := input.(DFC[E])

	procs, downstream := makeEmitters(prod)
	dfc.downstream = downstream

	wg.Add(1)
	go func() {
		defer wg.Done()
		prod.ProcessBundle(ctx, dfc)
		for _, p := range downstream {
			p.stop()
		}
	}()

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

// StartConsumer configures any downstream emitters on prepared outputs, and
// starts the BundleProc's goroutine.
func StartConsumer[E any](ctx context.Context, wg *sync.WaitGroup, id string, outs []processor, cons BundleProc[E]) DFC[E] {
	// TODO check type match ups.
	downstream := setEmitters(cons, outs)

	dfc := newDFC[E](id, downstream)
	wg.Add(1)

	go func() {
		defer wg.Done()
		cons.ProcessBundle(ctx, dfc)
		for _, p := range downstream {
			p.stop()
		}
	}()

	return dfc
}

func Wrap(procs ...processor) []processor {
	return procs
}

var _ emitIface = &Emitter[int]{}

// Set emitters, initializes the emitters with these ids in this order.
func setEmitters(cons any, procs []processor) map[string]processor {
	rv := reflect.ValueOf(cons)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}

	downstream := map[string]processor{}
	for i := range rv.NumField() {
		fv := rv.Field(i)
		if !fv.CanAddr() {
			continue
		}
		fv = fv.Addr()
		if emt, ok := fv.Interface().(emitIface); ok {
			proc := procs[len(downstream)]
			emt.setPColKey(proc.identifier())
			downstream[proc.identifier()] = proc
		}
	}
	if len(procs) != len(downstream) {
		panic(fmt.Sprintf("mistmatch between ids and outputs on %T: %v ids, outs %v", cons, len(procs), len(downstream)))
	}
	return downstream
}

func Impulse(dfc DFC[[]byte]) {
	dfc.upstream <- msg{
		ec: elmContext{
			eventTime: time.Now(),
		},
		elm: []byte{},
	}
	close(dfc.upstream)
}
