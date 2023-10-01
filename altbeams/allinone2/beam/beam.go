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
	"math"
	"time"
)

type Keys interface {
	comparable
}

type KV[K Keys, V Element] struct {
	Key   K
	Value V
}

type Element interface {
	any // Sadly, can't really restrict this without breaking iterators in GBK results.
}

// DFC is the DoFn Context for simple DoFns.
type DFC[E Element] struct {
	id nodeIndex

	dofn       Transform[E]
	downstream []processor

	perElm       func(ec ElmC, elm E) bool
	finishBundle func() error
}

type elmContext struct {
	eventTime time.Time
	windows   []time.Time
	pane      string
}

func newDFC[E Element](id nodeIndex, ds []processor) *DFC[E] {
	return &DFC[E]{
		id:         id,
		downstream: ds,
	}
}

// Process is what the user calls to handle the bundle of elements.
//
// Per the issued FAQ, probably won't make process loop compatible,
// since it's going to cause issues with error returns and similar.
//
//	for ec := range dfc.Process {
//	    // Do some processing with ec.Elm()
//	}
//
// Process can't return a function since we can't reprocess bundle data.
//
// TODO
func (c *DFC[E]) Process(perElm func(ec ElmC, elm E) bool) {
	if c.perElm != nil {
		panic("Process called twice")
	}
	// TODO obesrved windows can have a wrapper set to do the downstream explode.
	c.perElm = perElm
}

// FinishBundle can optionally be called to provide a callback
// for post bundle tasks.
func (c *DFC[E]) regBundleFinisher(finishBundle func() error) {
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
	pcollection() nodeIndex // The pcollection to be processed.
	update(dofn any, procs []processor)
	discard()
	multiplex(int) []processor

	start(ctx context.Context) error
	finish() error
}

var _ processor = &DFC[int]{}

func (c *DFC[E]) pcollection() nodeIndex {
	return c.id
}

func (c *DFC[E]) update(dofn any, procs []processor) {
	if c.dofn != nil {
		panic(fmt.Sprintf("double updated: dfc %v already has %T, but got %T", c.id, c.dofn, dofn))
	}
	c.dofn = dofn.(Transform[E])
	c.downstream = procs
}

func (c *DFC[E]) discard() {
	c.dofn = &discard[E]{}
}

func (c *DFC[E]) multiplex(numOut int) []processor {
	mplex := &multiplex[E]{Outs: make([]Emitter[E], numOut)}
	var procs []processor
	for i := range mplex.Outs {
		emt := &mplex.Outs[i] // Get a pointer to the emitter, rather than a value copy from the loop.
		emt.setPColKey(c.id, i)
		procs = append(procs, emt.newDFC(c.id))
	}
	c.dofn = mplex
	c.downstream = procs
	return procs
}

func (c *DFC[E]) processE(ec elmContext, elm E) {
	if !c.perElm(ElmC{ec, c.downstream}, elm) {
		panic(fmt.Errorf("short iteration"))
	}
}

func (c *DFC[E]) start(ctx context.Context) error {
	// Defend against multiple initializations due to SDK side flattens.
	if c.perElm != nil {
		return nil
	}
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
	// Clear away state for re-uses of this bundle plan.
	c.perElm = nil
	c.finishBundle = nil
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

// Transform is the only interface that needs to be implemented by most DoFns.
type Transform[E Element] interface {
	ProcessBundle(ctx context.Context, dfc *DFC[E]) error
}

type Iter[V Element] struct {
	source func() (V, bool) // source returns true if the element is valid.
}

func (Iter[V]) unencodable() {}

var _ unencodable = Iter[int]{}

type unencodable interface {
	unencodable()
}

func isUnencodable(v any) bool {
	_, ok := v.(unencodable)
	return ok
}

// All allows a single iteration of its stream of values.
func (it *Iter[V]) All() func(perElm func(elm V) bool) {
	return func(perElm func(elm V) bool) {
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
}

// GBK produces an output PCollection.
func GBK[K Keys, V Element](s *Scope, input Emitter[KV[K, V]]) Emitter[KV[K, Iter[V]]] {
	// TODO, use a real defered gbk edge, instead of the DoFn fake.
	return ParDo(s, input, &gbk[K, V]{}).Output
}

// gbk groups by the key type and value type.
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

type flatten[E Element] struct {
	Output Emitter[E]
}

func (fn *flatten[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) bool {
		fn.Output.Emit(ec, elm)
		return true
	})
	return nil
}

func start(dfc *DFC[[]byte]) error {
	if err := dfc.start(context.TODO()); err != nil {
		return err
	}
	dfc.processE(elmContext{
		eventTime: time.Now(),
	}, []byte{1, 2, 3, 4, 5, 6, 7, 7})
	return nil
}

// Scope is used for building pipeline graphs.
//
// Scope is a hierarchical grouping for composite transforms. Scopes can be
// enclosed in other scopes and for a tree structure. For pipeline updates,
// the scope chain form a unique name. The scope chain can also be used for
// monitoring and visualization purposes.
type Scope struct {
	name   string
	parent *Scope

	g *graph
}

func (s *Scope) String() string {
	if s == nil {
		return ""
	}
	return s.parent.String() + "/" + s.name
}

// Run begins executes the pipeline built in the construction function.
func Run(ctx context.Context, expand func(*Scope) error) error {
	var g graph
	s := &Scope{parent: nil, g: &g}
	g.root = s

	if err := expand(s); err != nil {
		return fmt.Errorf("pipeline construction error:%w", err)
	}

	// At this point the graph is complete, and we need to turn serialize/deserialize it
	// into executing code.

	// Now, we must rebuild it. Make it better, faster, actually executable.
	roots := g.build()
	for _, root := range roots {
		if err := start(root.(*DFC[[]byte])); err != nil {
			return err
		}
	}
	for _, root := range roots {
		if err := root.finish(); err != nil {
			return err
		}
	}
	return nil
}

func Impulse(s *Scope) Emitter[[]byte] {
	edgeID := s.g.curEdgeIndex()
	nodeID := s.g.curNodeIndex()
	s.g.edges = append(s.g.edges, &edgeImpulse{index: edgeID, output: nodeID})
	s.g.nodes = append(s.g.nodes, &typedNode[[]byte]{index: nodeID, parentEdge: edgeID})

	// This is a fictional input.
	return Emitter[[]byte]{globalIndex: nodeID}
}

// ParDo takes the users's DoFn and returns the same type for downstream piepline construction.
//
// The returned DoFn's emitter fields can then be used as inputs into other DoFns.
// What if we used Emitters as PCollections directly?
// Obviously, we'd rename the type PCollection or similar
// If only to also
func ParDo[E Element, DF Transform[E]](s *Scope, input Emitter[E], dofn DF) DF {
	edgeID := s.g.curEdgeIndex()
	ins, outs := s.g.deferDoFn(dofn, input.globalIndex, edgeID)

	// We do all the expected connections here.
	// Side inputs, are put on the side input at the DoFn creation time being passed in.

	s.g.edges = append(s.g.edges, &edgeDoFn[E]{index: edgeID, dofn: dofn, ins: ins, outs: outs, parallelIn: input.globalIndex})

	return dofn
}

// Composite transforms allow structural re-use of sub pipelines.
type Composite[O any] interface {
	Expand(s *Scope) O
}

func Expand[I Composite[O], O any](parent *Scope, name string, comp I) O {
	s := &Scope{name: name, parent: parent, g: parent.g}
	// We do all the expected connections here.
	// Side inputs, are put on the side input at the DoFn creation time being passed in.
	return comp.Expand(s)
}

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
