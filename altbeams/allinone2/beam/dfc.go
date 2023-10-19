package beam

import (
	"context"
	"fmt"
	"time"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"
)

// DFC is the DoFn Context for simple DoFns.
type DFC[E Element] struct {
	id        nodeIndex
	transform string

	dofn       Transform[E]
	downstream []processor

	perElm       Process[E]
	finishBundle func() error

	metrics *metricsStore
}

func (c *DFC[E]) transformID() string {
	return c.transform
}

type elmContext struct {
	eventTime time.Time
	windows   []coders.GWC
	pane      coders.PaneInfo
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
// TODO document better.
// Do we even need this though? Can we instead just have ProcessBundle return the perElm func?
func (c *DFC[E]) Process(perElm Process[E]) {
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

func (c *DFC[E]) metricsStore() *metricsStore {
	return c.metrics
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
	update(transform string, dofn any, procs []processor, mets *metricsStore)

	// discard signals that input this processor receives can be discarded.
	discard()
	// multiplex indicates this input is used by several consumers.
	multiplex(int) []processor

	// newTypeMultiEdge produces a configured edge of the matching type and generic parameter.
	produceTypedNode(id nodeIndex, bounded bool) node
	produceDoFnEdge(transform string, id edgeIndex, dofn any, ins, outs map[string]nodeIndex, opts beamopts.Struct) multiEdge

	start(ctx context.Context) error
	finish() error

	metricsStore() *metricsStore
}

var _ processor = &DFC[int]{}

func (c *DFC[E]) produceTypedNode(id nodeIndex, bounded bool) node {
	c.id = id
	return &typedNode[E]{index: id, isBounded: bounded}
}

func (c *DFC[E]) produceDoFnEdge(transform string, id edgeIndex, dofn any, ins, outs map[string]nodeIndex, opts beamopts.Struct) multiEdge {
	c.dofn = dofn.(Transform[E])
	return &edgeDoFn[E]{transform: transform, index: id, parallelIn: c.id, dofn: c.dofn, ins: ins, outs: outs, opts: opts, proc: c}
}

func (c *DFC[E]) update(transform string, dofn any, procs []processor, mets *metricsStore) {
	if c.dofn != nil {
		panic(fmt.Sprintf("double updated: dfc %v already has %T, but got %T", c.id, c.dofn, dofn))
	}
	c.transform = transform
	c.dofn = dofn.(Transform[E])
	c.downstream = procs
	c.metrics = mets
}

func (c *DFC[E]) discard() {
	c.dofn = &discard[E]{}
}

func getSingleValue[K comparable, V any](in map[K]V) V {
	for _, v := range in {
		return v
	}
	panic("expected single value map")
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
	if err := c.perElm(ElmC{ec, c.downstream}, elm); err != nil {
		panic(fmt.Errorf("doFn id %v failed: %w", c.id, err))
	}
}

func (c *DFC[E]) start(ctx context.Context) error {
	// Defend against multiple initializations due to SDK side flattens.
	if c.perElm != nil {
		return nil
	}
	if err := c.dofn.ProcessBundle(ctx, c); err != nil {
		return nil
	}
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
