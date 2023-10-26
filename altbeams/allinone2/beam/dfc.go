package beam

import (
	"context"
	"fmt"
	"time"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"
	fnpb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/fnexecution_v1"
)

// DFC is the DoFn Context for simple DoFns.
type DFC[E Element] struct {
	id        nodeIndex
	transform string
	edgeID    edgeIndex

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

// Process is where you set the per Element processing function that accepts
// elements. Process returns an error to allow inlining with the error return
// from a Transform's ProcessBundle method.
func (c *DFC[E]) Process(perElm Process[E]) error {
	if c.perElm != nil {
		panic("Process called twice")
	}
	// TODO obesrved windows can have a wrapper set to do the downstream explode.
	c.perElm = perElm
	return nil
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
	transformID() string
	update(edgeID edgeIndex, transform string, dofn any, procs []processor, mets *metricsStore)

	// discard signals that input this processor receives can be discarded.
	discard()
	// multiplex indicates this input is used by several consumers.
	multiplex(int) []processor

	produceTypedNode(global string, id nodeIndex, bounded bool) node
	produceDoFnEdge(transform string, id edgeIndex, dofn any, ins, outs map[string]nodeIndex, opts beamopts.Struct) multiEdge

	start(ctx context.Context) error
	split(*fnpb.ProcessBundleSplitRequest_DesiredSplit) *fnpb.ProcessBundleSplitResponse
	finish() error
}

var _ processor = &DFC[int]{}

func (c *DFC[E]) produceTypedNode(global string, id nodeIndex, bounded bool) node {
	c.id = id
	return &typedNode[E]{index: id, id: global, isBounded: bounded}
}

func (c *DFC[E]) produceDoFnEdge(transform string, id edgeIndex, dofn any, ins, outs map[string]nodeIndex, opts beamopts.Struct) multiEdge {
	c.dofn = dofn.(Transform[E])
	return &edgeDoFn[E]{transform: transform, index: id, parallelIn: c.id, dofn: c.dofn, ins: ins, outs: outs, opts: opts}
}

func (c *DFC[E]) update(edgeID edgeIndex, transform string, dofn any, procs []processor, mets *metricsStore) {
	if c.dofn != nil {
		panic(fmt.Sprintf("double updated: dfc %v already has %T, but got %T", c.id, c.dofn, dofn))
	}
	c.transform = transform
	c.edgeID = edgeID
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
		procs = append(procs, emt.newDFC(c.id))
	}
	c.dofn = mplex
	c.downstream = procs
	return procs
}

func (c *DFC[E]) processE(ec elmContext, elm E) {
	if c.metrics != nil {
		c.metrics.setState(1, c.edgeID)
	}
	if err := c.perElm(ElmC{ec, c.downstream}, elm); err != nil {
		panic(fmt.Errorf("doFn id %v failed: %w", c.id, err))
	}
}

func (c *DFC[E]) start(ctx context.Context) error {
	// Defend against multiple initializations due to SDK side flattens.
	if c.perElm != nil {
		return nil
	}
	if c.metrics != nil {
		c.metrics.setState(0, c.edgeID)
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

type splitAware interface {
	status(func(index, split int64, prog float64) (int64, float64, error)) (int64, bool)
}

func (c *DFC[E]) split(desired *fnpb.ProcessBundleSplitRequest_DesiredSplit) *fnpb.ProcessBundleSplitResponse {
	newSplit, ok := c.dofn.(splitAware).status(func(index, split int64, prog float64) (int64, float64, error) {
		bufSize := desired.GetEstimatedInputElements()
		if bufSize <= 0 || split < bufSize {
			bufSize = split
		}
		return splitHelper(index, bufSize, prog, desired.GetAllowedSplitPoints(), desired.GetFractionOfRemainder(), false)
	})
	if !ok {
		return &fnpb.ProcessBundleSplitResponse{}
	}
	return &fnpb.ProcessBundleSplitResponse{
		ChannelSplits: []*fnpb.ProcessBundleSplitResponse_ChannelSplit{
			{
				TransformId:          c.transform,
				LastPrimaryElement:   newSplit - 1,
				FirstResidualElement: newSplit,
			},
		},
	}
}

func (c *DFC[E]) finish() error {
	if c.metrics != nil {
		c.metrics.setState(2, c.edgeID)
	}
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
