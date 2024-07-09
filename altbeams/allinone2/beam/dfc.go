package beam

import (
	"context"
	"fmt"
	"time"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"
	fnpb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/fnexecution_v1"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// Process is what the user calls to handle the bundle of elements.
//
// 2024/07/08
// BUT WHAT IF IT WASN'T?!
// What if, we just homogenize the whole experience.
// Yes, even "simple" DoFns would need a struct embed to catch it...
// but THEY WOULDN'T HAVE TO.
//
// Since we'd provide a helper for that simplest case.
//
// Then the DFC could refer to all the broad types.
//
// beam.DoFn
// beam.SplittableDoFn
// beam.UnboundedDoFn
//
// Each could be clearly documented.
//
// But it does mean repeating the Element type because
// we *must* have a method with the type so we can get construction time
// type safety.
//
// So we can't get rid of the ProcessBundle method as a result.
// Cost of having a *DoFn* mixin: repeated element type.
// Benefit: Similarity across types of DoFn (but not combinefns)
// Cost of not having that: Additional Complexity in the already complex case.
// Benefit: Simpler simple case.
//
// This does open up the question of whether CombineFns can do the same thing.
// For even more eveness/parity. Doesn't feel likely due to the labor division.

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
	elementSplit() (prog float64, splitElm elmSplitCallback)
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
	mplex := &multiplex[E]{Outs: make([]Output[E], numOut)}
	var procs []processor
	for i := range mplex.Outs {
		emt := &mplex.Outs[i]        // Get a pointer to the emitter, rather than a value copy from the loop.
		emt.localDownstreamIndex = i // Manually set emitter id for multiplex nodes, they aren't reprocessed.
		procs = append(procs, emt.newDFC(c.id))
	}
	c.dofn = mplex
	c.downstream = procs
	return procs
}

func (c *DFC[E]) start(ctx context.Context) error {
	// Defend against multiple initializations due to SDK side flattens.
	if c.perElm != nil {
		return nil
	}
	c.metrics.setState(0, c.edgeID)
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

type sourceSplitter interface {
	splitSource(func(index, split int64) int64)
}

func (c *DFC[E]) split(desired *fnpb.ProcessBundleSplitRequest_DesiredSplit) *fnpb.ProcessBundleSplitResponse {
	// Splits are conducted in the datasource's critical section to not race to downstream processing.
	var lastPrimary, firstResidual int64
	var pRoots []*fnpb.BundleApplication
	var rRoots []*fnpb.DelayedBundleApplication
	var splitSuccess bool
	c.dofn.(sourceSplitter).splitSource(func(index, split int64) int64 {
		bufSize := desired.GetEstimatedInputElements()
		if bufSize <= 0 || split < bufSize {
			bufSize = split
		}

		// c.downstream[0] // Check if the single downstream has an SDF & get progress.
		// I assume it should lock down restriction tracker handling too.
		// TODO defer unlock the downstream SDF if available.
		prog, splitCallback := c.downstream[0].elementSplit()

		// We now need to use the fraction on the Splittable DoFn.
		// But we don't *have* the SDF. That's on the downstream DFC.
		// So ultimately, this DoFn shouldn't do anything about it, it should delegate.

		suggestedSplit, fr, err := splitHelper(
			index, bufSize, prog,
			desired.GetAllowedSplitPoints(),
			desired.GetFractionOfRemainder(),
			splitCallback != nil)
		if err != nil {
			// TODO log error
			return split // return original split. No changes here.
		}
		// A fraction less than 0 means this is a channel split. We're done!
		if fr < 0 {
			splitSuccess = true
			lastPrimary = suggestedSplit - 1
			firstResidual = suggestedSplit
			// inform the datasource of new split
			return firstResidual
		}
		// Do Sub Element Splitting!

		// While technically this is blocking new sub element and datasource progress the whole time the split
		// is taking place, the source lock is already blocked since sub element processing is happening.
		// However, the callback should minimize the time it maintains a lock blocking processing.
		// TODO evaluate lock contention WRT progress requests and processing on sub element splits.
		sr := splitCallback(fr)

		// In a sub-element split, newSplit is currIdx, so we need to increment it, so we don't pre-maturely channel split.
		firstResidual = suggestedSplit + 1
		lastPrimary = suggestedSplit - 1
		if sr.PS != nil && len(sr.PS) > 0 && sr.RS != nil && len(sr.RS) > 0 {
			pRoots = make([]*fnpb.BundleApplication, len(sr.PS))
			for i, p := range sr.PS {
				pRoots[i] = &fnpb.BundleApplication{
					TransformId: sr.TId,
					InputId:     sr.InId,
					Element:     p,
				}
			}
			rRoots = make([]*fnpb.DelayedBundleApplication, len(sr.RS))
			for i, r := range sr.RS {
				rRoots[i] = &fnpb.DelayedBundleApplication{
					Application: &fnpb.BundleApplication{
						TransformId:      sr.TId,
						InputId:          sr.InId,
						Element:          r,
						OutputWatermarks: sr.OW,
					},
				}
			}
		}

		return firstResidual
	})
	// Split didn't succeed, return a neutral response since split failures are not Bundle failures.
	if !splitSuccess {
		return &fnpb.ProcessBundleSplitResponse{}
	}
	return &fnpb.ProcessBundleSplitResponse{
		ChannelSplits: []*fnpb.ProcessBundleSplitResponse_ChannelSplit{
			{
				TransformId:          c.transform,
				LastPrimaryElement:   lastPrimary,
				FirstResidualElement: firstResidual,
			},
		},
	}
}

// elmSplitCallback handles sub element splitting against the SDF, commits the split
// and returns the serialized split result.
type elmSplitCallback func(fraction float64) elementSplitResult

// elementSplitResult is the result of sub element splitting.
type elementSplitResult struct {
	PS   [][]byte // Primary splits. If an element is split, these are the encoded primaries.
	RS   [][]byte // Residual splits. If an element is split, these are the encoded residuals.
	TId  string   // Transform ID of the transform receiving the split elements.
	InId string   // Input ID of the input the split elements are received from.

	OW map[string]*timestamppb.Timestamp // Map of outputs to output watermark for the plan being split
}

type elementSplitter interface {
	// splitElementSource returns the progress fraction of the current element, and a callback to handle sub element splits.
	// If the source can't sub element split, the callback must be nil.
	splitElementSource() (prog float64, splitElement elmSplitCallback)
}

// elementSplit is called on the DFC for a downstream Splittable DoFn.
func (c *DFC[E]) elementSplit() (prog float64, splitElement elmSplitCallback) {
	es, ok := c.dofn.(elementSplitter)
	if !ok {
		return 0.5, nil
	}
	return es.splitElementSource()
}

func (c *DFC[E]) finish() error {
	c.metrics.setState(2, c.edgeID)
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
