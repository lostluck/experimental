package beam

import (
	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
)

// beamMixin is added to all DoFn beam field types to allow them to bypass
// encoding. Only needed when the value has state and shouldn't be embedded.
type beamMixin struct{}

func (beamMixin) beamBypass() {}

type bypassInterface interface {
	beamBypass()
}

// dofns.go is about the different mix-ins and addons that can be added.

// Output represents an output of a DoFn.
//
// At pipeline construction time, they represent an output PCollection, and
// can be connected as inputs to downstream DoFns.
//
// At pipeline execution time, they are used in a ProcessBundle method to emit
// elements and pass along per element context, such as the EventTime and Window.
type Output[E Element] struct {
	beamMixin

	valid                bool
	globalIndex          nodeIndex
	localDownstreamIndex int
	mets                 *pcollectionMetrics
	coder                coders.Coder[E]
}

type emitIface interface {
	setPColKey(global nodeIndex, id int, coder any) *pcollectionMetrics
	newDFC(id nodeIndex) processor
	newNode(protoID string, global nodeIndex, parent edgeIndex, bounded bool) node
}

var _ emitIface = (*Output[any])(nil)

func (emt *Output[E]) setPColKey(global nodeIndex, id int, coder any) *pcollectionMetrics {
	emt.valid = true
	emt.globalIndex = global
	emt.localDownstreamIndex = id
	emt.mets = &pcollectionMetrics{nodeIdx: global, nextSampleIdx: 1}
	if coder != nil {
		emt.coder = coder.(coders.Coder[E])
	}
	return emt.mets
}

func (_ *Output[E]) newDFC(id nodeIndex) processor {
	return &DFC[E]{id: id}
}

func (_ *Output[E]) newNode(protoID string, global nodeIndex, parent edgeIndex, bounded bool) node {
	return &typedNode[E]{id: protoID, index: global, parentEdge: parent, isBounded: bounded}
}

// Emit the element within the current element's context.
//
// The ElmC value is sourced from the [DFC.Process] method.
func (emt *Output[E]) Emit(ec ElmC, elm E) {
	if emt.mets != nil {
		if emt.mets.Count() {
			enc := coders.NewEncoder()
			// TODO, optimize this with a sizer instead?
			emt.coder.Encode(enc, elm)
			emt.mets.Sample(int64(len(enc.Data())))
		}
	}
	proc := ec.pcollections[emt.localDownstreamIndex]
	dfc := proc.(*DFC[E])
	dfc.processE(ec.elmContext, elm)
}

// OnBundleFinish allows a DoFn to register a function that runs just before
// a bundle finishes. Elements may be emitted downstream, if an ElmC is retrieved
// from the DFC.
type OnBundleFinish struct{}

type bundleFinisher interface {
	regBundleFinisher(finishBundle func() error)
}

// Do registers a callback to execute after all bundle elements have been processed.
// Any resources that a DoFn needs explicitly cleaned up explicitly rather than implicitly
// via garbage collection, should be called here.
//
// Only a single callback may be registered, and it will be the last one passed to Do.
func (*OnBundleFinish) Do(dfc bundleFinisher, finishBundle func() error) {
	dfc.regBundleFinisher(finishBundle)
}

////////////////////////////////////////////////////////
// Below here are Not Yet Implemented field flavours. //
////////////////////////////////////////////////////////

// ObserveWindow indicates this DoFn needs to be aware of windows explicitly.
// Typical use is to embed ObserveWindows as a field.
type ObserveWindow struct{}

func (*ObserveWindow) Of(ec ElmC) any { // TODO make this a concrete window type.
	// When windows are observable, only a single window is present.
	return ec.windows[0]
}

// AfterBundle allows a DoFn to register a function that runs after
// the bundle has been durably committed. Emiting elements here will fail.
//
// TODO consider moving this to a simple interface function.
// Upside, not likely to try to incorrectly emit in the closure.
// Downside, the caching for anything to finalize needs to be stored in the DoFn struct
// this violates the potential of a ConfigOnly DoFn.
type AfterBundle struct{ beamMixin }

type bundleFinalizer interface {
	regBundleFinalizer(finalizeBundle func() error)
}

func (*AfterBundle) Do(dfc bundleFinalizer, finalizeBundle func() error) {
	dfc.regBundleFinalizer(finalizeBundle)
}

// How do we make SDFs work as expected with this paradigm, and be more construction time
// safe? Eg. Require both the marker, and certain methods to be implemented on the DoFn.

type Restriction interface {
	Element
	Size() float64
}
type Tracker[R Restriction] interface {
	Split(R) []R
	TrySplit(fraction float64) (primary, residual R, err error)
}

type Splittable[E Element, R Restriction, T Tracker[R]] struct{}

func (*Splittable[E, R, T]) CreateInitialRestriction(func(E) R) {}

func (*Splittable[E, R, T]) InitialSplits(func(R) []R) {}

func (*Splittable[E, R, T]) SplitRestriction() {}

func (*Splittable[E, R, T]) CreateTracker() T {
	var t T
	return t
}

// SDFs
// PairWithRestriction + Size Restriction
// Initial Splits / Split Restriction
// ProcessBundle(E, R[P], T[R])
//  -> GetInitialPosition(R) P
//  -> for T.Claim(P) {
//       DoWork emit whatever
//    }
//
////
/// But what if we Combine Claim and the position into a callback loop?
//
// T.ProcessClaims(initialPosition	func(R)P,  func(P) P, error)
//
//

// OK, so we want to avoid users specifying manual looping, claiming etc. It's a feels bad API.
//
//

// TODO Watermark Estimators and ProcessContinuations for StreamingDoFn

//////////////////////
// State and Timers //
//////////////////////

type state struct{ beamMixin }

func (state) state() {}

type StateBag[E Element] struct{ state }
type StateValue[E Element] struct{ state }
type StateCombining[E Element] struct{ state }
type StateMap[K, V Element] struct{ state }
type StateSet[E Element] struct{ state }

type timer struct{ beamMixin }

func (timer) timer() {}

type TimerEvent struct{ timer }
type TimerProcessing struct{ timer }

// what else am I missing?
//
// Error and panic propagation.
//
// Triggers, Windowing, CustomWindowFn,
// Metrics
// GroupIntoBatches (With Sharded Key)
// CoGBK
//
//  CoCombine?
//
// Preserve Keys, Observe Keys
//
// DisplayData, Annotations
//
// DoFn Sampler and State Caching
//
// logging is slog.
