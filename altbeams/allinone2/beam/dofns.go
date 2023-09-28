package beam

// dofns.go is about the different mix-ins and addons that can be added.

type Emitter[E Element] struct {
	localDownstreamIndex int
}

type emitIface interface {
	setPColKey(id int)
	newDFC(id nodeIndex) processor
	newNode(global nodeIndex, parent edgeIndex, bounded bool) node
}

var _ emitIface = (*Emitter[any])(nil)

func (emt *Emitter[E]) setPColKey(id int) {
	emt.localDownstreamIndex = id
}

func (_ *Emitter[E]) newDFC(id nodeIndex) processor {
	return newDFC[E](id, nil)
}

func (_ *Emitter[E]) newNode(global nodeIndex, parent edgeIndex, bounded bool) node {
	return &typedNode[E]{index: global, parentEdge: parent, isBounded: bounded}
}

// Emit the element within the current element's context.
//
// The ElmC value is sourced from the [DFC.Process] method.
func (emt Emitter[E]) Emit(ec ElmC, elm E) {
	// derive the elmContext, and direct the element down to its PCollection handle
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

func (*OnBundleFinish) Do(dfc bundleFinisher, finishBundle func() error) {
	dfc.regBundleFinisher(finishBundle)
}

type ObserveWindow struct{}

func (*ObserveWindow) Get(ec ElmC) any {
	return ec.windows[0]
}

////////////////////////////////////////////////////////
// Below here are Not Yet Implemented field flavours. //
////////////////////////////////////////////////////////

type sideInput struct{}

func (*sideInput) isSideInput() {}

type sideIface interface{ isSideInput() }

type IterSideInput[E Element] struct{ sideInput }

type MapSideInput[E Element] struct{ sideInput }

// AfterBundle allows a DoFn to register a function that runs after
// the bundle has been durably committed. Emiting elements here will fail.
//
// TODO consider moving this to a simple interface function.
// Upside, not likely to try to incorrectly emit in the closure.
// Downside, the caching for anything to finalize needs to be stored in the DoFn struct
// this violates the potential of a ConfigOnly DoFn.
type AfterBundle struct{}

type bundleFinalizer interface {
	regBundleFinalizer(finalizeBundle func() error)
}

func (*AfterBundle) Do(dfc bundleFinalizer, finalizeBundle func() error) {
	dfc.regBundleFinalizer(finalizeBundle)
}

// How do we make SDFs work as expected with this paradigm, and be more construction time
// safe. Eg. Require both the marker, and certain methods to be implemented on the DoFn.
//

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

// TODO Watermark Estimators and ProcessContinuations for StreamingDoFn

//////////////////////
// State and Timers //
//////////////////////

type state struct{}

func (state) state() {}

type StateBag[E Element] struct{ state }
type StateValue[E Element] struct{ state }
type StateCombining[E Element] struct{ state }
type StateMap[K, V Element] struct{ state }
type StateSet[E Element] struct{ state }

type timer struct{}

func (timer) timer() {}

type TimerEvent struct{ timer }
type TimerProcessing struct{ timer }

// what else am I missing?
//
// Triggers, Windowing, CustomWindowFn,
// Metrics
// Partition 
// Flatten
// GroupIntoBatches (With Sharded Key)
//
// CombineFns.
//
// CreateAccumulator() A
// AddInput[I, A](I, A) A
// MergeAccumulators[A](A, A) A
// ExtractOutput[A, O](A) O
//
// Preserve Keys, Observe Keys
//
// DisplayData, Annotations
