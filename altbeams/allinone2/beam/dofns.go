package beam

import "fmt"

// beamMixin is added to all DoFn beam field types to allow them to bypass
// encoding. Only needed when the value has state and shouldn't be embedded.
type beamMixin struct{}

func (beamMixin) beamBypass() {}

type bypassInterface interface {
	beamBypass()
}

// dofns.go is about the different mix-ins and addons that can be added.

// Emitter represents an output of a DoFn.
//
// At pipeline construction time, they represent an output PCollection, and
// can be connected as an input to downstream DoFns. At pipeline execution
// time, they are used in a ProcessBundle method to emit outputs and pass along
// per element context, such as the EventTime and Window.
type Emitter[E Element] struct {
	beamMixin

	valid                bool
	globalIndex          nodeIndex
	localDownstreamIndex int
}

type emitIface interface {
	setPColKey(global nodeIndex, id int)
	newDFC(id nodeIndex) processor
	newNode(global nodeIndex, parent edgeIndex, bounded bool) node
}

var _ emitIface = (*Emitter[any])(nil)

func (emt *Emitter[E]) setPColKey(global nodeIndex, id int) {
	emt.valid = true
	emt.globalIndex = global
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
func (emt *Emitter[E]) Emit(ec ElmC, elm E) {
	proc := ec.pcollections[emt.localDownstreamIndex]
	// TODO: PCollection metrics are correct here.
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

////////////////////////////////////////////////////////
// Below here are Not Yet Implemented field flavours. //
////////////////////////////////////////////////////////

// ObserveWindow indicates this DoFn needs to be aware of windows explicitly.
// Typical use is to embed ObserveWindows as a field.
type ObserveWindow struct{}

func (*ObserveWindow) Of(ec ElmC) any {
	// When windows are observable, only a single window is present.
	return ec.windows[0]
}

type sideInputCommon struct {
	beamMixin

	valid  bool
	global nodeIndex
}

func (si *sideInputCommon) sideInput() nodeIndex {
	return si.global
}

type sideIface interface{ sideInput() nodeIndex }

type IterSideInput[E Element] struct{ sideInputCommon }

var _ sideIface = &IterSideInput[int]{}

func (si *IterSideInput[E]) All(ec ElmC) func(perElm func(elm E) bool) {
	return func(perElm func(elm E) bool) {
		panic("uninitialized side input iterator")
	}
}

func validateSideInput[E any](emt Emitter[E]) {
	if !emt.valid {
		panic("emitter is invalid")
	}
	var e E
	if isMetaType(e) {
		panic(fmt.Sprintf("type %T cannot be used as a side input value", e))
	}
}

// AsSideIter initializes an IterSideInput from a valid upstream Emitter.
// It allows access to the data of that Emitter's PCollection,
func AsSideIter[E Element](emt Emitter[E]) IterSideInput[E] {
	validateSideInput(emt)
	return IterSideInput[E]{sideInputCommon{valid: true, global: emt.globalIndex}}
}

// MapSideInput allows a side input to be accessed via key lookups.
type MapSideInput[K Keys, V Element] struct{ sideInputCommon }

var _ sideIface = &MapSideInput[int, int]{}

// Get looks up an iterator of values associated with the key.
func (si *MapSideInput[K, V]) Get(ec ElmC, k K) Iter[V] {
	return Iter[V]{source: nil}
}

// AsSideMap initializes a MapSideInput from a valid upstream Emitter.
func AsSideMap[K Keys, V Element](emt Emitter[KV[K, V]]) MapSideInput[K, V] {
	validateSideInput(emt)
	return MapSideInput[K, V]{sideInputCommon{valid: true, global: emt.globalIndex}}
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
// CombineFns.
//
//  - CreateAccumulator() A
//  - AddInput[I, A](I, A) A
//  - MergeAccumulators[A](A, A) A
//  - ExtractOutput[A, O](A) O
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
