package beam

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"math"
	"sync"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/coders"
	"golang.org/x/exp/slices"
)

// splitHelper is a helper function that finds a split point in a range.
//
// currIdx and endIdx should match the DataSource's index and splitIdx fields,
// and represent the start and end of the splittable range respectively.
//
// currProg represents the progress through the current element (currIdx).
//
// splits is an optional slice of valid split indices, and if nil then all
// indices are considered valid split points.
//
// frac must be between [0, 1], and represents a fraction of the remaining work
// that the split point aims to be as close as possible to.
//
// splittable indicates that sub-element splitting is possible (i.e. the next
// unit is an SDF).
//
// Returns the element index to split at (first element of residual). If the
// split position qualifies for sub-element splitting, then this also returns
// the fraction of remaining work in the current element to use as a split
// fraction for a sub-element split, and otherwise returns -1.
//
// A split point is sub-element splittable iff the split point is the current
// element, the splittable param is set to true, and both the element being
// split and the following element are valid split points.
func splitHelper(
	currIdx, endIdx int64,
	currProg float64,
	splits []int64,
	frac float64,
	splittable bool) (int64, float64, error) {
	// Get split index from fraction. Find the closest index to the fraction of
	// the remainder.
	start := float64(currIdx) + currProg
	safeStart := currIdx + 1 // safeStart avoids splitting at 0, or <= currIdx
	if safeStart <= 0 {
		safeStart = 1
	}
	var splitFloat = start + frac*(float64(endIdx)-start)

	// Handle simpler cases where all split points are valid first.
	if len(splits) == 0 {
		if splittable && int64(splitFloat) == currIdx {
			// Sub-element splitting is valid.
			_, f := math.Modf(splitFloat)
			// Convert from fraction of entire element to fraction of remainder.
			fr := (f - currProg) / (1.0 - currProg)
			return int64(splitFloat), fr, nil
		}
		// All split points are valid so just split at safe index closest to
		// fraction.
		splitIdx := int64(math.Round(splitFloat))
		if splitIdx < safeStart {
			splitIdx = safeStart
		}
		return splitIdx, -1.0, nil
	}

	// Cases where we have to find a valid split point.
	slices.Sort(splits)
	if splittable && int64(splitFloat) == currIdx {
		// Check valid split points to see if we can do a sub-element split.
		// We need to find the currIdx and currIdx + 1 for it to be valid.
		c, cp1 := false, false
		for _, s := range splits {
			if s == currIdx {
				c = true
			} else if s == currIdx+1 {
				cp1 = true
				break
			} else if s > currIdx+1 {
				break
			}
		}
		if c && cp1 { // Sub-element splitting is valid.
			_, f := math.Modf(splitFloat)
			// Convert from fraction of entire element to fraction of remainder.
			fr := (f - currProg) / (1.0 - currProg)
			return int64(splitFloat), fr, nil
		}
	}

	// For non-sub-element splitting, find the closest unprocessed split
	// point to our fraction.
	var prevDiff = math.MaxFloat64
	var bestS int64 = -1
	for _, s := range splits {
		if s >= safeStart && s <= endIdx {
			diff := math.Abs(splitFloat - float64(s))
			if diff <= prevDiff {
				prevDiff = diff
				bestS = s
			} else {
				break // Stop early if the difference starts increasing.
			}
		}
	}
	if bestS != -1 {
		return bestS, -1.0, nil
	}
	// Printing all splits is expensive. Instead, return the current start and
	// end indices, and fraction along with the range of the indices and how
	// many there are. This branch requires at least one split index, so we don't
	// need to bounds check the slice.
	return -1, -1.0, fmt.Errorf("failed to split DataSource (at index: %v, last index: %v) at fraction %.4f with requested splits (%v indices from %v to %v)",
		currIdx, endIdx, frac, len(splits), splits[0], splits[len(splits)-1])
}

// SplittableDoFns have a few components we need to be able to generate worker side.
// Like with CombineFns, we basically use a hidden wrapper to be able to generate
// the ones with the right types worker side.
//
// However, unlike Combines we aren't arresting what users can do, WRT outputs and similar,
// in particular for the final stage.
//
// The necessary components splittableDoFns are
// sdf_pair_with_restriction
// sdf_split_and_size_restrictions
// sdf_process_sized_element_and_restrictions <- basically what the user "wrote"
// And for unbounded SDFs
// sdf_truncate_sized_restrictions
//
// We'll be reusing this shorthand a bit, so we're clarifying it here.
// The original element type is O
// Restriction is R.
// The Watermark Estimator State is WES
// The Size is a float64 we'll call S.
//
// The Watermark Estimator State isn't a "real" concept in Beam, WRT the model, but it's
// defined to be and it is folded into the into the RestrictionCoder with a KV type.
// We'll default this to a boolean in this initial implementation so we can ensure the
// plumbing exists for later.
//
// Pair with Restriction
// Input: O
// Output: KV<O, KV<R,WE>> (singular)
//
// Makes the initial restriction that represents processing all of O.
//
// Split and Size Restrictions
// Input:  KV<O, KV<R,WE>>
// Output: KV< KV<O, KV<R,WE>>, S> (at least one)
//
// Produces one or more sub restrictions based on the initial restriction. They also include the
// sizes of the individual elements.
//
// Process Sized Element And Restrictiosn
// Input:  KV< KV<O, KV<R,WE>>, S>
// Output: Whatever the user wants.
//
// Then we process all the restricted and sized elements, outputing as the user code requires.
// There may be runner side calls to split, which will bisect restrictions as needed.
// This is ultimately behind the scenes of user side code to avoid loss or duplication of work.
// This is handled by the tracker which is what manages the current state of processing of
// the restriction.

type pairWithRestriction[FAC RestrictionFactory[O, R, P], O Element, R Restriction[P], P, WES any] struct {
	Factory FAC
	Output  Output[KV[O, KV[R, WES]]]
}

func (fn *pairWithRestriction[FAC, O, R, P, WES]) ProcessBundle(ctx context.Context, dfc *DFC[O]) error {
	if err := fn.Factory.Setup(); err != nil {
		return err
	}
	return dfc.Process(func(ec ElmC, elm O) error {
		// TODO, how to actually create the initial restriction for real?
		// How much setup do people want/need for this?
		// How much amortized work needs to be available? RPCs etc?
		//
		// Require a bonus function?
		// A different factory type? <- Probably this.
		r := fn.Factory.Produce(elm)
		var wes WES

		fn.Output.Emit(ec, Pair(elm, Pair(r, wes)))
		return nil
	})
}

// Restriction Factory must have a valid zero value. It will not be serialized?
// HMMM. Might need to support receiving the user DoFn as configuration.
type RestrictionFactory[O Element, R Restriction[P], P any] interface {
	// Setup takes in the DoFn for configuration?
	Setup() error
	// Produce returns a restriction that processess the entire element.
	Produce(O) R

	// InitialSplit returns an iterator of a non-overlapping sub restriction and it's relative size.
	InitialSplit(O, R) iter.Seq2[R, float64]
}

type splitAndSizeRestrictions[FAC RestrictionFactory[O, R, P], O Element, R Restriction[P], P, WES any] struct {
	Factory FAC
	Output  Output[KV[KV[O, KV[R, WES]], float64]]
}

func (fn *splitAndSizeRestrictions[FAC, O, R, P, WES]) ProcessBundle(ctx context.Context, dfc *DFC[KV[O, KV[R, WES]]]) error {
	fn.Factory.Setup()
	return dfc.Process(func(ec ElmC, elm KV[O, KV[R, WES]]) error {
		for subR, size := range fn.Factory.InitialSplit(elm.Key, elm.Value.Key) {
			fn.Output.Emit(ec, Pair(Pair(elm.Key, Pair(subR, elm.Value.Value)), size))
		}
		return nil
	})
}

type processSizedElementAndRestriction[FAC RestrictionFactory[O, R, P], O Element, T Tracker[R, P], R Restriction[P], P, WES any] struct {
	Transform[O]
	// initialized at construction.
	// This needs to be the full WindowedValue that we use as input, not just the restriction.
	fullElementCoder coders.Coder[KV[KV[O, KV[R, WES]], float64]]
	tid, inputID     string

	// Locking tracker for asynchronous split management.
	// Holds the current tracker.
	mu      sync.Mutex
	tracker *lockingTracker[T, R, P]
	curEc   ElmC
	curElm  KV[KV[O, KV[R, WES]], float64]
}

func (fn *processSizedElementAndRestriction[FAC, O, T, R, P, WES]) ProcessBundle(ctx context.Context, dfc *DFC[KV[KV[O, KV[R, WES]], float64]]) error {

	// Create a "fake" DFC to pass to the user ProcessBundle.
	// Like normal processing, we use this to extract configuration from the user
	// that we then manipulate and execute.
	userDfc := &DFC[O]{
		id: dfc.id, logger: dfc.logger, edgeID: dfc.edgeID,
		transform: dfc.transform, downstream: dfc.downstream,
		metrics: dfc.metrics,
	}

	// User transform is initialized like usual here.
	if err := fn.Transform.ProcessBundle(ctx, userDfc); err != nil {
		return err
	}
	if userDfc.perElm != nil {
		return fmt.Errorf("User transform called *DFC.Process, but should have called SDF.Process.")
	}
	dfc.finishBundle = userDfc.finishBundle

	// But now the user DFC has the correct SDF handler init ialization to proc
	makeTracker := userDfc.makeTracker.(func(R) T)
	perElmAndRest := userDfc.perElmAndRest.(ProcessRestriction[O, R, P])

	// But *now* we use the existing DFC loop to go through each true element in the bundle
	// and do the boiler plate handling instead of the user.
	return dfc.Process(func(ec ElmC, fullElm KV[KV[O, KV[R, WES]], float64]) error {
		// Extract the restriction and create the tracker.
		r := fullElm.Key.Value.Key

		// Manage per element state so it can be accessed in
		// the split thread.
		fn.mu.Lock()
		fn.curElm = fullElm
		fn.curEc = ec
		fn.tracker = wrapWithLockTracker(makeTracker(r), &fn.mu)
		fn.mu.Unlock()

		defer func() {
			fn.mu.Lock()
			defer fn.mu.Unlock()
			fn.tracker = nil
			fn.curElm = KV[KV[O, KV[R, WES]], float64]{}
			fn.curEc = ElmC{}
		}()

		return perElmAndRest(ec, fullElm.Key.Key, fn.tracker.GetRestriction(),
			/*TryClaim*/ func(perPos func(P) (P, error)) error {
				p := r.Start()
				// We don't need to claim the initial position, hence the tail
				// break condition instead of waiting for the end.
				for {
					newPos, err := perPos(p)
					if err != nil {
						return err
					}
					p = newPos
					if !fn.tracker.TryClaim(p) {
						break
					}
				}
				return fn.tracker.GetError()
			})
	})
}

func (fn *processSizedElementAndRestriction[FAC, O, T, R, P, WES]) getUserTransform() any {
	return fn.Transform
}

func (fn *processSizedElementAndRestriction[FAC, O, T, R, P, WES]) splitElementSource() (float64, elmSplitCallback) {
	// At worse the progess here is a little behind.
	// It shouldn't really change subsequent split decisions.
	// The important thing is that we prevent SDK processing
	// while we're actually committing to the split.
	done, remaining := fn.tracker.GetProgress()
	prog := done / (done + remaining)

	return prog, func(fraction float64) elementSplitResult {
		// Lock to prevent new claims and similar befor the split is complete
		fn.mu.Lock()
		// Get a local instance of what we're working with
		// After the split, we don't care if the element makes additional progress
		// on the remaining restriction.
		ec := fn.curEc
		elm := fn.curElm
		wrapped := fn.tracker.wrapped

		prim, resi, err := wrapped.TrySplit(fraction)
		fn.mu.Unlock()
		if err != nil {
			// TODO what do we do here?
			slog.Error("sub element split error", "error", err)
		}
		primBuffer := coders.NewEncoder()
		resiBuffer := coders.NewEncoder()

		// This needs to be the fullly qualified WindowedValueElement with the new restriction and size.
		// TODO get the real window/pane here. Better event time plumbing.
		coders.EncodeWindowedValueHeader(primBuffer, ec.EventTime(), []coders.GWC{{}}, coders.PaneInfo{})
		coders.EncodeWindowedValueHeader(resiBuffer, ec.EventTime(), []coders.GWC{{}}, coders.PaneInfo{})

		primSize := wrapped.Size(prim)
		resiSize := wrapped.Size(resi)
		// KV[KV[O, KV[R, WES]], float64]
		// TODO, manually split out encoding to the component parts
		// WVH header, element *then restriction and watermark estimator state* to avoid duplicate encoding work.
		fn.fullElementCoder.Encode(primBuffer, Pair(Pair(elm.Key.Key, Pair(prim, elm.Key.Value.Value)), primSize))
		fn.fullElementCoder.Encode(resiBuffer, Pair(Pair(elm.Key.Key, Pair(resi, elm.Key.Value.Value)), resiSize))
		return elementSplitResult{
			PS:   [][]byte{primBuffer.Data()},
			RS:   [][]byte{resiBuffer.Data()},
			TId:  fn.tid,
			InId: fn.inputID,
			// TODO watermark estimator state.
		}
	}
}

var _ procSizedElmAndRestIface = &processSizedElementAndRestriction[RestrictionFactory[int, Restriction[int], int], int, Tracker[Restriction[int], int], Restriction[int], int, int]{}

type procSizedElmAndRestIface interface {
	getUserTransform() any
	splitElementSource() (prog float64, splitElement elmSplitCallback)
}

// TODO Truncate Restricton.
