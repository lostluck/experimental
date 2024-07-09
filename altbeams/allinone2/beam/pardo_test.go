package beam_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/lostluck/experimental/altbeams/allinone2/beam"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"
)

// convenience function to allow the discard type to be inferred.
func namedDiscard[E beam.Element](s *beam.Scope, input beam.Output[E], name string) {
	beam.ParDo(s, input, &beam.DiscardFn[E]{}, beam.Name(name))
}

func pipeName(tb testing.TB) beamopts.Options {
	return beam.Name(tb.Name())
}

// 2024/07/06
// It's been a few months since I looked at this, and it looks like I've attempted
// the problem from the fnapi side already, and still need to sort it out from the
// user side.
//
// What I did for combinefns was to basically have users write a completely different
// transform style. Makes sense for Combines since they're weird by definition.
// SplittableDoFns are also weird, but conceptually we want as much of that to be the
// same as regular DoFns.
//
// It might be cleaner to simply make *all* DoFns SplittableDoFns, and have wrappers to
// simplify them later instead of assuming the basics. The analysis could detect
// "I'm simple" and proceed accordingly to avoid SDF complexity.
//
// Where the DoFns are the following:
//  E Element type (arbitrary)
//  R Restriction
//  M waterMark estimator.
//  S Size (float64)
//  W Windows
//  T Timestamps
//
// Watermark estimator is essentially arbitrary data.
//
// These are how the SDF URNs need to be handled.
//
// PairWithRestriction
//  In: WindowedValue(E, W, T)
//  Out: WindowedValue(KV<E, KV<R, M>>, W, T)  (E is unchanged)
//
// SplitAndSizeRestrictions
//  In: WindowedValue(KV<E, KV<R, M>>, W, T)
//  Out: WindowedValue(KV<KV<E, KV<R,M>, S>, W, T) (E is unchanged)
//
// TruncateSizedRestrictions
//  In: WindowedValue(KV<KV<E, KV<R,M>, S>, W, T)
//  Out: WindowedValue(KV<KV<E, KV<R,M>, S>, W, T)  (but with R updated to be finite)
//
// ProcessSizedElementsAndRestrictions
//   In: WindowedValue(KV<KV<E, KV<R,M>, S>, W, T)
//   Out: WindowedValue(Arbitrary, W, T)
//
// The user side ideally doesn't worry about all this.
// But we do need SDFs to process correctly.
// We need to be able to get the types of the Restriction and the Watermark Estimator,
// so the sub parts are instantiable.
// It needs to be able to be documented.
// It needs to be able to have the generic sub parts be instantiable.
//
// We also want ProcessContinuations to be possible too. But those don't have type problems at least.
//
// So perhaps we just need a magic thing for users to embed (field include) into their main type.
//
// BoundSDF[F[R[P],WE], WE, R[P], P]
// UnboundSDF[F[R[P],WE], RE[R[P],P], WE, R[P]] ?? (for handling Process Continuations?)
//
// With P for the position of the restriction, and RE for the Range End Estimator.
//
// If we make users write F (or provide F's), that simplifies the type definitions enough.
// R should be sizable by F, so it can be synchronized a bit for splits
//  (two goroutines need to interfere with restrictions)
//
// IIRC the only difference between the bound and unbound ones are Process continuations?
//
// Heh. What if we could just make it a loop iterator, and if ProcessContinuations break/return early
// we send the delay or time?
// Or we just have the ProcesContinuation be a state set by the user code,
// and they return/break out of the loop.
//
// Probably not. Easier to just have users provide their own which works out.
//
// But SDFs are typically handled by first getting the initial restrictions, and
// then shuffling them up, with the actual processing happening at the root
// of the processing graph. So we can pretty easily have special processing
// to hook in split handling, and special decoding handling.
//
// The difficulty is in allowing dynamic splits to happen within the given
// restriction. This is why the normal SDKs have a TryClaim approach.
//
// Consider textio: Produce lines of text as separated by newlines.
//
// It uses an offset range restriction but not from 0 to N for the number of
// new lines in the file, but over the number of bytes on the file. This is
// because we can't know where all the new lines are in the file when we start.
// We could pre-process the file, but that would be an inefficient scan, and
// that would be worse as larger files appear. So we start with what we have,
// which is the number of bytes in the file.
//
// This opens up a new problem: How to map from restrictions to lines of text?
// If the range starts at 0, then we know we're at the start of the file, so
// everying before that first newline is ours! We can roll from there, consuming
// text until we hit the newline, until the end of our restriction. So we don't
// end up with lines split into parts, we actually consume all text the first newline
// *after* the the end of our restriction.
//
// Because of that last property, how do we handle starts at i > 0?
// We need to discard everything before i for sure.
// We also need to discard everything until the first newline we see after i, since
// an "earlier" restriction would have produced that line.
// This means that not all restrictions will produce an output, in particular those that
// do not cover an entire line. That's OK.
//
// We can't allow claiming positions outside of the current restriction.
// And the current restriction must be fully claimed to ensure full processing.
// It's up to the textio code to only claim within the restriction, even though
// for restrictions that end before the last byte of the file, we may reach outside
// of our last position. And we have to claim positions whether or not the restriction
// produces output.
//
// So can we have something less finicky than the current very manual approach?
// Do we need that if we have type safety though?
//
// I think essentially, I don't want the user to have to manually have a tracker, since there
// are many methods on it, that only the framework should have access to.
// But we do want users to be able to write them, even if they're complicated.
//
// What is part of the DoFn vs part of the mechanics of the Restriction?
// Users clearly need access to the restriction itself, since it may have real values.
//
// The mongodbio SDF needs to claim positions *before* emitting them, because it doesn't
// know the ID of a value, and whether it's in it's range, until it's fetched them.
// This can lead to extra reads if a split happened, but this can be acceptable.
//
// But that just means a loop rotation, so it forces a consistency, even if it does mean an off
// side closure variable, to emit the *next* value.
//
// So I think all we need is a closure, that needs the element context, single element, and the restriction, and
// an iterator widget. The iterator widget is then called with a closure that accepts the input
// position, and computes the next position, and an error. Within, it can do whatever it likes.
//
// For a streaming output, it can return, a process continuation symbol,  the next position, and
// an error.

// OffsetRange is an offset range restriction.
type OffsetRange struct {
	Min, Max int64
}

func (r OffsetRange) Start() int64 {
	return r.Min
}

func (r OffsetRange) End() int64 {
	return r.Max
}

func (r OffsetRange) Bounded() bool {
	return r.Max != math.MaxInt64
}

// ORTracker is a tracker for an offset range restriction.
type ORTracker struct {
	rest      OffsetRange
	claimed   int64 // Tracks the last claimed position.
	stopped   bool  // Tracks whether TryClaim has indicated to stop processing elements.
	attempted int64 // Tracks the last attempted position to claim.
	err       error
}

// Size returns a an estimate of the amount of work in this restrction.
func (t *ORTracker) Size(rest OffsetRange) float64 {
	return float64(rest.Max - rest.Min)
}

// TryClaim validates that the position is within the restriction and has been unclaimed.
func (tracker *ORTracker) TryClaim(pos int64) bool {
	if tracker.stopped {
		tracker.err = errors.New("ORTracker: cannot claim work after restriction tracker returns false")
		return false
	}

	tracker.attempted = pos
	if pos < tracker.rest.Min {
		tracker.stopped = true
		tracker.err = fmt.Errorf("ORTracker: position claimed is out of bounds of the restriction: pos %v, rest.Min %v", pos, tracker.rest.Min)
		return false
	}
	if pos <= tracker.claimed {
		tracker.stopped = true
		tracker.err = fmt.Errorf("ORTracker: cannot claim a position lower than the previously claimed position: pos %v, claimed %v", pos, tracker.claimed)
		return false
	}

	tracker.claimed = pos
	if pos >= tracker.rest.Max {
		tracker.stopped = true
		return false
	}
	return true
}

// GetError returns the error that caused the tracker to stop, if there is one.
func (tracker *ORTracker) GetError() error {
	return tracker.err
}

// GetRestriction returns the restriction.
func (tracker *ORTracker) GetRestriction() OffsetRange {
	return tracker.rest
}

// TrySplit splits at the nearest integer greater than the given fraction of the remainder. If the
// fraction given is outside of the [0, 1] range, it is clamped to 0 or 1.
func (tracker *ORTracker) TrySplit(fraction float64) (primary, residual OffsetRange, err error) {
	if tracker.stopped || tracker.IsDone() {
		return tracker.rest, OffsetRange{}, nil
	}
	if fraction < 0 {
		fraction = 0
	} else if fraction > 1 {
		fraction = 1
	}

	// Use Ceil to always round up from float split point.
	// Use Max to make sure the split point is greater than the current claimed work since
	// claimed work belongs to the primary.
	splitPt := tracker.claimed + int64(math.Max(math.Ceil(fraction*float64(tracker.rest.Min-tracker.claimed)), 1))
	if splitPt >= tracker.rest.Max {
		return tracker.rest, OffsetRange{}, nil
	}
	residual = OffsetRange{splitPt, tracker.rest.Max}
	tracker.rest.Max = splitPt
	return tracker.rest, residual, nil
}

// GetProgress reports progress based on the claimed size and unclaimed sizes of the restriction.
func (tracker *ORTracker) GetProgress() (done, remaining float64) {
	done = float64((tracker.claimed + 1) - tracker.rest.Min)
	remaining = float64(tracker.rest.Max - (tracker.claimed + 1))
	return
}

// IsDone returns true if the most recent claimed element is at or past the end of the restriction
func (tracker *ORTracker) IsDone() bool {
	return tracker.err == nil && (tracker.claimed+1 >= tracker.rest.Max || tracker.rest.Min >= tracker.rest.Max)
}

func makeORTracker(rest OffsetRange) *ORTracker {
	return &ORTracker{
		rest: OffsetRange{Min: 0, Max: 10},
	}
}

var (
	_ beam.Tracker[OffsetRange, int64] = (*ORTracker)(nil)
	_ beam.Restriction[int64]          = (OffsetRange{})
)

// Requires GODEBUG=gotypesalias=1
// type ORSDF[E any] = beam.BoundedSDF[int, *ORTracker, OffsetRange, int64]

// TODO 2024/07/08: Add additional implementation of OR tracker for testing it.
// Move OffsetRange and company to main package for future
// convenience, and coverage.

// countingSplitterFn produces a number of elements based on the integer provide,
// with the input integer N as key.
// and the output being all integers i: 0 <= i < N-1
type countingSplitterFn struct {
	beam.BoundedSDF[int, *ORTracker, OffsetRange, int64]

	Output beam.Output[beam.KV[int, int64]]
}

func (fn *countingSplitterFn) ProcessBundle(_ context.Context, dfc *beam.DFC[int]) error {
	return fn.BoundedSDF.Process(dfc,
		makeORTracker,
		func(ec beam.ElmC, i int, or OffsetRange, tc beam.TryClaim[int64]) error {
			return tc(func(pos int64) (int64, error) {
				fn.Output.Emit(ec, beam.Pair(i, pos))
				return pos + 1, nil
			})
		})
}

func TestSplittableDoFn(t *testing.T) {
	pr, err := beam.Run(context.TODO(), func(s *beam.Scope) error {
		imp := beam.Impulse(s)
		src := beam.ParDo(s, imp, &beam.SourceFn{Count: 10})
		keyedSrc := beam.ParDo(s, src.Output, &countingSplitterFn{})
		namedDiscard(s, keyedSrc.Output, "sink")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	// TODO change this to use actually generated value counts.
	if got, want := int(pr.Counters["sink.Processed"]), 55; got != want {
		t.Fatalf("processed didn't match bench number: got %v want %v", got, want)
	}
}
