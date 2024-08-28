package beam_test

import (
	"context"
	"fmt"
	"iter"
	"maps"
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

var (
	_ beam.Tracker[beam.OffsetRange, int64] = (*beam.ORTracker)(nil)
	_ beam.Restriction[int64]               = (beam.OffsetRange{})
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
	beam.BoundedSDF[simpleFac, int, *beam.ORTracker, beam.OffsetRange, int64, bool]

	Output beam.Output[beam.KV[int, int64]]
}

func (fn *countingSplitterFn) ProcessBundle(dfc *beam.DFC[int]) error {
	return fn.BoundedSDF.Process(dfc,
		func(rest beam.OffsetRange) *beam.ORTracker {
			return &beam.ORTracker{
				Rest: rest,
			}
		},
		func(ec beam.ElmC, i int, or beam.OffsetRange, tc beam.TryClaim[int64]) error {
			return tc(func(pos int64) (int64, error) {
				fn.Output.Emit(ec, beam.Pair(i, pos))
				return pos + 1, nil
			})
		})
}

type simpleFac struct{}

// This doesn't work, because we want a pointer, not a value type
// So we want to restrict it to be a pointer type.
func (simpleFac) Setup() error { return nil }

func (simpleFac) InitialSplit(_ int, r beam.OffsetRange) iter.Seq2[beam.OffsetRange, float64] {
	return maps.All(map[beam.OffsetRange]float64{
		r: float64(r.Max - r.Min),
	})
}

func (simpleFac) Produce(e int) beam.OffsetRange {
	fmt.Println("producing range for ", e)
	return beam.OffsetRange{0, int64(e) + 1}
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
	if got, want := int(pr.Counters["sink.Processed"]), 55; got != want {
		t.Fatalf("processed didn't match bench number: got %v want %v", got, want)
	}
}
