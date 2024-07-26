package beam

import (
	"errors"
	"fmt"
	"math"
)

// TODO, refine properly and move to a better location.
// This is just where we're putting a basic SDF Tracker and Restriction for now.

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

var (
	_ Tracker[OffsetRange, int64] = (*ORTracker)(nil)
	_ Restriction[int64]          = (OffsetRange{})
)
