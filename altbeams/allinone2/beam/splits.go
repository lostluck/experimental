package beam

import (
	"fmt"
	"math"

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
