package beam

import (
	"context"
	"testing"

	"golang.org/x/exp/constraints"
)

func TestCombineKeyedSum(t *testing.T) {
	// We need to have all the keys, so 1.
	pr, err := Run(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: 10})
		keyedSrc := ParDo(s, src.Output, &AddFixedKeyFn[int]{})
		sums := CombinePerKey(s, keyedSrc.Output, SimpleMerge(SumFn[int]{}))
		ParDo(s, sums, &DiscardFn[KV[int, int]]{}, Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink.Processed"]), 1; got != want {
		t.Fatalf("processed didn't match bench number: got %v want %v", got, want)
	}
}

func TestCombineKeyedMean(t *testing.T) {
	// We need to have all the keys, so 1.
	pr, err := Run(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: 10})
		keyedSrc := ParDo(s, src.Output, &AddFixedKeyFn[int]{})
		means := CombinePerKey(s, keyedSrc.Output, FullCombine(MeanFn[int]{}))
		namedDiscard(s, means, "sink")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink.Processed"]), 1; got != want {
		t.Fatalf("processed didn't match bench number: got %v want %v", got, want)
	}
}

type SumFn[E constraints.Integer | constraints.Float] struct{}

func (SumFn[E]) MergeAccumulators(a E, b E) E {
	return a + b
}

type AddFixedKeyFn[E Element] struct {
	Output Emitter[KV[int, E]]
}

func (fn *AddFixedKeyFn[E]) ProcessBundle(_ context.Context, dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) error {
		fn.Output.Emit(ec, KV[int, E]{Key: 0, Value: elm})
		return nil
	})
	return nil
}

type MeanFn[E constraints.Integer | constraints.Float] struct{}

type meanAccum[E constraints.Integer | constraints.Float] struct {
	Count int32
	Sum   E
}

func (MeanFn[E]) AddInput(a meanAccum[E], i E) meanAccum[E] {
	a.Count += 1
	a.Sum += i
	return a
}

func (MeanFn[E]) MergeAccumulators(a meanAccum[E], b meanAccum[E]) meanAccum[E] {
	return meanAccum[E]{Count: a.Count + b.Count, Sum: a.Sum + a.Sum}
}

func (MeanFn[E]) ExtractOutput(a meanAccum[E]) float64 {
	return float64(a.Sum) / float64(a.Count)
}
