package beam_test

import (
	"context"
	"testing"

	"github.com/lostluck/experimental/altbeams/allinone2/beam"
)

type countFn[E comparable] struct {
	Countable []E

	Hit, Miss beam.CounterInt64
}

func (fn *countFn[E]) ProcessBundle(dfc *beam.DFC[E]) error {
	return dfc.Process(func(ec beam.ElmC, elm E) error {
		for _, countable := range fn.Countable {
			if elm == countable {
				fn.Hit.Inc(dfc, 1)
				return nil
			}
		}
		fn.Miss.Inc(dfc, 1)
		return nil
	})
}

func TestLightweight(t *testing.T) {
	p, err := beam.LaunchAndWait(context.TODO(), func(s *beam.Scope) error {
		imp := beam.Impulse(s)
		wantWord := "squeamish_ossiphrage"
		out1 := beam.Map(s, imp, func([]byte) string { return wantWord })
		beam.ParDo(s, out1, &countFn[string]{
			Countable: []string{wantWord},
		}, beam.Name("count"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Errorf("pipeline failed: %v", err)
	}
	if got, want := p.Counters["count.Hit"], int64(1); got != want {
		t.Errorf("Hit an unexpected amount, got %v, want %v", got, want)
	}
	if got, want := p.Counters["count.Miss"], int64(0); got != want {
		t.Errorf("Missed an unexpected amount, got %v, want %v", got, want)
	}
}
