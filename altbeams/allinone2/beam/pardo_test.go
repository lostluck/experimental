package beam

import (
	"context"
	"testing"
)

type OnlySideIter[E Element] struct {
	Side SideInputIter[E]

	Out Emitter[E]
}

func (fn *OnlySideIter[E]) ProcessBundle(ctx context.Context, dfc *DFC[[]byte]) error {
	dfc.Process(func(ec ElmC, elm []byte) error {
		fn.Side.All(ec)(func(elm E) bool {
			fn.Out.Emit(ec, elm)
			return true
		})
		return nil
	})
	return nil
}

func TestSideInputIter(t *testing.T) {
	pr, err := Run(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: 10})
		onlySide := ParDo(s, imp, &OnlySideIter[int]{Side: AsSideIter(src.Output)})
		ParDo(s, onlySide.Out, &DiscardFn[int]{}, Name("sink"))
		return nil
	})
	if err != nil {
		t.Error(err)
	}

	if got, want := int(pr.Counters["sink.Processed"]), 10; got != want {
		t.Errorf("discard1 got %v, want %v", got, want)
	}
}
