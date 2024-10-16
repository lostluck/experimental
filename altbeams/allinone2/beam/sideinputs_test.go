package beam

import (
	"context"
	"testing"
)

// convenience function to allow the discard type to be inferred.
func namedDiscard[E Element](s *Scope, input PCol[E], name string) {
	ParDo(s, input, &DiscardFn[E]{}, Name(name))
}

func TestSideInputIter(t *testing.T) {
	pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: 10})
		onlySide := ParDo(s, imp, &OnlySideIter[int]{Side: AsSideIter(src.Output)})
		namedDiscard(s, onlySide.Out, "sink")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink.Processed"]), 10; got != want {
		t.Errorf("discard1 got %v, want %v", got, want)
	}
}

func TestSideInputMap(t *testing.T) {
	pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: 10})
		kvsrc := ParDo(s, src.Output, &KeyMod[int]{Mod: 3})
		onlySide := ParDo(s, imp, &OnlySideMap[int, int]{Side: AsSideMap(kvsrc.Output)})
		namedDiscard(s, onlySide.Out, "sink")
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink.Processed"]), 10; got != want {
		t.Errorf("discard1 got %v, want %v", got, want)
	}
}

type OnlySideIter[E Element] struct {
	Side SideInputIter[E]

	Out PCol[E]
}

func (fn *OnlySideIter[E]) ProcessBundle(dfc *DFC[[]byte]) error {
	return dfc.Process(func(ec ElmC, elm []byte) error {
		fn.Side.All(ec)(func(elm E) bool {
			fn.Out.Emit(ec, elm)
			return true
		})
		return nil
	})
}

type OnlySideMap[K, V Element] struct {
	Side SideInputMap[K, V]

	Out PCol[KV[K, V]]
}

func (fn *OnlySideMap[K, V]) ProcessBundle(dfc *DFC[[]byte]) error {
	return dfc.Process(func(ec ElmC, elm []byte) error {
		fn.Side.Keys(ec)(func(key K) bool {
			fn.Side.Get(ec, key)(func(val V) bool {
				fn.Out.Emit(ec, KV[K, V]{key, val})
				return true
			})
			return true
		})
		return nil
	})
}
