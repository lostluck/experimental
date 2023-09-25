package beam

import (
	"context"
	"fmt"
	"testing"
	"time"

	"golang.org/x/exp/constraints"
)

type SourceFn struct {
	Count  int
	Output Emitter[int]
}

func (fn *SourceFn) ProcessBundle(ctx context.Context, dfc *DFC[[]byte]) error {
	// Do some startbundle work.
	processed := 0
	dfc.Process(func(ec ElmC, _ []byte) bool {
		for i := 0; i < fn.Count; i++ {
			processed++
			fn.Output.Emit(ec, i)
		}
		return true
	})
	return nil
}

type DiscardFn[E Elements] struct {
	processed int
}

func (fn *DiscardFn[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	fn.processed = 0
	dfc.Process(func(ec ElmC, elm E) bool {
		fn.processed++
		return true
	})
	return nil
}

type IdenFn[E Elements] struct {
	Output Emitter[E]
}

func (fn *IdenFn[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) bool {
		fn.Output.Emit(ec, elm)
		return true
	})
	return nil
}

func TestBuild(t *testing.T) {
	imp := Impulse()
	src := ParDo(imp, &SourceFn{Count: 10})
	ParDo(src[0], &DiscardFn[int]{})

	Start(imp)
}

// BenchmarkPipe benchmarks along the number of DoFns.
//
// goos: linux
// goarch: amd64
// pkg: github.com/lostluck/experimental/altbeams/allinone2/beam
// cpu: 12th Gen Intel(R) Core(TM) i7-1260P
// BenchmarkPipe/var_dofns_0-16         	97536266	        11.86 ns/op	        11.00 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_1-16         	38748386	        30.96 ns/op	        30.00 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_2-16         	24103524	        49.15 ns/op	        24.00 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_3-16         	17758483	        67.46 ns/op	        22.00 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_5-16         	11575345	       103.7 ns/op	        20.00 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_10-16        	 6107569	       196.5 ns/op	        19.00 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_100-16       	  450638	      2234 ns/op	        22.00 ns/elm	       0 B/op	       0 allocs/op
func BenchmarkPipe(b *testing.B) {
	makeBench := func(numDoFns int) func(b *testing.B) {
		return func(b *testing.B) {
			b.ReportAllocs()
			imp := Impulse()
			src := ParDo(imp, &SourceFn{Count: b.N})
			iden := src
			for i := 0; i < numDoFns; i++ {
				iden = ParDo(iden[0], &IdenFn[int]{})
			}
			discard := &DiscardFn[int]{}
			ParDo(iden[0], discard)
			b.ResetTimer()

			Start(imp)
			if discard.processed != b.N {
				b.Fatalf("processed dodn't match bench number: got %v want %v", discard.processed, b.N)
			}
			d := b.Elapsed()
			div := numDoFns
			if div == 0 {
				div = 1
			}
			div = div * b.N
			b.ReportMetric(float64(d/(time.Duration(div))), "ns/elm")
		}
	}
	for _, numDoFns := range []int{0, 1, 2, 3, 5, 10, 100} {
		b.Run(fmt.Sprintf("var_dofns_%d", numDoFns), makeBench(numDoFns))
	}
}

type KeyMod[V constraints.Integer] struct {
	Mod V

	Output Emitter[KV[V, V]]
}

func (fn *KeyMod[V]) ProcessBundle(ctx context.Context, dfc *DFC[V]) error {
	dfc.Process(func(ec ElmC, elm V) bool {
		mod := elm % fn.Mod
		fn.Output.Emit(ec, KV[V, V]{
			Key:   V(mod),
			Value: elm,
		})
		return true
	})
	return nil
}

type SumByKey[K Keys, V constraints.Integer | constraints.Float] struct {
	Output Emitter[KV[K, V]]
}

func (fn *SumByKey[K, V]) ProcessBundle(ctx context.Context, dfc *DFC[KV[K, Iter[V]]]) error {
	dfc.Process(func(ec ElmC, elm KV[K, Iter[V]]) bool {
		var sum V
		elm.Value.All(func(elm V) bool {
			sum += elm
			return true
		})
		fn.Output.Emit(ec, KV[K, V]{Key: elm.Key, Value: sum})
		return true
	})
	return nil
}

type GroupKeyModSum[V constraints.Integer] struct {
	Mod V

	Output Emitter[KV[V, V]]
}

func (fn *GroupKeyModSum[V]) ProcessBundle(ctx context.Context, dfc *DFC[V]) error {
	grouped := map[V]V{}
	dfc.Process(func(ec ElmC, elm V) bool {
		mod := elm % fn.Mod
		v := grouped[mod]
		v += elm
		grouped[mod] = v
		return true
	})

	dfc.FinishBundle(func() error {
		ec := dfc.ToElmC(EOGW) // TODO pull from the window that's been closed.
		for k, v := range grouped {
			fn.Output.Emit(ec, KV[V, V]{Key: k, Value: v})
		}
		return nil
	})
	return nil
}

func TestGBKSum(t *testing.T) {
	imp := Impulse()
	src := ParDo(imp, &SourceFn{Count: 10})
	mod := 3
	keyed := ParDo(src[0], &KeyMod[int]{Mod: mod})
	grouped := GBK[int, int](keyed[0])
	sums := ParDo(grouped, &SumByKey[int, int]{})

	discard := &DiscardFn[KV[int, int]]{}
	ParDo(sums[0], discard)
	Start(imp)

	if got, want := discard.processed, mod; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func BenchmarkGBKSum_int(b *testing.B) {
	for _, mod := range []int{2, 3, 5, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("mod_%v", mod), func(b *testing.B) {
			imp := Impulse()
			src := ParDo(imp, &SourceFn{Count: b.N})
			keyed := ParDo(src[0], &KeyMod[int]{Mod: mod})
			grouped := GBK[int, int](keyed[0])
			sums := ParDo(grouped, &SumByKey[int, int]{})

			discard := &DiscardFn[KV[int, int]]{}
			ParDo(sums[0], discard)

			b.ResetTimer()
			Start(imp)

			want := mod
			if b.N < mod {
				want = b.N
			}

			if got := discard.processed; got != want {
				b.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

func BenchmarkGBKSum_Lifted_int(b *testing.B) {
	for _, mod := range []int{2, 3, 5, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("mod_%v", mod), func(b *testing.B) {
			imp := Impulse()
			src := ParDo(imp, &SourceFn{Count: b.N})
			keyed := ParDo(src[0], &GroupKeyModSum[int]{Mod: mod})
			discard := &DiscardFn[KV[int, int]]{}
			ParDo(keyed[0], discard)

			b.ResetTimer()
			Start(imp)

			want := mod
			if b.N < mod {
				want = b.N
			}

			if got := discard.processed; got != want {
				b.Errorf("got %v, want %v", got, want)
			}
		})
	}
}
