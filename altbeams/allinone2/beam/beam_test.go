package beam

import (
	"context"
	"fmt"
	"testing"

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

type DiscardFn[E Element] struct {
	start, processed, finished int

	OnBundleFinish
}

func (fn *DiscardFn[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	fn.processed = 0
	fn.start++
	dfc.Process(func(ec ElmC, elm E) bool {
		fn.processed++
		return true
	})
	fn.OnBundleFinish.Do(dfc, func() error {
		fn.finished++
		if fn.finished > fn.start {
			return fmt.Errorf("DiscardFn finished more times than started: %v > %v", fn.finished, fn.start)
		}
		return nil
	})
	return nil
}

type IdenFn[E Element] struct {
	Output Emitter[E]
}

func (fn *IdenFn[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) bool {
		fn.Output.Emit(ec, elm)
		return true
	})
	return nil
}

func TestSimple(t *testing.T) {
	Run(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: 10})
		ParDo(s, src.Output, &DiscardFn[int]{})
		return nil
	})
}

func TestAutomaticDiscard(t *testing.T) {
	Run(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		ParDo(s, imp, &SourceFn{Count: 10})
		// drop the output.
		return nil
	})
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

			discard := &DiscardFn[int]{}
			if err := Run(context.TODO(), func(s *Scope) error {
				imp := Impulse(s)
				src := ParDo(s, imp, &SourceFn{Count: b.N})
				iden := src.Output
				for i := 0; i < numDoFns; i++ {
					iden = ParDo(s, iden, &IdenFn[int]{}).Output
				}
				ParDo(s, iden, discard)
				return nil
			}); err != nil {
				b.Errorf("Run error: %v", err)
			}
			if discard.processed != b.N {
				b.Fatalf("processed didn't match bench number: got %v want %v", discard.processed, b.N)
			}
			if discard.finished != 1 {
				b.Fatalf("finished didn't match bundle counter: got %v want %v", discard.finished, 1)
			}
			d := b.Elapsed()
			div := numDoFns
			if div == 0 {
				div = 1
			}
			div = div * b.N
			b.ReportMetric(float64(d)/float64(div), "ns/elm")
		}
	}
	for _, numDoFns := range []int{0, 1, 2, 3, 5, 10, 100} {
		b.Run(fmt.Sprintf("var_dofns_%d", numDoFns), makeBench(numDoFns))
	}
}

type ModPartition[V constraints.Integer] struct {
	Outputs []Emitter[V] // The count needs to be properly serialized, ultimately.
}

func (fn *ModPartition[V]) ProcessBundle(ctx context.Context, dfc *DFC[V]) error {
	mod := V(len(fn.Outputs))
	dfc.Process(func(ec ElmC, elm V) bool {
		rem := elm % mod
		fn.Outputs[rem].Emit(ec, elm)
		return true
	})
	return nil
}

type WideNarrow struct {
	Wide int

	In Emitter[int]
}

var _ Composite[struct{ Out Emitter[int] }] = ((*WideNarrow)(nil))

func (src *WideNarrow) Expand(s *Scope) (out struct{ Out Emitter[int] }) {
	partition := ParDo(s, src.In, &ModPartition[int]{Outputs: make([]Emitter[int], src.Wide)})
	out.Out = Flatten(s, partition.Outputs...)
	return out
}

func TestPartitionFlatten(t *testing.T) {
	discard := &DiscardFn[int]{}
	count, mod := 100, 10
	Run(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: count})
		exp := Expand(s, "WideNarrow", &WideNarrow{Wide: mod, In: src.Output})
		ParDo(s, exp.Out, discard)
		return nil
	})
	if discard.processed != count {
		t.Fatalf("processed dodn't match bench number: got %v want %v", discard.processed, count)
	}
	if discard.finished != 1 {
		t.Fatalf("finished didn't match bundle countr: got %v want %v", discard.finished, 1)
	}
}

// BenchmarkPartitionPipe benchmarks dispatch across arbitrary partioning, and a flatten.
//
// goos: linux
// goarch: amd64
// pkg: github.com/lostluck/experimental/altbeams/allinone2/beam
// cpu: 12th Gen Intel(R) Core(TM) i7-1260P
// BenchmarkPartitionPipe/num_partitions_1-16         	26054823	        45.68 ns/op	       0 B/op	       0 allocs/op
// BenchmarkPartitionPipe/num_partitions_2-16         	25842020	        45.76 ns/op	       0 B/op	       0 allocs/op
// BenchmarkPartitionPipe/num_partitions_3-16         	26205663	        45.62 ns/op	       0 B/op	       0 allocs/op
// BenchmarkPartitionPipe/num_partitions_5-16         	26325379	        45.63 ns/op	       0 B/op	       0 allocs/op
// BenchmarkPartitionPipe/num_partitions_10-16        	26314922	        45.64 ns/op	       0 B/op	       0 allocs/op
// BenchmarkPartitionPipe/num_partitions_100-16       	26035390	        45.79 ns/op	       0 B/op	       0 allocs/op
func BenchmarkPartitionPipe(b *testing.B) {
	makeBench := func(numPartitions int) func(b *testing.B) {
		return func(b *testing.B) {
			b.ReportAllocs()

			discard := &DiscardFn[int]{}
			Run(context.TODO(), func(s *Scope) error {
				imp := Impulse(s)
				src := ParDo(s, imp, &SourceFn{Count: b.N})
				exp := Expand(s, "WideNarrow", &WideNarrow{Wide: numPartitions, In: src.Output})
				ParDo(s, exp.Out, discard)
				return nil
			})
			if discard.processed != b.N {
				b.Fatalf("processed dodn't match bench number: got %v want %v", discard.processed, b.N)
			}
		}
	}
	for _, numDoFns := range []int{1, 2, 3, 5, 10, 100} {
		b.Run(fmt.Sprintf("num_partitions_%d", numDoFns), makeBench(numDoFns))
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
		elm.Value.All()(func(elm V) bool {
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

	OnBundleFinish
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

	fn.OnBundleFinish.Do(dfc, func() error {
		ec := dfc.ToElmC(EOGW) // TODO pull from the window that's been closed.
		for k, v := range grouped {
			fn.Output.Emit(ec, KV[V, V]{Key: k, Value: v})
		}
		return nil
	})
	return nil
}

func TestGBKSum(t *testing.T) {
	discard := &DiscardFn[KV[int, int]]{}
	mod := 3
	Run(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: 10})
		keyed := ParDo(s, src.Output, &KeyMod[int]{Mod: mod})
		grouped := GBK[int, int](s, keyed.Output)
		sums := ParDo(s, grouped, &SumByKey[int, int]{})
		ParDo(s, sums.Output, discard)
		return nil
	})

	if got, want := discard.processed, mod; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func BenchmarkGBKSum_int(b *testing.B) {
	for _, mod := range []int{2, 3, 5, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("mod_%v", mod), func(b *testing.B) {
			discard := &DiscardFn[KV[int, int]]{}
			Run(context.TODO(), func(s *Scope) error {
				imp := Impulse(s)
				src := ParDo(s, imp, &SourceFn{Count: b.N})
				keyed := ParDo(s, src.Output, &KeyMod[int]{Mod: mod})
				grouped := GBK[int, int](s, keyed.Output)
				sums := ParDo(s, grouped, &SumByKey[int, int]{})
				ParDo(s, sums.Output, discard)
				return nil
			})

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

			discard := &DiscardFn[KV[int, int]]{}
			Run(context.TODO(), func(s *Scope) error {
				imp := Impulse(s)
				src := ParDo(s, imp, &SourceFn{Count: b.N})
				keyed := ParDo(s, src.Output, &GroupKeyModSum[int]{Mod: mod})
				ParDo(s, keyed.Output, discard)
				return nil
			})
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

func TestTwoSubGraphs(t *testing.T) {
	discard1, discard2 := &DiscardFn[int]{}, &DiscardFn[int]{}
	count := 10
	Run(context.TODO(), func(s *Scope) error {
		imp1, imp2 := Impulse(s), Impulse(s)
		src1, src2 := ParDo(s, imp1, &SourceFn{Count: 10}), ParDo(s, imp2, &SourceFn{Count: count})
		ParDo(s, src1.Output, discard1)
		ParDo(s, src2.Output, discard2)
		return nil
	})

	if got, want := discard1.processed, count; got != want {
		t.Errorf("discard1 got %v, want %v", got, want)
	}
	if got, want := discard2.processed, count; got != want {
		t.Errorf("discard2 got %v, want %v", got, want)
	}
	if discard1.finished != 1 {
		t.Fatalf("finished didn't match bundle counter: got %v want %v", discard1.finished, 1)
	}
	if discard2.finished != 1 {
		t.Fatalf("finished didn't match bundle counter: got %v want %v", discard1.finished, 1)
	}
}

func TestMultiplex(t *testing.T) {
	discard1, discard2 := &DiscardFn[int]{}, &DiscardFn[int]{}
	count := 10
	Run(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src1, src2 := ParDo(s, imp, &SourceFn{Count: 10}), ParDo(s, imp, &SourceFn{Count: count})
		ParDo(s, src1.Output, discard1)
		ParDo(s, src2.Output, discard2)
		return nil
	})

	if got, want := discard1.processed, count; got != want {
		t.Errorf("discard1 got %v, want %v", got, want)
	}
	if got, want := discard2.processed, count; got != want {
		t.Errorf("discard2 got %v, want %v", got, want)
	}
	if discard1.finished != 1 {
		t.Fatalf("finished didn't match bundle counter: got %v want %v", discard1.finished, 1)
	}
	if discard2.finished != 1 {
		t.Fatalf("finished didn't match bundle counter: got %v want %v", discard1.finished, 1)
	}
}

type OnlySideIter[E Element] struct {
	Side IterSideInput[E]

	Out Emitter[E]
}

func (fn *OnlySideIter[E]) ProcessBundle(ctx context.Context, dfc *DFC[[]byte]) error {
	dfc.Process(func(ec ElmC, elm []byte) bool {
		fn.Side.All(ec)(func(elm E) bool {
			fn.Out.Emit(ec, elm)
			return true
		})
		return true
	})
	return nil
}

func TestSideInputIter(t *testing.T) {
	t.Skip("currently insufficiently implemented")
	discard := &DiscardFn[int]{}
	Run(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: 10})
		onlySide := ParDo(s, imp, &OnlySideIter[int]{Side: AsSideIter(src.Output)})
		ParDo(s, onlySide.Out, discard)
		return nil
	})

	if got, want := discard.processed, 10; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
