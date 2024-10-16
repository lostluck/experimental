package beam

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"
	"golang.org/x/exp/constraints"
)

// TODO sort out running tests in non-loopback mode.

type SourceFn struct {
	Count  int
	Output PCol[int]
}

func (fn *SourceFn) ProcessBundle(dfc *DFC[[]byte]) error {
	// Do some startbundle work.
	processed := 0
	dfc.Process(func(ec ElmC, _ []byte) error {
		for i := 0; i < fn.Count; i++ {
			processed++
			fn.Output.Emit(ec, i)
		}
		return nil
	})
	return nil
}

type DiscardFn[E Element] struct {
	OnBundleFinish

	Processed, Finished CounterInt64
}

func (fn *DiscardFn[E]) ProcessBundle(dfc *DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) error {
		fn.Processed.Inc(dfc, 1)
		return nil
	})
	fn.OnBundleFinish.Do(dfc, func() error {
		fn.Finished.Inc(dfc, 1)
		return nil
	})
	return nil
}

type IdenFn[E Element] struct {
	Output PCol[E]

	BundleStarts CounterInt64
}

func (fn *IdenFn[E]) ProcessBundle(dfc *DFC[E]) error {
	fn.BundleStarts.Inc(dfc, 1)
	dfc.Process(func(ec ElmC, elm E) error {
		fn.Output.Emit(ec, elm)
		return nil
	})
	return nil
}

func pipeName(tb testing.TB) beamopts.Options {
	return Name(tb.Name())
}

func TestSimple(t *testing.T) {
	_, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: 10})
		ParDo(s, src.Output, &DiscardFn[int]{})
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
}

func TestAutomaticDiscard(t *testing.T) {
	_, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		ParDo(s, imp, &SourceFn{Count: 10})
		// drop the output.
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
}

func TestSimpleNamed(t *testing.T) {
	pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: 10})
		ParDo(s, src.Output, &DiscardFn[int]{}, Name("pants"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	t.Log(pr.Counters)
	if got, want := int(pr.Counters["pants.Processed"]), 10; got != want {
		t.Fatalf("processed didn't match bench number: got %v want %v", got, want)
	}
}

// BenchmarkPipe benchmarks along the number of DoFns.
//
// goos: linux
// goarch: amd64
// pkg: github.com/lostluck/experimental/altbeams/allinone2/beam
// cpu: 12th Gen Intel(R) Core(TM) i7-1260P
// BenchmarkPipe/var_dofns_0-16         	70822042	        16.65 ns/op	 480.54 MB/s	        16.65 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_1-16         	35603048	        33.70 ns/op	 474.83 MB/s	        33.70 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_2-16         	24342855	        48.95 ns/op	 490.25 MB/s	        24.48 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_3-16         	17800094	        66.13 ns/op	 483.91 MB/s	        22.04 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_5-16         	12088483	        99.11 ns/op	 484.32 MB/s	        19.82 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_10-16        	 6605112	       181.5 ns/op	 484.75 MB/s	        18.15 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_100-16       	  582006	      2030 ns/op	 398.00 MB/s	        20.30 ns/elm	       0 B/op	       0 allocs/op
func BenchmarkPipe(b *testing.B) {
	makeBench := func(numDoFns int) func(b *testing.B) {
		return func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(8 * int64(numDoFns+1))

			pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
				imp := Impulse(s)
				src := ParDo(s, imp, &SourceFn{Count: b.N})
				iden := src.Output
				for i := 0; i < numDoFns; i++ {
					iden = ParDo(s, iden, &IdenFn[int]{}).Output
				}
				ParDo(s, iden, &DiscardFn[int]{}, Name("sink"))
				return nil
			}, pipeName(b))
			if err != nil {
				b.Errorf("Run error: %v", err)
			}
			if got, want := int(pr.Counters["sink.Processed"]), b.N; got != want {
				b.Fatalf("processed didn't match bench number: got %v want %v", got, want)
			}
			if got, want := int(pr.Counters["sink.Finished"]), 1; got != want {
				b.Fatalf("finished didn't match bundle counter: got %v want %v", got, want)
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
	for _, numDoFns := range []int{0, 0, 1, 2, 3, 5, 10, 100} {
		b.Run(fmt.Sprintf("dofns=%d", numDoFns), makeBench(numDoFns))
	}
}

type ModPartition[V constraints.Integer] struct {
	Outputs []PCol[V] // The count needs to be properly serialized, ultimately.
}

func (fn *ModPartition[V]) ProcessBundle(dfc *DFC[V]) error {
	mod := V(len(fn.Outputs))
	dfc.Process(func(ec ElmC, elm V) error {
		rem := elm % mod
		fn.Outputs[rem].Emit(ec, elm)
		return nil
	})
	return nil
}

type WideNarrow struct {
	Wide int

	In PCol[int]
}

var _ Composite[struct{ Out PCol[int] }] = ((*WideNarrow)(nil))

func (src *WideNarrow) Expand(s *Scope) (out struct{ Out PCol[int] }) {
	partition := ParDo(s, src.In, &ModPartition[int]{Outputs: make([]PCol[int], src.Wide)})
	out.Out = Flatten(s, partition.Outputs...)
	return out
}

func TestPartitionFlatten(t *testing.T) {
	count, mod := 10, 2
	pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: count})
		exp := Expand(s, "WideNarrow", &WideNarrow{Wide: mod, In: src.Output})
		ParDo(s, exp.Out, &DiscardFn[int]{}, Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink.Processed"]), count; got != want {
		t.Fatalf("processed didn't match bench number: got %v want %v", got, want)
	}
	if got, want := int(pr.Counters["sink.Finished"]), 1; got != want {
		t.Fatalf("finished didn't match bundle countr: got %v want %v", got, want)
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

			pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
				imp := Impulse(s)
				src := ParDo(s, imp, &SourceFn{Count: b.N})
				exp := Expand(s, "WideNarrow", &WideNarrow{Wide: numPartitions, In: src.Output})
				ParDo(s, exp.Out, &DiscardFn[int]{}, Name("sink"))
				return nil
			}, pipeName(b))
			if err != nil {
				b.Error(err)
			}
			if got, want := int(pr.Counters["sink.Processed"]), b.N; got != want {
				b.Fatalf("processed didn't match bench number: got %v want %v", got, want)
			}
		}
	}
	for _, numDoFns := range []int{1, 2, 3, 5, 10, 100} {
		b.Run(fmt.Sprintf("num_partitions=%d", numDoFns), makeBench(numDoFns))
	}
}

type KeyMod[V constraints.Integer] struct {
	Mod V

	Output PCol[KV[V, V]]
}

func (fn *KeyMod[V]) ProcessBundle(dfc *DFC[V]) error {
	dfc.Process(func(ec ElmC, elm V) error {
		mod := elm % fn.Mod
		fn.Output.Emit(ec, KV[V, V]{
			Key:   V(mod),
			Value: elm,
		})
		return nil
	})
	return nil
}

type SumByKey[K Keys, V constraints.Integer | constraints.Float] struct {
	Output PCol[KV[K, V]]
}

func (fn *SumByKey[K, V]) ProcessBundle(dfc *DFC[KV[K, Iter[V]]]) error {
	dfc.Process(func(ec ElmC, elm KV[K, Iter[V]]) error {
		var sum V
		elm.Value.All()(func(elm V) bool {
			sum += elm
			return true
		})
		fn.Output.Emit(ec, KV[K, V]{Key: elm.Key, Value: sum})
		return nil
	})
	return nil
}

type GroupKeyModSum[V constraints.Integer] struct {
	Mod V

	Output PCol[KV[V, V]]

	OnBundleFinish
}

var (
	MaxET time.Time = time.UnixMilli(math.MaxInt64 / 1000)
	EOGW            = MaxET.Add(-time.Hour * 24)
)

func (fn *GroupKeyModSum[V]) ProcessBundle(dfc *DFC[V]) error {
	grouped := map[V]V{}
	dfc.Process(func(ec ElmC, elm V) error {
		mod := elm % fn.Mod
		v := grouped[mod]
		v += elm
		grouped[mod] = v
		return nil
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
	mod := 3
	pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: 10})
		keyed := ParDo(s, src.Output, &KeyMod[int]{Mod: mod})
		grouped := GBK[int, int](s, keyed.Output)
		sums := ParDo(s, grouped, &SumByKey[int, int]{})
		ParDo(s, sums.Output, &DiscardFn[KV[int, int]]{}, Name("sink"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink.Processed"]), mod; got != want {
		t.Fatalf("processed didn't match bench number: got %v want %v", got, want)
	}
}

func BenchmarkGBKSum_int(b *testing.B) {
	for _, mod := range []int{2, 3, 5, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("mod_%v", mod), func(b *testing.B) {
			discard := &DiscardFn[KV[int, int]]{}
			pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
				imp := Impulse(s)
				src := ParDo(s, imp, &SourceFn{Count: b.N})
				keyed := ParDo(s, src.Output, &KeyMod[int]{Mod: mod})
				grouped := GBK[int, int](s, keyed.Output)
				sums := ParDo(s, grouped, &SumByKey[int, int]{})
				ParDo(s, sums.Output, discard, Name("sink"))
				return nil
			}, pipeName(b))
			if err != nil {
				b.Error(err)
			}
			want := mod
			if b.N < mod {
				want = b.N
			}
			if got, want := int(pr.Counters["sink.Processed"]), want; got != want {
				b.Fatalf("processed didn't match bench number: got %v want %v", got, want)
			}
		})
	}
}

func BenchmarkGBKSum_Lifted_int(b *testing.B) {
	for _, mod := range []int{2, 3, 5, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("mod_%v", mod), func(b *testing.B) {
			pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
				imp := Impulse(s)
				src := ParDo(s, imp, &SourceFn{Count: b.N})
				keyed := ParDo(s, src.Output, &GroupKeyModSum[int]{Mod: mod})
				ParDo(s, keyed.Output, &DiscardFn[KV[int, int]]{}, Name("sink"))
				return nil
			}, pipeName(b))
			if err != nil {
				b.Error(err)
			}
			want := mod
			if b.N < mod {
				want = b.N
			}
			if got, want := int(pr.Counters["sink.Processed"]), want; got != want {
				b.Fatalf("processed didn't match bench number: got %v want %v", got, want)
			}
		})
	}
}

func TestTwoSubGraphs(t *testing.T) {
	count := 10
	pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
		imp1, imp2 := Impulse(s), Impulse(s)
		src1, src2 := ParDo(s, imp1, &SourceFn{Count: count + 1}), ParDo(s, imp2, &SourceFn{Count: count + 2})
		ParDo(s, src1.Output, &DiscardFn[int]{}, Name("sink1"))
		ParDo(s, src2.Output, &DiscardFn[int]{}, Name("sink2"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink1.Processed"]), count+1; got != want {
		t.Errorf("discard1 got %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink2.Processed"]), count+2; got != want {
		t.Errorf("discard2 got %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink1.Finished"]), 1; got != want {
		t.Fatalf("finished1 didn't match bundle counter: got %v want %v", got, want)
	}
	if got, want := int(pr.Counters["sink2.Finished"]), 1; got != want {
		t.Fatalf("finished2 didn't match bundle counter: got %v want %v", got, want)
	}
}

func TestMultiplexImpulse(t *testing.T) {
	count := 10
	pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
		imp := Impulse(s) // As a Runner transform, impulses don't multiplex.
		src1, src2 := ParDo(s, imp, &SourceFn{Count: count + 1}), ParDo(s, imp, &SourceFn{Count: count + 2})
		ParDo(s, src1.Output, &DiscardFn[int]{}, Name("sink1"))
		ParDo(s, src2.Output, &DiscardFn[int]{}, Name("sink2"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink1.Processed"]), count+1; got != want {
		t.Errorf("discard1 got %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink2.Processed"]), count+2; got != want {
		t.Errorf("discard2 got %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink1.Finished"]), 1; got != want {
		t.Fatalf("finished1 didn't match bundle counter: got %v want %v", got, want)
	}
	if got, want := int(pr.Counters["sink2.Finished"]), 1; got != want {
		t.Fatalf("finished2 didn't match bundle counter: got %v want %v", got, want)
	}
}

func TestMultiplex(t *testing.T) {
	count := 10
	pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
		imp := Impulse(s)
		src := ParDo(s, imp, &SourceFn{Count: count})
		ParDo(s, src.Output, &DiscardFn[int]{}, Name("sink1"))
		ParDo(s, src.Output, &DiscardFn[int]{}, Name("sink2"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["sink1.Processed"]), count; got != want {
		t.Errorf("discard1 got %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink2.Processed"]), count; got != want {
		t.Errorf("discard2 got %v, want %v", got, want)
	}
	if got, want := int(pr.Counters["sink1.Finished"]), 1; got != want {
		t.Fatalf("finished1 didn't match bundle counter: got %v want %v", got, want)
	}
	if got, want := int(pr.Counters["sink2.Finished"]), 1; got != want {
		t.Fatalf("finished2 didn't match bundle counter: got %v want %v", got, want)
	}
}
