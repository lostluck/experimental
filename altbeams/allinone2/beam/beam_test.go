package beam

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type SourceFn struct {
	Count  int
	Output Emitter[int]
}

func (fn *SourceFn) ProcessBundle(ctx context.Context, dfc *DFC[[]byte]) error {
	// Do some startbundle work.
	processed := 0
	dfc.Process(func(ec ElmC, _ []byte) bool {
		for i := range fn.Count {
			processed++
			fn.Output.Emit(ec, i)
		}
		return false
	})
	return nil
}

type DiscardFn[E any] struct {
	processed int
}

func (fn *DiscardFn[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	fn.processed = 0
	for _, _ = range dfc.Process {
		fn.processed++
	}
	return nil
}

type IdenFn[E any] struct {
	Output Emitter[E]
}

func (fn *IdenFn[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	for ec, e := range dfc.Process {
		fn.Output.Emit(ec, e)
	}
	return nil
}

func (fn *DFC[E]) impulse() {
	fn.process(
		elmContext{
			eventTime: time.Now(),
		},
		[]byte{},
	)
}

func TestBuild(t *testing.T) {
	imp := Start()
	src := RunDoFn(context.Background(), imp, &SourceFn{Count: 10})
	RunDoFn(context.Background(), src[0], &DiscardFn[int]{})

	Impulse(imp)
}

// BenchmarkPipe benchmarks along the number of DoFns.
//
// goos: linux
// goarch: amd64
// pkg: github.com/lostluck/experimental/altbeams/allinone2/beam
// cpu: 12th Gen Intel(R) Core(TM) i7-1260P
// BenchmarkPipe
// BenchmarkPipe/var_dofns_0
// BenchmarkPipe/var_dofns_0-16            49666132                24.02 ns/op             24.00 ns/elm           7 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_1
// BenchmarkPipe/var_dofns_1-16            20401291                58.37 ns/op             58.00 ns/elm          15 B/op          1 allocs/op
// BenchmarkPipe/var_dofns_2
// BenchmarkPipe/var_dofns_2-16            13080327                92.52 ns/op             46.00 ns/elm          23 B/op          2 allocs/op
// BenchmarkPipe/var_dofns_3
// BenchmarkPipe/var_dofns_3-16             9421681               124.6 ns/op              41.00 ns/elm          31 B/op          3 allocs/op
// BenchmarkPipe/var_dofns_5
// BenchmarkPipe/var_dofns_5-16             6235714               191.4 ns/op              38.00 ns/elm          47 B/op          5 allocs/op
// BenchmarkPipe/var_dofns_10
// BenchmarkPipe/var_dofns_10-16            3193524               373.2 ns/op              37.00 ns/elm          87 B/op         10 allocs/op
// BenchmarkPipe/var_dofns_20
// BenchmarkPipe/var_dofns_20-16            1646349               731.3 ns/op              36.00 ns/elm         167 B/op         20 allocs/op
// BenchmarkPipe/var_dofns_30
// BenchmarkPipe/var_dofns_30-16            1000000              1107 ns/op                36.00 ns/elm         247 B/op         30 allocs/op
// BenchmarkPipe/var_dofns_50
// BenchmarkPipe/var_dofns_50-16             607266              1868 ns/op                37.00 ns/elm         407 B/op         50 allocs/op
// BenchmarkPipe/var_dofns_75
// BenchmarkPipe/var_dofns_75-16             400274              2995 ns/op                39.00 ns/elm         607 B/op         75 allocs/op
// BenchmarkPipe/var_dofns_100
// BenchmarkPipe/var_dofns_100-16            283448              4033 ns/op                40.00 ns/elm         807 B/op        100 allocs/op
// BenchmarkPipe/var_dofns_500
// BenchmarkPipe/var_dofns_500-16             59900             19905 ns/op                39.00 ns/elm        3990 B/op        498 allocs/op
func BenchmarkPipe(b *testing.B) {
	makeBench := func(numDoFns int) func(b *testing.B) {
		return func(b *testing.B) {
			b.ReportAllocs()
			ctx := context.Background()
			imp := Start()
			src := RunDoFn(ctx, imp, &SourceFn{Count: b.N})
			iden := src
			for range numDoFns {
				iden = RunDoFn(ctx, iden[0], &IdenFn[int]{})
			}
			discard := &DiscardFn[int]{}
			RunDoFn(ctx, iden[0], discard)
			b.ResetTimer()

			imp.impulse()
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
	for _, numDoFns := range []int{0, 1, 2, 3, 5, 10, 20, 30, 50, 75, 100, 500} {
		b.Run(fmt.Sprintf("var_dofns_%d", numDoFns), makeBench(numDoFns))
	}
}
