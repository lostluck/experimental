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

func TestBuild(t *testing.T) {
	imp := Impulse()
	src := ParDo(context.Background(), imp, &SourceFn{Count: 10})
	ParDo(context.Background(), src[0], &DiscardFn[int]{})

	Start(imp)
}

// BenchmarkPipe benchmarks along the number of DoFns.
//
// goos: linux
// goarch: amd64
// pkg: github.com/lostluck/experimental/altbeams/allinone2/beam
// cpu: 12th Gen Intel(R) Core(TM) i7-1260P
// BenchmarkPipe
// BenchmarkPipe/var_dofns_0
// BenchmarkPipe/var_dofns_0-16            76823004                13.98 ns/op             13.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_1
// BenchmarkPipe/var_dofns_1-16            38088778                30.59 ns/op             30.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_2
// BenchmarkPipe/var_dofns_2-16            24171816                49.44 ns/op             24.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_3
// BenchmarkPipe/var_dofns_3-16            17444563                67.09 ns/op             22.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_5
// BenchmarkPipe/var_dofns_5-16            11590651               103.0 ns/op              20.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_10
// BenchmarkPipe/var_dofns_10-16            6210330               192.8 ns/op              19.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_20
// BenchmarkPipe/var_dofns_20-16            3111206               385.3 ns/op              19.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_30
// BenchmarkPipe/var_dofns_30-16            1935891               619.7 ns/op              20.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_50
// BenchmarkPipe/var_dofns_50-16            1000000              1024 ns/op                20.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_75
// BenchmarkPipe/var_dofns_75-16             710708              1657 ns/op                22.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_100
// BenchmarkPipe/var_dofns_100-16            529915              2275 ns/op                22.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_500
// BenchmarkPipe/var_dofns_500-16            106410             11172 ns/op                22.00 ns/elm           0 B/op          0 allocs/op
func BenchmarkPipe(b *testing.B) {
	makeBench := func(numDoFns int) func(b *testing.B) {
		return func(b *testing.B) {
			b.ReportAllocs()
			ctx := context.Background()
			imp := Impulse()
			src := ParDo(ctx, imp, &SourceFn{Count: b.N})
			iden := src
			for range numDoFns {
				iden = ParDo(ctx, iden[0], &IdenFn[int]{})
			}
			discard := &DiscardFn[int]{}
			ParDo(ctx, iden[0], discard)
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
	for _, numDoFns := range []int{0, 1, 2, 3, 5, 10, 20, 30, 50, 75, 100, 500} {
		b.Run(fmt.Sprintf("var_dofns_%d", numDoFns), makeBench(numDoFns))
	}
}
