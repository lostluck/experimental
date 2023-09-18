package beam

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type SourceFn struct {
	Count  int
	Output Emitter[int]
}

func (fn *SourceFn) ProcessBundle(ctx context.Context, dfc DFC[[]byte]) error {
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

func (fn *DiscardFn[E]) ProcessBundle(ctx context.Context, dfc DFC[E]) error {
	fn.processed = 0
	for _, _ = range dfc.Process {
		fn.processed++
	}
	return nil
}

type IdenFn[E any] struct {
	Output Emitter[E]
}

func (fn *IdenFn[E]) ProcessBundle(ctx context.Context, dfc DFC[E]) error {
	for ec, e := range dfc.Process {
		fn.Output.Emit(ec, e)
	}
	return nil
}

func TestBuild(t *testing.T) {
	var wg sync.WaitGroup
	imp := Start()
	src := RunDoFn(context.Background(), &wg, imp, &SourceFn{Count: 10})
	RunDoFn(context.Background(), &wg, src[0], &DiscardFn[int]{})
	wg.Wait()
}

// BenchmarkPipe benchmarks along the number of DoFns and the Channel buffer sizes.
//
// goos: linux
// goarch: amd64
// pkg: github.com/lostluck/experimental/altbeams/allinone/beam
// cpu: 12th Gen Intel(R) Core(TM) i7-1260P
// BenchmarkPipe
// BenchmarkPipe/fixed_dofns_10_var_buf_0
// BenchmarkPipe/fixed_dofns_10_var_buf_0-16                 194977              5923 ns/op               592.0 ns/elm
// BenchmarkPipe/fixed_dofns_10_var_buf_1
// BenchmarkPipe/fixed_dofns_10_var_buf_1-16                 417501              2636 ns/op               263.0 ns/elm
// BenchmarkPipe/fixed_dofns_10_var_buf_10
// BenchmarkPipe/fixed_dofns_10_var_buf_10-16                973578              1373 ns/op               137.0 ns/elm
// BenchmarkPipe/fixed_dofns_10_var_buf_100
// BenchmarkPipe/fixed_dofns_10_var_buf_100-16              2669761               451.3 ns/op              45.00 ns/elm <--- good enough.
// BenchmarkPipe/fixed_dofns_10_var_buf_200
// BenchmarkPipe/fixed_dofns_10_var_buf_200-16              2817710               427.1 ns/op              42.00 ns/elm
// BenchmarkPipe/fixed_dofns_10_var_buf_300
// BenchmarkPipe/fixed_dofns_10_var_buf_300-16              2821352               429.5 ns/op              42.00 ns/elm
// BenchmarkPipe/fixed_dofns_10_var_buf_500
// BenchmarkPipe/fixed_dofns_10_var_buf_500-16              2829500               422.6 ns/op              42.00 ns/elm
// BenchmarkPipe/fixed_dofns_10_var_buf_750
// BenchmarkPipe/fixed_dofns_10_var_buf_750-16              2688441               447.1 ns/op              44.00 ns/elm
// BenchmarkPipe/fixed_dofns_10_var_buf_1000
// BenchmarkPipe/fixed_dofns_10_var_buf_1000-16             2782318               549.9 ns/op              54.00 ns/elm
// BenchmarkPipe/fixed_dofns_10_var_buf_1500
// BenchmarkPipe/fixed_dofns_10_var_buf_1500-16             1519592               827.2 ns/op              82.00 ns/elm
// BenchmarkPipe/fixed_dofns_10_var_buf_10000
// BenchmarkPipe/fixed_dofns_10_var_buf_10000-16            3204487               533.8 ns/op              53.00 ns/elm
// BenchmarkPipe/fixed_dofns_10_var_buf_100000
// BenchmarkPipe/fixed_dofns_10_var_buf_100000-16           2998924               620.3 ns/op              62.00 ns/elm
//
// BenchmarkPipe/var_dofns_0_fixed_buf_100
// BenchmarkPipe/var_dofns_0_fixed_buf_100-16              13641354               108.9 ns/op             108.0 ns/elm
// BenchmarkPipe/var_dofns_1_fixed_buf_100
// BenchmarkPipe/var_dofns_1_fixed_buf_100-16               6883555               171.9 ns/op             171.0 ns/elm
// BenchmarkPipe/var_dofns_2_fixed_buf_100
// BenchmarkPipe/var_dofns_2_fixed_buf_100-16               4720983               250.5 ns/op             125.0 ns/elm
// BenchmarkPipe/var_dofns_3_fixed_buf_100
// BenchmarkPipe/var_dofns_3_fixed_buf_100-16               4116758               293.7 ns/op              97.00 ns/elm
// BenchmarkPipe/var_dofns_5_fixed_buf_100
// BenchmarkPipe/var_dofns_5_fixed_buf_100-16               3227955               369.1 ns/op              73.00 ns/elm
// BenchmarkPipe/var_dofns_10_fixed_buf_100
// BenchmarkPipe/var_dofns_10_fixed_buf_100-16              2650719               453.6 ns/op              45.00 ns/elm
// BenchmarkPipe/var_dofns_100_fixed_buf_100
// BenchmarkPipe/var_dofns_100_fixed_buf_100-16              843068              1463 ns/op                14.00 ns/elm
func BenchmarkPipe(b *testing.B) {

	makeBench := func(numDoFns int, bufSize uint32) func(b *testing.B) {
		return func(b *testing.B) {
			numBuf.Store(bufSize)
			ctx := context.Background()
			var wg sync.WaitGroup
			imp := Start()
			src := RunDoFn(ctx, &wg, imp, &SourceFn{Count: b.N})
			iden := src
			for range numDoFns {
				iden = RunDoFn(ctx, &wg, iden[0], &IdenFn[int]{})
			}
			discard := &DiscardFn[int]{}
			RunDoFn(ctx, &wg, iden[0], discard)
			b.ResetTimer()
			wg.Wait()
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

	numDoFns := 10
	for _, bufSize := range []uint32{0, 1, 10, 100, 200, 300, 500, 750, 1000, 1500, 10000, 100000} {
		b.Run(fmt.Sprintf("fixed_dofns_%d_var_buf_%d", numDoFns, bufSize), makeBench(numDoFns, bufSize))
	}
	const bufSize = 100
	for _, numDoFns := range []int{0, 1, 2, 3, 5, 10, 20, 30, 50, 75, 100, 500} {
		b.Run(fmt.Sprintf("var_dofns_%d_fixed_buf_%d", numDoFns, bufSize), makeBench(numDoFns, bufSize))
	}
}
