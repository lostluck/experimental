package beam

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

type SourceFn struct {
	Count  int
	Output Emitter[int]
}

func (fn *SourceFn) ProcessBundle(ctx context.Context, dfc DFC[[]byte]) error {
	// Do some startbundle work.
	processed := 0
	dfc.Process(func(ec ElmC, _ []byte) bool {
		for i := 0; i < fn.Count; i++ {
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
	dfc.Process(func(ec ElmC, elm E) bool {
		fn.processed++
		return true
	})
	return nil
}

type IdenFn[E any] struct {
	Output Emitter[E]
}

func (fn *IdenFn[E]) ProcessBundle(ctx context.Context, dfc DFC[E]) error {
	dfc.Process(func(ec ElmC, elm E) bool {
		fn.Output.Emit(ec, elm)
		return true
	})
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
// BenchmarkPipe/fixed_dofns_10_var_buf_0-16         	  383590	      3304 ns/op	       330.4 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/fixed_dofns_10_var_buf_1-16         	  512403	      2311 ns/op	       231.1 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/fixed_dofns_10_var_buf_10-16        	 1000000	      1214 ns/op	       121.4 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/fixed_dofns_10_var_buf_100-16       	 2955867	       412.5 ns/op	        41.25 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/fixed_dofns_10_var_buf_200-16       	 3199198	       375.2 ns/op	        37.52 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/fixed_dofns_10_var_buf_300-16       	 3444544	       350.2 ns/op	        35.02 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/fixed_dofns_10_var_buf_500-16       	 3316599	       360.2 ns/op	        36.02 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/fixed_dofns_10_var_buf_750-16       	 3445196	       531.3 ns/op	        53.13 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/fixed_dofns_10_var_buf_1000-16      	 2990779	       366.3 ns/op	        36.62 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/fixed_dofns_10_var_buf_1500-16      	 2992812	       404.7 ns/op	        40.47 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/fixed_dofns_10_var_buf_10000-16     	 1596625	       730.9 ns/op	        73.09 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/fixed_dofns_10_var_buf_100000-16    	 2989074	       400.2 ns/op	        40.02 ns/elm	       0 B/op	       0 allocs/op
//
// BenchmarkPipe/var_dofns_0_fixed_buf_100-16        	16465645	        72.12 ns/op	        72.12 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_1_fixed_buf_100-16        	 7327598	       157.9 ns/op	       157.9 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_2_fixed_buf_100-16        	 4987459	       243.0 ns/op	       121.5 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_3_fixed_buf_100-16        	 3987898	       290.4 ns/op	        96.79 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_5_fixed_buf_100-16        	 3513968	       339.9 ns/op	        67.99 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_10_fixed_buf_100-16       	 2932188	       410.9 ns/op	        41.09 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_100_fixed_buf_100-16      	 1408825	       854.0 ns/op	         8.540 ns/elm	       0 B/op	       0 allocs/op
func BenchmarkPipe(b *testing.B) {
	makeBench := func(numDoFns int, bufSize uint32) func(b *testing.B) {
		return func(b *testing.B) {
			numBuf.Store(bufSize)
			ctx := context.Background()
			var wg sync.WaitGroup
			imp := Start()
			src := RunDoFn(ctx, &wg, imp, &SourceFn{Count: b.N})
			iden := src
			for i := 0; i < numDoFns; i++ {
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
			b.ReportMetric(float64(d)/float64(div), "ns/elm")
		}
	}

	numDoFns := 10
	for _, bufSize := range []uint32{0, 1, 10, 100, 200, 300, 500, 750, 1000, 1500, 10000, 100000} {
		b.Run(fmt.Sprintf("fixed_dofns_%d_var_buf_%d", numDoFns, bufSize), makeBench(numDoFns, bufSize))
	}
	const bufSize = 100
	for _, numDoFns := range []int{0, 1, 2, 3, 5, 10, 100} {
		b.Run(fmt.Sprintf("var_dofns_%d_fixed_buf_%d", numDoFns, bufSize), makeBench(numDoFns, bufSize))
	}
}
