package beam

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type SourceFn struct {
	Count int

	Output Emitter[int]

	processed int
}

func (fn *SourceFn) StartBundle(ctx context.Context, bc BundC) error {
	fn.processed = 0
	return nil
}

func (fn *SourceFn) ProcessElement(ctx context.Context, ec ElmC, _ []byte) error {
	for i := 0; i < fn.Count; i++ {
		fn.processed++
		fn.Output.Emit(ec, i)
	}
	return nil
}

func (fn *SourceFn) FinishBundle(ctx context.Context, bc BundC) error {
	return nil
}

type IdenFn[E any] struct {
	Output    Emitter[E]
	processed int
}

func (fn *IdenFn[E]) StartBundle(ctx context.Context, bc BundC) error {
	fn.processed = 0
	return nil
}

func (fn *IdenFn[E]) ProcessElement(ctx context.Context, ec ElmC, elm E) error {
	fn.processed++
	fn.Output.Emit(ec, elm)
	return nil
}

func (fn *IdenFn[E]) FinishBundle(ctx context.Context, bc BundC) error {
	return nil
}

type DiscardFn[E any] struct {
	Name string

	processed int
}

func (fn *DiscardFn[E]) StartBundle(ctx context.Context, bc BundC) error {
	// Do some startbundle work.
	fn.processed = 0
	return nil
}

func (fn *DiscardFn[E]) ProcessElement(ctx context.Context, ec ElmC, elm E) error {
	fn.processed++
	return nil
}

func (fn *DiscardFn[E]) FinishBundle(ctx context.Context, bc BundC) error {
	// Do some finish bundle work.
	return nil
}

func TestBuild(t *testing.T) {
	ctx := context.Background()
	p := NewPlan()
	src := ParDo(ctx, p.Root, &SourceFn{Count: 10})
	ParDo(ctx, src[0], &DiscardFn[int]{})
	p.Process(ctx)
}

// BenchmarkPipe benchmarks along the number of DoFns.
//
// goos: linux
// goarch: amd64
// pkg: github.com/lostluck/experimental/altbeams/lifecycle/beam
// cpu: 12th Gen Intel(R) Core(TM) i7-1260P
// BenchmarkPipe/var_dofns_0-16         	73659538	        16.06 ns/op	        16.00 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_1-16         	34601924	        34.52 ns/op	        34.00 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_2-16         	22680674	        52.27 ns/op	        26.00 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_3-16         	16853106	        71.28 ns/op	        23.00 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_5-16         	11161624	       107.4 ns/op	        21.00 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_10-16        	 5691624	       210.5 ns/op	        21.00 ns/elm	       0 B/op	       0 allocs/op
// BenchmarkPipe/var_dofns_100-16       	  549522	      2166 ns/op	        21.00 ns/elm	       0 B/op	       0 allocs/op
func BenchmarkPipe(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{0, 1, 2, 3, 5, 10, 100} {
		n := n
		b.Run(fmt.Sprintf("var_dofns_%d", n), func(b *testing.B) {
			b.ReportAllocs()
			p := NewPlan()
			src := ParDo(ctx, p.Root, &SourceFn{Count: b.N})
			iden := src
			for i := 0; i < n; i++ {
				iden = ParDo(ctx, iden[0], &IdenFn[int]{})
			}
			discard := &DiscardFn[int]{}
			ParDo(ctx, iden[0], discard)
			b.ResetTimer()
			p.Process(ctx)
			if discard.processed != b.N {
				b.Fatalf("processed dodn't match bench number: got %v want %v", discard.processed, b.N)
			}
			d := b.Elapsed()
			div := n
			if div == 0 {
				div = 1
			}
			div = div * b.N
			b.ReportMetric(float64(d/(time.Duration(div))), "ns/elm")
		})
	}
}
