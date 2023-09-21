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
	for i := range fn.Count {
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
// BenchmarkPipe
// BenchmarkPipe/var_dofns_0
// BenchmarkPipe/var_dofns_0-16            76313715                15.21 ns/op             15.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_1
// BenchmarkPipe/var_dofns_1-16            34851698                34.35 ns/op             34.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_2
// BenchmarkPipe/var_dofns_2-16            21951789                54.18 ns/op             27.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_3
// BenchmarkPipe/var_dofns_3-16            16154935                73.84 ns/op             24.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_5
// BenchmarkPipe/var_dofns_5-16            10462424               114.0 ns/op              22.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_10
// BenchmarkPipe/var_dofns_10-16            5641654               212.0 ns/op              21.00 ns/elm           0 B/op          0 allocs/op
// BenchmarkPipe/var_dofns_100
// BenchmarkPipe/var_dofns_100-16            466226              2460 ns/op                24.00 ns/elm           0 B/op          0 allocs/op
func BenchmarkPipe(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{0, 1, 2, 3, 5, 10, 100} {
		n := n
		b.Run(fmt.Sprintf("var_dofns_%d", n), func(b *testing.B) {
			b.ReportAllocs()
			p := NewPlan()
			src := ParDo(ctx, p.Root, &SourceFn{Count: b.N})
			iden := src
			for range n {
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
