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

func BenchmarkPipe(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{0, 1, 10, 100} {
		n := n
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
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
			b.Logf("%v - %d - %v", n, b.N, d/(time.Duration(div)))
		})
	}
}
