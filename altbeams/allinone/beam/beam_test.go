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

func BenchmarkPipe(b *testing.B) {
	for _, n := range []int{0, 1, 10, 100} {
		n := n
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			b.ReportAllocs()
			var wg sync.WaitGroup
			imp := Start()
			src := RunDoFn(context.Background(), &wg, imp, &SourceFn{Count: b.N})
			iden := src
			for range n {
				iden = RunDoFn(context.Background(), &wg, iden[0], &IdenFn[int]{})
			}
			discard := &DiscardFn[int]{}
			RunDoFn(context.Background(), &wg, iden[0], discard)
			b.ResetTimer()
			wg.Wait()
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

type MyIncDoFn struct {
	Name string

	Output Emitter[int]
}
