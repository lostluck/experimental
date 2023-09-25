package main

import (
	"context"
	"fmt"
	"runtime"

	"github.com/lostluck/experimental/altbeams/lifecycle/beam"
)

type MyDoFn struct {
	Name string

	Output beam.Emitter[string]

	processed int
}

func (fn *MyDoFn) StartBundle(ctx context.Context, bc beam.BundC) error {
	// Do some startbundle work.
	fmt.Printf("%v started\n", fn.Name)
	fn.processed = 0
	return nil
}

func (fn *MyDoFn) ProcessElement(ctx context.Context, ec beam.ElmC, elm string) error {
	fn.processed++
	fmt.Printf("%v %v\n", fn.Name, elm)
	fn.Output.Emit(ec, elm)
	return nil
}

func (fn *MyDoFn) FinishBundle(ctx context.Context, bc beam.BundC) error {
	// Do some finish bundle work.
	fmt.Printf("%v finished - %v processsed\n", fn.Name, fn.processed)
	return nil
}

type SourceFn struct {
	Name  string
	Count int

	Output beam.Emitter[int]

	processed int
}

func (fn *SourceFn) StartBundle(ctx context.Context, bc beam.BundC) error {
	// Do some startbundle work.
	fmt.Printf("%v started\n", fn.Name)
	fn.processed = 0
	return nil
}

func (fn *SourceFn) ProcessElement(ctx context.Context, ec beam.ElmC, _ []byte) error {
	for i := 0; i < fn.Count; i++ {
		fn.processed++
		fmt.Printf("%v %v\n", fn.Name, i)
		fn.Output.Emit(ec, i)
	}
	return nil
}

func (fn *SourceFn) FinishBundle(ctx context.Context, bc beam.BundC) error {
	// Do some finish bundle work.
	fmt.Printf("%v finished - %v processsed\n", fn.Name, fn.processed)
	return nil
}

type MyIncDoFn struct {
	Name string

	Output beam.Emitter[int]

	processed int
}

func (fn *MyIncDoFn) StartBundle(ctx context.Context, bc beam.BundC) error {
	// Do some startbundle work.
	fmt.Printf("%v started\n", fn.Name)
	fn.processed = 0
	return nil
}

func (fn *MyIncDoFn) ProcessElement(ctx context.Context, ec beam.ElmC, elm int) error {
	fn.processed++
	elm += 1
	fmt.Printf("%v %v\n", fn.Name, elm)
	fn.Output.Emit(ec, elm)
	return nil
}

func (fn *MyIncDoFn) FinishBundle(ctx context.Context, bc beam.BundC) error {
	// Do some finish bundle work.
	fmt.Printf("%v finished - %v processsed\n", fn.Name, fn.processed)
	return nil
}

type DiscardFn[E any] struct {
	Name string

	processed int
}

func (fn *DiscardFn[E]) StartBundle(ctx context.Context, bc beam.BundC) error {
	// Do some startbundle work.
	fmt.Printf("%v started\n", fn.Name)
	fn.processed = 0
	return nil
}

func (fn *DiscardFn[E]) ProcessElement(ctx context.Context, ec beam.ElmC, elm E) error {
	fn.processed++
	fmt.Printf("%v %v\n", fn.Name, elm)
	return nil
}

func (fn *DiscardFn[E]) FinishBundle(ctx context.Context, bc beam.BundC) error {
	// Do some finish bundle work.
	fmt.Printf("%v finished - %v processsed\n", fn.Name, fn.processed)
	return nil
}

func main() {
	ctx := context.Background()

	// Forward Construction approach.

	p := beam.NewPlan()
	src := beam.ParDo(ctx, p.Root, &SourceFn{
		Name:  "Source",
		Count: 10,
	})
	inc := beam.ParDo(ctx, src[0], &MyIncDoFn{
		Name: "IncFn",
	})
	beam.ParDo(ctx, inc[0], &DiscardFn[int]{
		Name: "DiscardFn",
	})

	p.Process(ctx)

	fmt.Println("goroutines", runtime.NumGoroutine())
}
