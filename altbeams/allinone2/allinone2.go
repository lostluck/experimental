// package allinone is an experimental mockup of an Apache Beam Go SDK API that
// leverages generics, and a more opinionated construction method. It exists
// to explore the ergonomics and feasibility of such an approach.
//
// Core Ideas:
// * Single Process method, that is passed a special beam context object: DoFnC.
//   - User manage Iteration avoids start and finish bundle.
//   - Isolating certain types to passed in callbacks avoids context errors.
//   - Should also avoid certain issues around sampling, and
//   - Beam context object has specific methods that can accept a function to push iterate over.
package main

import (
	"context"
	"fmt"
	"runtime"

	"github.com/lostluck/experimental/altbeams/allinone2/beam"
)

type MyDoFn struct {
	Name string

	Output beam.Emitter[string]
}

func (fn *MyDoFn) ProcessBundle(ctx context.Context, dfc *beam.DFC[string]) error {
	// Do some startbundle work.
	fmt.Printf("%v started\n", fn.Name)
	processed := 0

	for ec, elm := range dfc.Process {
		processed++
		fmt.Printf("%v \n", fn.Name)
		fn.Output.Emit(ec, elm)
	}

	dfc.FinishBundle(func() error {
		// Do some finish bundle work.
		fmt.Printf("%v finished - %v processsed\n", fn.Name, processed)
		return nil
	})
	return nil
}

type MyIncDoFn struct {
	Name string

	Output beam.Emitter[int]
}

func (fn *MyIncDoFn) ProcessBundle(ctx context.Context, dfc *beam.DFC[int]) error {
	// Do some startbundle work.
	fmt.Printf("%v started\n", fn.Name)
	processed := 0

	for ec, elm := range dfc.Process {
		processed++
		elm += 1
		fmt.Printf("%v %v\n", fn.Name, elm)
		fn.Output.Emit(ec, elm)
	}

	dfc.FinishBundle(func() error {
		// Do some finish bundle work.
		fmt.Printf("%v finished - %v processsed\n", fn.Name, processed)
		return nil
	})
	return nil
}

type SourceFn struct {
	Name  string
	Count int

	Output beam.Emitter[int]
}

func (fn *SourceFn) ProcessBundle(ctx context.Context, dfc *beam.DFC[[]byte]) error {
	// Do some startbundle work.
	fmt.Printf("%v started\n", fn.Name)
	processed := 0

	dfc.Process(func(ec beam.ElmC, _ []byte) bool {
		for i := range fn.Count {
			processed++
			fmt.Printf("%v %v\n", fn.Name, i)
			fn.Output.Emit(ec, i)
		}
		return false
	})

	dfc.FinishBundle(func() error {
		// Do some finish bundle work.
		fmt.Printf("%v finished - %v processsed\n", fn.Name, processed)
		return nil
	})
	return nil
}

type DiscardFn[E any] struct {
	Name string
}

func (fn *DiscardFn[E]) ProcessBundle(ctx context.Context, dfc *beam.DFC[E]) error {
	// Do some startbundle work.
	fmt.Printf("%v started\n", fn.Name)
	processed := 0

	for _, elm := range dfc.Process {
		processed++
		fmt.Printf("%v %v\n", fn.Name, elm)
	}

	dfc.FinishBundle(func() error {
		// Do some finish bundle work.
		fmt.Printf("%v finished - %v processsed\n", fn.Name, processed)
		return nil
	})

	return nil
}

func main() {
	ctx := context.Background()

	// Forward Construction approach.
	imp := beam.Impulse()
	src := beam.ParDo(ctx, imp, &SourceFn{
		Name:  "Source",
		Count: 10,
	})
	inc := beam.ParDo(ctx, src[0], &MyIncDoFn{
		Name: "IncFn",
	})
	beam.ParDo(ctx, inc[0], &DiscardFn[int]{
		Name: "DiscardFn",
	})
	beam.Start(imp)
	fmt.Println("goroutines", runtime.NumGoroutine())
}
