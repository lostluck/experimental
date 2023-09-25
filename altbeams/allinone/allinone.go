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
//
// * Build off of the State and Timer "provider" widgets, expanding them to side inputs, and emitters.
//
// This package in particular is for demonstrating how user code would look, while the "beam" sub package
// here is for seeing how it namespaces correctly.
//
// The Goal in this exploration is to see how complicated data injestion and extraction would be around
// this approach, and whether we can simplify the execution architecture, and how well Go can optimize
// this flow.
//
// While certainly heavy weight,
package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/lostluck/experimental/altbeams/allinone/beam"
)

type MyDoFn struct {
	Name string

	Output beam.Emitter[string]
}

func (fn *MyDoFn) ProcessBundle(ctx context.Context, dfc beam.DFC[string]) error {
	// Do some startbundle work.
	fmt.Printf("%v started\n", fn.Name)
	processed := 0

	for ec, elm := range dfc.Process {
		processed++
		fmt.Printf("%v \n", fn.Name)
		fn.Output.Emit(ec, elm)
	}

	// Do some finish bundle work.
	fmt.Printf("%v finished - %v processsed\n", fn.Name, processed)
	return nil
}

type MyIncDoFn struct {
	Name string

	Output beam.Emitter[int]
}

func (fn *MyIncDoFn) ProcessBundle(ctx context.Context, dfc beam.DFC[int]) error {
	// Do some startbundle work.
	fmt.Printf("%v started\n", fn.Name)
	processed := 0

	for ec, elm := range dfc.Process {
		processed++
		elm += 1
		fmt.Printf("%v %v\n", fn.Name, elm)
		fn.Output.Emit(ec, elm)
	}

	// Do some finish bundle work.
	fmt.Printf("%v finished - %v processsed\n", fn.Name, processed)
	return nil
}

type SourceFn struct {
	Name  string
	Count int

	Output beam.Emitter[int]
}

func (fn *SourceFn) ProcessBundle(ctx context.Context, dfc beam.DFC[[]byte]) error {
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

	// Do some finish bundle work.
	fmt.Printf("%v finished - %v processsed\n", fn.Name, processed)
	return nil
}

type DiscardFn[E any] struct {
	Name string
}

func (fn *DiscardFn[E]) ProcessBundle(ctx context.Context, dfc beam.DFC[E]) error {
	// Do some startbundle work.
	fmt.Printf("%v started\n", fn.Name)
	processed := 0

	for _, elm := range dfc.Process {
		processed++
		fmt.Printf("%v %v\n", fn.Name, elm)
	}

	// Do some finish bundle work.
	fmt.Printf("%v finished - %v processsed\n", fn.Name, processed)
	return nil
}

func main() {
	ctx := context.Background()
	var wg sync.WaitGroup

	// Forward Construction approach.
	imp := beam.Start()
	src := beam.RunDoFn(ctx, &wg, imp, &SourceFn{
		Name:  "Source",
		Count: 10,
	})
	inc := beam.RunDoFn(ctx, &wg, src[0], &MyIncDoFn{
		Name: "IncFn",
	})
	beam.RunDoFn(ctx, &wg, inc[0], &DiscardFn[int]{
		Name: "DiscardFn",
	})

	wg.Wait()
	fmt.Println("goroutines", runtime.NumGoroutine())
}
