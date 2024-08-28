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

	"github.com/lostluck/experimental/altbeams/allinone2/beam"
)

type MyDoFn struct {
	Name string

	Output beam.Output[string]
	beam.OnBundleFinish
}

func (fn *MyDoFn) ProcessBundle(dfc *beam.DFC[string]) error {
	// Do some startbundle work.
	fmt.Printf("%v started\n", fn.Name)
	processed := 0

	dfc.Process(func(ec beam.ElmC, elm string) error {
		processed++
		fmt.Printf("%v \n", fn.Name)
		fn.Output.Emit(ec, elm)
		return nil
	})

	fn.OnBundleFinish.Do(dfc, func() error {
		// Do some finish bundle work.
		fmt.Printf("%v finished - %v processsed\n", fn.Name, processed)
		return nil
	})
	return nil
}

type MyIncDoFn struct {
	Name string

	Output beam.Output[int]
	beam.OnBundleFinish
}

func (fn *MyIncDoFn) ProcessBundle(dfc *beam.DFC[int]) error {
	// Do some startbundle work.
	logger := dfc.Logger().With("name", fn.Name)
	logger.Info("MyIncDoFn started")
	processed := 0

	dfc.Process(func(ec beam.ElmC, elm int) error {
		processed++
		elm += 1
		logger.Info("MyIncDoFn finished", "elm", elm)
		fn.Output.Emit(ec, elm)
		return nil
	})

	fn.OnBundleFinish.Do(dfc, func() error {
		// Do some finish bundle work.
		logger.Info("MyIncDoFn finished", "processed", processed)
		return nil
	})
	return nil
}

type SourceFn struct {
	Name  string
	Count int

	Output beam.Output[int]
	beam.OnBundleFinish
}

func (fn *SourceFn) ProcessBundle(dfc *beam.DFC[[]byte]) error {
	// Do some startbundle work.
	logger := dfc.Logger().With("name", fn.Name)
	logger.Info("SourceFn started")
	processed := 0

	dfc.Process(func(ec beam.ElmC, _ []byte) error {
		for i := 0; i < fn.Count; i++ {
			processed++
			logger.Info("emitting", "elm", i)
			fn.Output.Emit(ec, i)
		}
		return nil
	})

	fn.OnBundleFinish.Do(dfc, func() error {
		// Do some finish bundle work.
		logger.Info("SourceFn finished", "processed", processed)
		return nil
	})
	return nil
}

type DiscardFn[E any] struct {
	Name string
	beam.OnBundleFinish

	Processed beam.Counter
}

func (fn *DiscardFn[E]) ProcessBundle(dfc *beam.DFC[E]) error {
	// Do some startbundle work.
	logger := dfc.Logger().With("name", fn.Name)
	logger.Info("DiscardFn started")

	dfc.Process(func(ec beam.ElmC, elm E) error {
		fn.Processed.Inc(dfc, 1)
		logger.Info("element received", "elm", elm)
		return nil
	})

	fn.OnBundleFinish.Do(dfc, func() error {
		// Do some finish bundle work.
		logger.Info("DiscardFn finished", "name", fn.Name)
		return nil
	})

	return nil
}

func main() {
	ctx := context.Background()

	pr, err := beam.Run(ctx, func(s *beam.Scope) error {
		imp := beam.Impulse(s)
		src := beam.ParDo(s, imp, &SourceFn{
			Name:  "Source",
			Count: 10,
		})
		inc := beam.ParDo(s, src.Output, &MyIncDoFn{
			Name: "IncFn",
		})
		beam.ParDo(s, inc.Output, &DiscardFn[int]{
			Name: "DiscardFn",
		}, beam.Name("sink"))
		return nil
	}, beam.Name("testjob"))

	if err != nil {
		fmt.Println("error:", err)
	} else {
		fmt.Println("results:", pr)
	}
}
