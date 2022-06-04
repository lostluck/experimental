package main

import (
	"context"
	"fmt"
	"reflect"
)

type EventTime int

// What if DoFns had a fixed arity in parameters, but had convenience structs that unambiguously
// defined the inputs and outputs?
// We still need all the various side types, and awareness of if the DoFn observes windows and such.
// This allows them to happen via reflection as they do currently, but avoids us needing to have such complex execution harnesses.
// But we avoid the extremely large parameter lists as a result, which is nice.

// While not demonstrated here, Structural DoFns would still exist for Lifecycle method handling
// But since side inputs and emits are Tag based, and type based, they are easier to include/exclude if not needed
// Due to the positional tagging.

// The tagged approach also could better enable registering ViewFns and similar, since the tags are fully user specified.

// Complex DoFn example
func Do(
	ctx context.Context,
	elem struct { // Element as normal here. Could leverage BeamSchemas through field tags, and allow input and passthrough of unused fields.
		K string
		V int
	},
	meta struct {
		Et EventTime
		// Windows, BundleFinalizers, RTrackers such types go here.
		// Possibly State and Timers when they're appropriate parameters,
	}, inputs struct {
		Foo func(*int) bool    `beamtag:"tag1"`
		Bar func(*string) bool `beamtag:"tag2"`
	}, outputs struct {
		Emit1 func(k string, v int) `beamtag:"tag1"`
		Emit2 func(k string, v int) `beamtag:"tag2"`
	}) error {

	var i int
	inputs.Foo(&i)
	var s string
	inputs.Bar(&s)

	fmt.Println("Do called with:", meta, elem, i, s)

	outputs.Emit1(elem.K, elem.V)
	outputs.Emit2(elem.K, elem.V)

	return nil
}

// Simple DoFn example (no error)
func Do0(_ context.Context, elem int, _, _, _ struct{}) {
	fmt.Println("Do0 called with:", elem)
}

// If we opt to have the 4 combinations of return options of (ProcessContinuation?,Error?)
// No "main return" nonesense.

// Pipeline Construction would be simplified to either have the simple convenience case
// of 1:1, but otherwise could have the map[string]PCollection return type.

// Doesn't enable compile type (with typed PCollections) safety easily unfortunately, not without additional type assertions.

// A downside is that users will not get compile time safety with the tags,
// because one can't construct tags from string constants by design https://github.com/golang/go/issues/4740

// This partly represents the invoker, but also the side/emitter setup as well.
// Since side inputs and emitters can already be reset, this
func CallWith[E any](fn interface{}, v E, sides, emits map[string]interface{}) error {
	t := reflect.TypeOf(fn)
	rv := reflect.ValueOf(fn)

	mst := t.In(2)
	m := reflect.New(mst).Elem()
	if mst.NumField() == 1 {
		f := m.Field(0)
		f.Set(reflect.ValueOf(EventTime(123456789)))
	}

	sst := t.In(3)
	s := reflect.New(sst).Elem()

	// TODO validation on types and counts for the compressed fields

	for i := 0; i < sst.NumField(); i++ {
		sf := sst.Field(i)
		tag := sf.Tag.Get("beamtag")
		side := sides[tag]
		f := s.Field(i)
		rside := reflect.ValueOf(side)
		f.Set(rside)
	}

	est := t.In(4)
	e := reflect.New(est).Elem()

	for i := 0; i < est.NumField(); i++ {
		sf := est.Field(i)
		tag := sf.Tag.Get("beamtag")
		emit := emits[tag]
		f := e.Field(i)
		remit := reflect.ValueOf(emit)
		f.Set(remit)
	}

	rv.Call([]reflect.Value{reflect.ValueOf(context.TODO()), reflect.ValueOf(v), m, s, e})
	return nil
}

func main() {

	CallWith(Do, struct {
		K string
		V int
	}{"foo", 42},
		map[string]interface{}{
			"tag1": func(i *int) bool {
				*i = 57
				return false
			},
			"tag2": func(s *string) bool {
				*s = "eureka"
				return false
			},
		},
		map[string]interface{}{
			"tag1": func(k string, v int) { fmt.Println("emit1", k, v) },
			"tag2": func(k string, v int) { fmt.Println("emit2", k, v) },
		},
	)

	CallWith(Do0, 0, nil, nil)
}
