// Package retrospective contains sample code and
// benchmarks borrowed librally from the beam
// exec package benchmarks.
package retrospective

import (
	"bytes"
	"fmt"
	"reflect"
	"text/template"
)

// Foo is an example float64 to float64 function.
func Foo(f float64) float64 {
	return f * f
}

// Bar is an example float64 to int64 function.
func Bar(f float64) int64 {
	return int64(f)
}

// Baz is an example int64 to string function.
func Baz(i int64) string {
	return fmt.Sprintf("%d", i)
}

// The squareroot of 42.
const input = 6.48074069841

// direct is an example pipeline in ordinary Go.
func direct(in float64) string {
	a := Foo(in)
	b := Bar(a)
	c := Baz(b)
	return c
}

// Indirect is an example pipeline indirecting the
// parameters through an interface{} value.
func indirect(in interface{}) interface{} {
	var val interface{}
	val = in
	val = Foo(val.(float64))
	val = Bar(val.(float64))
	val = Baz(val.(int64))
	return val
}

// Foo2 is an example interface{} to interface{} function.
func Foo2(f interface{}) interface{} {
	return f.(float64) * f.(float64)
}

// Bar2 is an example interface{} to interface{} function.
func Bar2(f interface{}) interface{} {
	return int64(f.(float64))
}

// Baz2 is an example interface{} to interface{} function.
func Baz2(i interface{}) interface{} {
	return fmt.Sprintf("%d", i)
}

// indirectFuncs uses in
func indirectFuncs(in interface{}) interface{} {
	val := interface{}(in)
	val = Foo2(val)
	val = Bar2(val)
	val = Baz2(val)
	return val
}

// This version doesn't work since we can't "generically" execute functions
// that are hidden under interface{}.
// func pipeline(in interface{}) {
// 	val := interface{}(in)
// 	fns := []interface{}{Foo2, Bar2, Baz2}
// 	for _, fn := range fns {
// 		val = fn(val)
//  }
// }

// Fn is a "generic" type signature, that the "2"
// variants of the functions implement.
type Fn func(interface{}) interface{}

// pipeline uses the Fn interface to be able to execute the
// functions.
func pipeline(in interface{}) interface{} {
	val := interface{}(in)
	fns := []Fn{Foo2, Bar2, Baz2}
	for _, fn := range fns {
		val = fn(val)
	}
	return val
}

// pipelineOpt is the same as pipeline, but allows the function
// slice to be executed to be passed in instead of being hard coded.
func pipelineOpt(in interface{}, fns []Fn) interface{} {
	val := interface{}(in)
	for _, fn := range fns {
		val = fn(val)
	}
	return val
}

// reflectPipeline uses the reflect library to execute the function.
func reflectPipeline(in interface{}) interface{} {
	val := reflect.ValueOf(in)
	fns := []interface{}{Foo, Bar, Baz}
	for _, fn := range fns {
		rFn := reflect.ValueOf(fn)
		rVals := rFn.Call([]reflect.Value{val})
		val = rVals[0]
	}
	return val.Interface()
}

// reflectPipelineAdjusted is the same as reflectPipeline, but
// avoids reallocating slices unnecessarily.
func reflectPipelineAdjusted(in interface{}) interface{} {
	rVals := []reflect.Value{reflect.ValueOf(in)}
	fns := []interface{}{Foo, Bar, Baz}
	for _, fn := range fns {
		rFn := reflect.ValueOf(fn)
		rVals = rFn.Call(rVals)
	}
	return rVals[0].Interface()
}

// reflectPipelineOpt allows for the pre-processed pipeline functions
// to be passed in, while also re-using the slices repeatedly.
func reflectPipelineOpt(in interface{}, rFns []reflect.Value) interface{} {
	rVals := []reflect.Value{reflect.ValueOf(in)}
	for _, rFn := range rFns {
		rVals = rFn.Call(rVals)
	}
	return rVals[0].Interface()
}

// switchCall uses a type switch to type assert the passed in
// interfaces to their original signatures and calls it.
func switchCall(fn, in interface{}) interface{} {
	switch call := fn.(type) {
	case Fn:
		return call(in)
	case func(float64) float64:
		return call(in.(float64))
	case func(float64) int64:
		return call(in.(float64))
	case func(int64) string:
		return call(in.(int64))
	default:
		panic(fmt.Sprintf("unknown signature %T", fn))
	}
}

// switchedPipeline uses switchCall to call the functions.
func switchedPipeline(in interface{}) interface{} {
	val := interface{}(in)
	fns := []interface{}{Foo, Bar, Baz}
	for _, fn := range fns {
		val = switchCall(fn, val)
	}
	return val
}

// switchCall2 uses a type switch to type assert the passed in
// interfaces to their original signatures and calls it, but
// also uses reflection as a fallback.
func switchCall2(fn, in interface{}) interface{} {
	switch call := fn.(type) {
	case Fn:
		return call(in)
	case func(float64) float64:
		return call(in.(float64))
	case func(float64) int64:
		return call(in.(float64))
	case func(int64) string:
		return call(in.(int64))
	default:
		rFn := reflect.ValueOf(fn)
		rVals := rFn.Call([]reflect.Value{reflect.ValueOf(in)})
		return rVals[0].Interface()
	}
}

// Not quite a Generic Go function. (missing type parameters)
// func template(fn, in interface{}) interface{} {
// 	return fn.(func(I) O)(in.(I))
// }

// Not quite a Generic Go struct. (missing type parameters)
// type template struct {
// 	fn func(I) O
// }

// func (t *template) Call(in interface{}) interface{} {
// 	return t.fn(in.(I))
// }

// Caller allows any struct to act as an adapter/wrapper
// to any 1x1 function or method.
type Caller interface {
	Call(interface{}) interface{}
}

//  Example Caller wrappers for Foo, Bar, and Baz.

type cF64rF64 struct {
	fn func(float64) float64
}

func (c *cF64rF64) Call(in interface{}) interface{} {
	return c.fn(in.(float64))
}

type cF64rI64 struct {
	fn func(float64) int64
}

func (c *cF64rI64) Call(in interface{}) interface{} {
	return c.fn(in.(float64))
}

type cI64rStr struct {
	fn func(int64) string
}

func (c *cI64rStr) Call(in interface{}) interface{} {
	return c.fn(in.(int64))
}

// callerPipeline implements our pipeline using the Caller interface.
func callerPipeline(in interface{}) interface{} {
	val := in
	cFns := []Caller{
		&cF64rF64{fn: Foo},
		&cF64rI64{fn: Bar},
		&cI64rStr{fn: Baz},
	}
	for _, cFn := range cFns {
		val = cFn.Call(val)
	}
	return val
}

// callerPipelineOpt is the same as callerPipeline, but has the function
// pipeline passed in.
func callerPipelineOpt(in interface{}, cFns []Caller) interface{} {
	val := in
	for _, cFn := range cFns {
		val = cFn.Call(val)
	}
	return val
}

// makerRegistry holds the registrations for all the Caller factories.
// Keyed on the string of the type signature of the function.
var makerRegistry = map[string]func(fn interface{}) Caller{}

// RegisterMaker adds caller factories to the registry for later use.
func RegisterMaker(t reflect.Type, factory func(interface{}) Caller) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	makerRegistry[t.String()] = factory
}

// MakeCaller wraps the passed in function in a caller object
// so it can be executed by pipeline without worrying about types.
//
// If a given type signature doesn't exist, generate the appropriate
// code for a maker, and include that in the error string.
func MakeCaller(fn interface{}) (Caller, error) {
	t := reflect.TypeOf(fn)
	if factory, ok := makerRegistry[t.String()]; ok {
		return factory(fn), nil
	}
	d := callerData{
		Name: fmt.Sprintf("c%vx%v", t.In(0).String(), t.Out(0).String()),
		I:    t.In(0).String(),
		Sig:  t.String(),
	}
	var buf bytes.Buffer
	if err := callerTmpl.Execute(&buf, d); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("%s", buf.String())
}

type callerData struct {
	Name, I, Sig string
}

var callerTmpl = template.Must(
	template.New("caller").Parse(`
	type {{.Name}} struct {
		fn {{.Sig}}
	}
	
	func (c *{{.Name}}) Call(in interface{}) interface{} {
		return c.fn(in.({{.I}}))
	}
	
	func {{.Name}}Maker(fn interface{}) Caller {
		return &{{.Name}}{fn: fn.({{.Sig}})}
	}

	func init() {
		RegisterMaker(reflect.TypeOf((*{{.Sig}})(nil)), {{.Name}}Maker)
	}
`))

// Below here is the pasted in code from the above generator.
// The above code has many limitations that can be worked around,
// and have been in the system used for the code generation system
// written for the Apache Beam Go SDK.
//
// https://github.com/apache/beam/blob/master/sdks/go/pkg/beam/util/starcgenx/starcgenx.go
// https://github.com/apache/beam/blob/master/sdks/go/pkg/beam/util/shimx/generate.go
//
// With Generics in Go though, it's possible to do the same thing without
// a 2 pass system (either run + rerun, or go generate + run), at the small cost of user specificity.
//
// See https://go2goplay.golang.org/p/5W39ybOQooA for an example of how this looks
// with type parameters instead.

type cfloat64xfloat64 struct {
	fn func(float64) float64
}

func (c *cfloat64xfloat64) Call(in interface{}) interface{} {
	return c.fn(in.(float64))
}

func cfloat64xfloat64Maker(fn interface{}) Caller {
	return &cfloat64xfloat64{fn: fn.(func(float64) float64)}
}

func init() {
	RegisterMaker(reflect.TypeOf((*func(float64) float64)(nil)), cfloat64xfloat64Maker)
}

type cfloat64xint64 struct {
	fn func(float64) int64
}

func (c *cfloat64xint64) Call(in interface{}) interface{} {
	return c.fn(in.(float64))
}

func cfloat64xint64Maker(fn interface{}) Caller {
	return &cfloat64xint64{fn: fn.(func(float64) int64)}
}

func init() {
	RegisterMaker(reflect.TypeOf((*func(float64) int64)(nil)), cfloat64xint64Maker)
}

type cint64xstring struct {
	fn func(int64) string
}

func (c *cint64xstring) Call(in interface{}) interface{} {
	return c.fn(in.(int64))
}

func cint64xstringMaker(fn interface{}) Caller {
	return &cint64xstring{fn: fn.(func(int64) string)}
}

func init() {
	RegisterMaker(reflect.TypeOf((*func(int64) string)(nil)), cint64xstringMaker)
}
