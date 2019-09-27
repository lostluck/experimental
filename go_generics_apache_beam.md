# Possible uses of Go Generics in the Apache Beam Go SDK

author: Robert Burke (lostluck@)
last updated: 2019-07-29

This document is intended as an experience report on different ways the 
Apache Beam Go SDK can use generics. This document explores the possibilities
based on the [July 25th 2019 Draft Design for Generics in Go](https://go.googlesource.com/proposal/+/refs/heads/master/design/go2draft-contracts.md)

As it is a draft design, no concrete plans can be made about the Go SDK and Generics. 
The current draft is backwards compatible, so user code will be allowed to use Generics even
without the SDK adopting them. That said this document outlines the opportunities 
the draft enables.

## Motivation

Generics are of particular interest to Apache Beam because of the project's roots in Java.
The Go SDK implementation at present had to take a different approach due to the lack of 
generics [See the Design RFC](https://s.apache.org/beam-go-sdk-design-rfc).
It's worth exploring if a subsequent version of the Go SDK can be closer to the Java 
implementation in appearance to ease understanding beam code. However, this shouldn't
be done at the expense of the SDK following eventual Go Generic Idioms, as they arise.

The Go SDK by default uses Reflection extensively both at Pipeline Construction time, and
at Pipeline Execution time. 

## Opportunities

### Execution Time - Function Invocation

This section explores if it's possible to avoid a code generation step for
DoFn authors under the generics draft design.

To avoid reflection at Pipeline Execution time, which adds
undesirable overhead per DoFn invocation, the Go SDK provides a mechanism to use type
assertion instead. The code is sufficiently boiler plate that a code generator exists
designed to work with `go generate` so users can generate specializations for their
DoFns as needed. 

This avoids the reflection overhead invoking user functions, and  wrapping and unwrapping
values with reflect.Value, in favour of using `interface{}` and type assertion.

There are three places the Go SDK uses code generation to avoid reflection at 
pipeline execution time. First is general Function Invocation, second is passing
Emitted values, and the third is receiving iterator inputs. 

OK, so imagine I have the following user function:
```go
func MyIntFunc(k, v int) error 
```
`MyIntFunc` is of type `func(k, v int) error`

This function takes 2 arguments and returns error (thus is a reflectx.Func2x1). TODO: explain & link.

I’d like to be able to call a function of type `func(arg0, arg1 interface{}) interface{}`, or of type `func([]interface{}) []interface{}` which then invokes `MyIntFunc`. 

The generated code looks similar to the following:
```go
type myCaller2x1 struct{
  fn func(k, v int) error 
}

func (c *myCaller2x1) Call(args []interface{}) []interface{} {
	out0:= c.fn(args[0].(int), args[1].(int))
	return []interface{}{out0}
}

func (c *myCaller2x1) Call2x1(arg0, arg1, interface{}) interface{} {
	return c.fn(arg0.(int), arg1.(int)))
}
```
In order to create these, a factory function of type `func(interface{}) reflectx.Func` must exist.
This combined with the a reflect.Type for the function, permits registration and lookup of the factory,
which can then be commonly invoked.

```
// Factory for MyCaller2x1
func funcMakerMyCaller2x1(fn interface{}) reflectx.Func {
	f := fn.(func(k, v int) error)
	return &myCaller2x1{fn: f}
}
```
The generated code above has been edited, the parameter and return types are usually included
into the names of the caller type and the factory function. They've been omitted for clarity.

Can we make it so generics (as drafted) and the compiler to do this work instead?

Lets re-imagine our Caller as Generic.

```go
// P indicates a parameter type, and R indicates a return type.
type myCaller2x1G(type P0, P1, R0) struct{
  fn func(P0, P1) R0 
}

func (c *myCaller3x1( P0, P1, R0)) Call(args []interface{}) []interface{} {
	out0:= c.fn(args[0].(P0), args[1].(P1))
	return []interface{}{out0}
}

func (c *myCaller3x1(P0, P1, R0)) Call3x1(arg0, arg1 interface{}) interface{} {
	return c.fn(arg0.(P0), arg1.(P1))
}

```
This approach is promising, but an issue exists around the factory. There's no way
to get the generic parameters to a generic factory function.
```
// INVALID
// Generic factory function: Correct signature, but cannot be invoked.
func funcMakerMyCaller3x1(type P0, P1, R0)(interface{}) reflectx.Func {
	f := fn.(func(P0, P1, P2) R0)
	return &myCaller{fn: f}
}

// INVALID
// Generic factory function: Incorrect signature, can't be registered.
func funcMakerMyCaller3x1(type P0, P1, R0)(func(P0,P1) R0) reflectx.Func {
	f := fn.(func(P0, P1, P2) R0)
	return &myCaller{fn: f}
}
```

However, we have an advantage. Go has robust Closure support. We can produce a generic factory factory
and have the types passed through a closure.

```
func FuncCaller3x1Maker(type P0, P1, R0)(fn func(P0,P1) R0) (reflect.Type, func(interface{}) reflectx.Func ) {
  // Convenience register to avoid verbosity
  beam.RegisterFunction(fn) 
  return reflect.TypeOf(fn), func(fn interface{}) reflectx.Func {
    f := fn.(func(P0, P1, P2) R0)
    return &myCaller{fn: f}
  }
}

// Usage Example, for Functions, this is all that’s needed.
beam.RegisterFunc(gbeam.FuncCaller3x1Maker(int, int, error)(MyIntFunc))
beam.RegisterFunc(gbeam.FuncCaller3x1Maker(MyIntFunc)) // Type inference should permit eliding the types.

```

This implies that we can avoid the code generatore.
The Maker Factories can live in their own package, and we can generate the PxR variants  all combinations of P 0-7 x R 0-4. (~40 variants), then users just need to add the one type checked registration call.  Function Overloads (HA) would simply avoid the suffix in the user use.


Ideally, we have something like

```go
beam.RegisterFunction(interface{})
```

And it would work. Unfortunately it’s not so simple.

IIRC there’s a requirement that the user code is invoking the type parameters

### Execution Time - Emitter functions, Iterators functions

TODO

Support more standard forms for iterators, and emitters?

### Value Transport (FullValue etc)

TODO

### Advanced PCollection types (KVs, CoGBKS etc)

TODO

### Avoid Positonal Arguments?

TODO

### Compile time Pipeline Construction Safety?

TODO
(What about Side Inputs etc)?

### Changes that required a new New Go SDK version

TODO

## Observations

Even with Generics in Go, the Go SDK cannot follow the JavaSDK exactly due to no
method overloading. Each identifier in Go may only refer to a single entity within
a given scope.
