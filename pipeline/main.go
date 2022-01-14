package main

import (
	"fmt"
	"strconv"
	"time"
)
var _ = time.Time{}

// Emitter types are a simple function.
type Emitter[E any] interface{
	Emit(E)
	EmitWithTime(time.Time,E)
}

// // ElementProcessor1Emit takes in a main input, and returns output with an emitter.
// type ElementProcessor1Emit[P0 any, R0 any] interface{
// 	ProcessElement(P0, Emitter[R0])
// }

// ElementProcessor1EmitFunc is a func takes in a main input, and returns output with an emitter.
type ElementProcessor1EmitFunc[P0 any, E0 Emitter[R0], R0 any] interface{
	func(P0, E0)
}

// ElementProcessor1Map takes in a main input, and returns an output directly, 1 to 1.
type ElementProcessor1Map[P0 any, R0 any] interface{
	ProcessElement(P0) R0
}

// ElementProcessor1MapFunc is a func that takes in a main input, and returns an output directly, 1 to 1.
type ElementProcessor1MapFunc[P0 any, R0 any] interface{
	func(P0) R0
}

// // ElementProcessor1 represents all types that process a single element and return a single element.
// type ElementProcessor1[P0 any, R0 any] interface{
// 	ElementProcessor1Emit[P0, R0] | ElementProcessor1EmitFunc[P0, R0] | ElementProcessor1Map[P0, R0] | ElementProcessor1MapFunc[P0, R0]
// }

// ElementProcessor1Func represents all func types that process a single element and return a single element.
type ElementProcessor1Func[P0 any, E0 Emitter[R0], R0 any] interface{
	ElementProcessor1MapFunc[P0, R0] | ElementProcessor1EmitFunc[P0, E0, R0] 
}

func ParDo1Func[Fn ElementProcessor1EmitFunc[P0, E0, R0], P0 any, E0 Emitter[R0], R0 any](dofn Fn, in PCollection[P0]) PCollection[R0] {
	return PCollection[R0]{Parent: fmt.Sprintf("%T", dofn)}
}

type PCollection[E any] struct{
	Parent string
	// No implementation because we care about whether things compile not if they work.
}

// Now we check all the variants.
func mapFn(a int) string {
	return fmt.Sprintf("%d", a)
}

func emitFn(a int, e Emitter[string]) {
	e.Emit(mapFn(a))
}

type EP1v1[P0, R0 any] interface{
	ProcessElement(P0, Emitter[R0])
}

func ParDo1v1[P0,R0 any](dofn EP1v1[P0,R0], in PCollection[P0]) PCollection[R0] {
	return PCollection[R0]{Parent: fmt.Sprintf("%T", dofn)}
}

type Side[P1 any] interface{
	Next(*P1) bool
}

type EP2v1[P0,P1, R0 any] interface{
	ProcessElement(P0, Side[P1], Emitter[R0])
}

func ParDo2v1[P0, P1,R0 any](dofn EP2v1[P0,P1,R0], in PCollection[P0], side PCollection[P1]) PCollection[R0] {
	return PCollection[R0]{Parent: fmt.Sprintf("%T", dofn)}
}


type itoaFn struct{}

func (*itoaFn) ProcessElement(v int,e Emitter[string]) {
	e.Emit(mapFn(v))
}

type atoiFn struct{}

func (*atoiFn) ProcessElement(v string, e Emitter[int]) {
	i, _ := strconv.Atoi(v)
	e.Emit(i)
}

type atoi2Fn struct{}

func (*atoi2Fn) ProcessElement(v string, iter Side[int], e Emitter[int]) {
    var i int
	iter.Next(&i)
	e.Emit(i)
}


type KV[K, V any] struct{
    Key K
    Value V
}

type KVsi = KV[string,int]

type toKVFn struct{}

func (*toKVFn) ProcessElement(v int, e Emitter[KVsi]) {
	e.Emit(KVsi{Key: mapFn(v), Value: v})
}

type MapSide[P1 KV[K,V], K, V any] interface{
	Side[P1]
	Lookup(K) Side[V]
}

type lookupFn struct{}

func (*lookupFn) ProcessElement(v string, iter Side[KVsi], e Emitter[int]) {
	lk := iter.(MapSide[KVsi, string, int])
	iter2 := lk.Lookup(v)
    var i int
	iter2.Next(&i)
	e.Emit(i)
}


func main() {
	inputCol := PCollection[int]{}

	itoaOutputCol := ParDo1v1[int, string](&itoaFn{}, inputCol)
	fmt.Printf("itoaOutputCol %T %+v\n", itoaOutputCol, itoaOutputCol)

	atoiOutputCol := ParDo1v1[string, int](&atoiFn{}, itoaOutputCol)
	fmt.Printf("atoiOutputCol %T %+v\n", atoiOutputCol, atoiOutputCol)

	atoi2OutputCol := ParDo2v1[string, int, int](&atoi2Fn{}, itoaOutputCol, inputCol)
	fmt.Printf("atoi2OutputCol %T %+v\n", atoi2OutputCol, atoi2OutputCol)

	kvOutputCol := ParDo1v1[int, KVsi](&toKVFn{}, inputCol)
	fmt.Printf("kvOutputCol %T %+v\n", kvOutputCol, kvOutputCol)

	lookupOutputCol := ParDo2v1[string, KVsi, int](&lookupFn{}, itoaOutputCol, kvOutputCol)
	fmt.Printf("lookupOutputCol %T %+v\n", lookupOutputCol, lookupOutputCol)
}

