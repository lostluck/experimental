# Purpose - 2022-01-14

This is me maintaining a dev log for trying to do terrible things with Go Generics.

Overall investigation goal is to see how much the constraint type inference in Go can
handle the multiplicity of definitions that Beam Pipelines can have.

In particular, handling DoFns with: Side Inputs, KVs, Emitters, or not.

Focusing on a single input PCollection and a single output PCollection.

This will inform just how far we can go in adding support for compile time pipeline safety
without forcing a full rewrite for users.

----

# First attempt, Funcs and Structs together

Unfortunately, we can't presently unify this for both functional and structural DoFns. 

Something like

```

// ElementProcessor1Map takes in a main input, and returns an output directly, 1 to 1.
type ElementProcessor1Map[P0 any, R0 any] interface{
	ProcessElement(P0) R0
}

// ElementProcessor1MapFunc is a func that takes in a main input, and returns an output directly, 1 to 1.
type ElementProcessor1MapFunc[P0 any, R0 any] interface{
	func(P0) R0
}

// ElementProcessor1 represents all types that process a single element and return a single element.
type ElementProcessor1[P0 any, R0 any] interface{
	ElementProcessor1Map[P0, R0] | ElementProcessor1MapFunc[P0, R0]
}

```

doesn't work because interfaces with methods can't be used in constraint unions at this time. 

So ElementProcessor1MapFunc, and it's like could be unified, since it's just got a type constraint
but not the ones that have different ProcessElement variant. Pity. Perhaps later.

------
```
// Emitter types are a simple function.
type Emitter[E any] interface{
	func(E)
}

// ElementProcessor1Emit takes in a main input, and returns output with an emitter.
type ElementProcessor1Emit[P0 any, R0 any] interface{
	ProcessElement(P0, Emitter[R0])
}
```

triggers a compile time error saying message "interface contains type constraints" which implies that
the constraint inference system can't go deeper for interfaces by themselves. 

The question now is can we work around it?

----

Changing the func emitter type to be included in the type parameters works.

```
// ElementProcessor1EmitFunc is a func takes in a main input, and returns output with an emitter.
type ElementProcessor1EmitFunc[P0 any, E0 Emitter[R0], R0 any] interface{
	func(P0, E0)
}
```

But we end up needing to hoist everything to whatever the top level is, especially WRT the emitters.
Eventually, there's a fundamental conflict between types that use emitters, and those that use a simple return.
The emitter type is unable to be inferred, requiring the types to be fully specified at the call site.

```
type ElementProcessor1Func[P0 any, E0 Emitter[R0], R0 any] interface{
	ElementProcessor1MapFunc[P0, R0] | ElementProcessor1EmitFunc[P0, E0, R0] 
}

func ParDo1Func[Fn ElementProcessor1Func[P0, E0, R0], P0 any, E0 Emitter[R0], R0 any](dofn Fn, in PCollection[P0]) PCollection[R0] {
	return PCollection[R0]{Parent: fmt.Sprintf("%T", dofn)}
}

func main() {
	inputCol := PCollection[int]{}
	mapOutputCol := ParDo1Func[func(int) string,int,func(string),string](mapFn, inputCol)
	fmt.Printf("mapOutputCol %T %+v\n", mapOutputCol, mapOutputCol)
	emitOutputCol := ParDo1Func[func(int, func(string)),int,func(string),string](emitFn, inputCol)
	fmt.Printf("emitOutputCol %T %+v\n", emitOutputCol, emitOutputCol)
}

// Output:
mapOutputCol main.PCollection[string] {Parent:func(int) string}
emitOutputCol main.PCollection[string] {Parent:func(int, func(string))}
```

This implies that any generic construction path will require a pruning of supported DoFn structures
in order to have compile time type safety of the pipeline.

But I could be missing something.

---

Removing the MapFn support, allows inference to occur, so that's the problem.
If it can't determine the type in all paths we have an issue. 

Further if we adjust the generic Emitter interface to have a different variant:

```
type Emitter[E any] interface{
	func(E) | func(time.Time, E)
}
```

Inference stops working there too. This implies that we probably want to sub in
a real "Emitter" type. This will require users to migrate all their code anyway.

-----

Since we're in "Hard Migration" territory anyway, a question comes down to how deep can we go?

Changing Emitter to just an generic interface type  and adjusting emitFn accordingly:

```
// Emitter types are a simple function.
type Emitter[E any] interface{
	Emit(E)
	EmitWithTime(time.Time,E)
}

func emitFn(a int, e Emitter[string]) {
	e.Emit(mapFn(a))
}

func ParDo1Func[Fn ElementProcessor1EmitFunc[P0, E0, R0], P0 any, E0 Emitter[R0], R0 any](dofn Fn, in PCollection[P0]) PCollection[R0] {
	return PCollection[R0]{Parent: fmt.Sprintf("%T", dofn)}
}
```

ends up having the the ParDo1Func being unable infer the return type (string). Which is also dissapointing.

Ideally we'd be able to infer and not worry about it. Doesn't seem possible at present.

-----

Lets assume then what if users are OK being explicit about their types at the call sites?
How does that look?
Can we make it minimal and gain some convenience, at the expense of some additional construction time functions?

This assumes the user values Compile Time type safety and doesn't mind having to pick out the right method.

Not so bad for the simplest cases.

```
type EP1v1[P0, R0 any] interface{
	ProcessElement(P0, Emitter[R0])
}

func ParDo1v1[P0,R0 any](dofn EP1v1[P0,R0], in PCollection[P0]) PCollection[R0] {
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

func main() {
	inputCol := PCollection[int]{}
	itoaOutputCol := ParDo1v1[int, string](&itoaFn{}, inputCol)
	fmt.Printf("itoaOutputCol %T %+v\n", itoaOutputCol, itoaOutputCol)
	atoiOutputCol := ParDo1v1[string, int](&atoiFn{}, itoaOutputCol)
	fmt.Printf("atoiOutputCol %T %+v\n", atoiOutputCol, atoiOutputCol)
}

// Output:
itoaOutputCol main.PCollection[string] {Parent:*main.itoaFn}
atoiOutputCol main.PCollection[int] {Parent:*main.atoiFn}
```

Adding in an iterable side input, reveals the limitations of this approach.

```
type Side[P1 any] interface{
	Next(*P1) bool
}

type EP2v1[P0,P1, R0 any] interface{
	ProcessElement(P0, Side[P1], Emitter[R0])
}

func ParDo2v1[P0, P1,R0 any](dofn EP2v1[P0,P1,R0], in PCollection[P0], side PCollection[P1]) PCollection[R0] {
	return PCollection[R0]{Parent: fmt.Sprintf("%T", dofn)}
}

type atoi2Fn struct{}

func (*atoi2Fn) ProcessElement(v string, iter Side[int], e Emitter[int]) {
    var i int
	iter.Next(&i)
	e.Emit(i)
}

atoi2OutputCol := ParDo2v1[string, int, int](&atoi2Fn{}, itoaOutputCol, inputCol)
```

If we wanted to handle *all* the various forms of side input as well, we run into some issues, since it could 
mean a different type and function calls for each of these. That explodes a fair amount by itself.
Iterators, ReIterators, Map side inputs... they'd all need to be represented.
Lets not forget "user views" which may eventually be added to the SDK.

All of these are without even considering KVs too. Easy enough to start.

```
type KV[K, V any] struct{
    Key K
    Value V
}
```

And moving along to try to support any kind of Map KVs, which we can know apriori by the PCollection used as a side input...

```

type KVsi = KV[string,int]

type toKVFn struct{}

func (*toKVFn) ProcessElement(v int, e Emitter[KVsi]) {
	e.Emit(KVsi{Key: mapFn(v), Value: v})
}

type MapSide[P1 KV[K, V], K, V any] interface{
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

...

lookupOutputCol := ParDo2v1[string, KVsi, int](&lookupFn{}, itoaOutputCol, kvOutputCol)
fmt.Printf("lookupOutputCol %T %+v\n", lookupOutputCol, lookupOutputCol)

// Output: 
lookupOutputCol main.PCollection[int] {Parent:*main.lookupFn}
```

The oddity there is we're moving some parts to the runtime in the DoFn, since the user needs
to type assert to the MapSide structure. Also, note that the `KVsi` type isn't a type, but
a type alias. As a concrete type, it requires KVsi does not implement KV[string, int].

We also can't trivially use the new `~` token to get around the alias. We instead get an error like

```
./main.go:120:17: invalid use of ~ (underlying type of KV[K, V] is struct{Key K; Value V})
./main.go:128:22: cannot implement ~KV[string, int] (empty type set)
```

It remains an open question whether we could implement this in a way that it executes.

That's enough for today though. There is much to think about.






