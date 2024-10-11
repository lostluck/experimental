package coders

import (
	"fmt"
	"reflect"
	"time"

	"golang.org/x/exp/constraints"
)

// Coder represents a coder for a specific type.
type Coder[E any] interface {
	Encode(enc *Encoder, v E)
	Decode(dec *Decoder) E
}

// Codable represents types that know how to code themselves.
type Codable interface {
	Encode(enc *Encoder)
	Decode(dec *Decoder)
}

// MakeCoder is a convenience function for primitive coders access.
func MakeCoder[E any]() Coder[E] {
	var e E
	rt := reflect.TypeOf(e)
	if rt.Kind() == reflect.Struct {
		return makeRowCoder[E](rt).(Coder[E])
	}
	// if rt.Kind() == reflect.Slice {
	// 	return makeSliceCoder[E](rt).(Coder[E])
	// }
	return makeCoder(reflect.TypeOf(e)).(Coder[E])
}

// makeCoder works around generic coding.
func makeCoder(rt reflect.Type) any {
	switch rt.Kind() {
	case reflect.Bool:
		return boolCoder{}
	case reflect.Int:
		return varintCoder[int]{}
	case reflect.Int8:
		return varintCoder[int8]{}
	case reflect.Int16:
		return varintCoder[int16]{}
	case reflect.Int32:
		return varintCoder[int32]{}
	case reflect.Int64:
		return varintCoder[int64]{}
	case reflect.Uint:
		return varintCoder[uint]{}
	case reflect.Uint8:
		return byteCoder{}
	case reflect.Uint16:
		return varintCoder[uint16]{}
	case reflect.Uint32:
		return varintCoder[uint32]{}
	case reflect.Uint64:
		return varintCoder[uint64]{}
	case reflect.Float64:
		return doubleCoder{}
	case reflect.String:
		return stringCoder{}
	case reflect.Slice:
		switch rt.Elem().Kind() {
		case reflect.Uint8:
			return bytesCoder{}
		}
	}
	// Returning nil since type assertion elsewhere will provide better information
	// to the developer.
	return nil
}

func makeRowCoder[E any](rt reflect.Type) any {
	return buildRowCoder(&rowStructCoder[E]{}, rt)
}

var (
	rtTimeTime = reflect.TypeFor[time.Time]()
)

// buildRowCoder abstracts between the generic top level,
// and the interface/reflect based nested levels for Row coders.
func buildRowCoder[C rowStructCoderBuilder](c C, rt reflect.Type) C {
	switch rt {
	case rtTimeTime:
		c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
			//fmt.Printf("rt %v %T\n", rt, c)
			t := rv.Interface().(time.Time)
			mar, _ := t.MarshalText()
			enc.Bytes(mar)
		})
		c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
			t := time.Time{}
			t.UnmarshalText(dec.Bytes())
			rv.Set(reflect.ValueOf(t))
		})
		return c
	}
	// TODO: move this to be generated from the Schema + the user type.
	// Also need to deal with length prefixing. Ugh.
	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)
		if !sf.IsExported() {
			// Put in dummy handlers for unexported fields.
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {})
			continue
		}
		switch sf.Type.Kind() {
		case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
				enc.Varint(uint64(rv.Int()))
			})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
				rv.SetInt(int64(dec.Varint()))
			})
		case reflect.String:
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
				enc.StringUtf8(rv.String())
			})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
				rv.SetString(dec.StringUtf8())
			})
		case reflect.Struct:
			nrc := buildRowCoder(&rowStructCoderNested{rt: sf.Type}, sf.Type)
			c.appendEncoder(func(enc *Encoder, rv reflect.Value) {
				nrc.Encode(enc, rv)
			})
			c.appendDecoder(func(dec *Decoder, rv reflect.Value) {
				rv.Set(nrc.Decode(dec))
			})
		default:
			panic("row field type unknown:" + sf.Type.Kind().String() + " for type " + rt.Name())
		}
	}
	return c
}

type rowStructCoderBuilder interface {
	appendEncoder(func(enc *Encoder, rv reflect.Value))
	appendDecoder(func(dec *Decoder, rv reflect.Value))
}

type rowStructCoder[T any] struct {
	fieldEncoders []func(enc *Encoder, rv reflect.Value)
	fieldDecoders []func(dec *Decoder, rv reflect.Value)
}

func (c *rowStructCoder[T]) appendEncoder(encFn func(enc *Encoder, rv reflect.Value)) {
	c.fieldEncoders = append(c.fieldEncoders, encFn)
}

func (c *rowStructCoder[T]) appendDecoder(decFn func(dec *Decoder, rv reflect.Value)) {
	c.fieldDecoders = append(c.fieldDecoders, decFn)
}

func (c *rowStructCoder[T]) Encode(enc *Encoder, v T) {
	//	fmt.Printf("rowStructCoder[T].Encode %T %+v %v\n", c, v, len(c.fieldEncoders))
	rv := reflect.ValueOf(v)
	enc.Varint(uint64(rv.NumField()))
	for i := 0; i < rv.NumField(); i++ {
		c.fieldEncoders[i](enc, rv.Field(i))
	}
}

func (c *rowStructCoder[T]) Decode(dec *Decoder) T {
	var v T
	rv := reflect.ValueOf(&v).Elem()
	i := 0
	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Sprintf("field %v:\n%v", i, e))
		}
	}()
	n := dec.Varint()
	if int(n) != rv.NumField() {
		panic(fmt.Sprintf("row value got %v fields want %v fields for a %v", n, rv.NumField(), rv.Type()))
	}
	for ; i < rv.NumField(); i++ {
		c.fieldDecoders[i](dec, rv.Field(i))
	}
	return rv.Interface().(T)
}

// Is this the way to do nested types?
type rowStructCoderNested struct {
	rt            reflect.Type
	fieldEncoders []func(enc *Encoder, rv reflect.Value)
	fieldDecoders []func(dec *Decoder, rv reflect.Value)
}

func (c *rowStructCoderNested) appendEncoder(encFn func(enc *Encoder, rv reflect.Value)) {
	c.fieldEncoders = append(c.fieldEncoders, encFn)
}

func (c *rowStructCoderNested) appendDecoder(decFn func(dec *Decoder, rv reflect.Value)) {
	c.fieldDecoders = append(c.fieldDecoders, decFn)
}

func (c *rowStructCoderNested) Encode(enc *Encoder, rv reflect.Value) {
	//	fmt.Println("rowStructCoderNested.Encode", c.rt, rv, len(c.fieldEncoders))
	enc.Varint(uint64(rv.NumField()))
	// This is the worst formulation.
	// We need to be able to override types, but substitute them with arbitrary encoding structures.
	switch rv.Type() {
	case rtTimeTime:
		c.fieldEncoders[0](enc, rv)
		return
	}
	for i := 0; i < rv.NumField(); i++ {
		c.fieldEncoders[i](enc, rv.Field(i))
	}
}

func (c *rowStructCoderNested) Decode(dec *Decoder) reflect.Value {
	// fmt.Println("rowStructCoderNested.Decode", c.rt, len(c.fieldDecoders))
	rv := reflect.New(c.rt).Elem()
	i := 0
	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Sprintf("field %v:\n%v", i, e))
		}
	}()
	n := dec.Varint()
	// This is the worst formulation.
	// We need to be able to override types, but substitute them with arbitrary encoding structures.
	switch rv.Type() {
	case rtTimeTime:
		c.fieldDecoders[0](dec, rv)
		return rv
	}
	if int(n) != rv.NumField() {
		panic(fmt.Sprintf("row value got %v fields want %v fields for a %v", n, rv.NumField(), rv.Type()))
	}
	for ; i < rv.NumField(); i++ {
		c.fieldDecoders[i](dec, rv.Field(i))
	}
	return rv
}

func makeSliceCoder[E any](rt reflect.Type) any {
	panic("makeSliceCoder is unimplemented")
}

type sliceCoder[T any] struct{}

type varintCoder[T constraints.Integer] struct{}

func (varintCoder[T]) Encode(enc *Encoder, v T) {
	enc.Varint(uint64(v))
}

func (varintCoder[T]) Decode(dec *Decoder) T {
	return T(dec.Varint())
}

type byteCoder struct{}

func (byteCoder) Encode(enc *Encoder, v byte) {
	enc.Byte(v)
}

func (byteCoder) Decode(dec *Decoder) byte {
	return dec.Byte()
}

type bytesCoder struct{}

func (bytesCoder) Encode(enc *Encoder, v []byte) {
	enc.Bytes(v)
}

func (bytesCoder) Decode(dec *Decoder) []byte {
	return dec.Bytes()
}

type stringCoder struct{}

func (stringCoder) Encode(enc *Encoder, v string) {
	enc.StringUtf8(v)
}

func (stringCoder) Decode(dec *Decoder) string {
	return dec.StringUtf8()
}

type doubleCoder struct{}

func (doubleCoder) Encode(enc *Encoder, v float64) {
	enc.Double(v)
}

func (doubleCoder) Decode(dec *Decoder) float64 {
	return dec.Double()
}

type boolCoder struct{}

func (boolCoder) Encode(enc *Encoder, v bool) {
	enc.Bool(v)
}

func (boolCoder) Decode(dec *Decoder) bool {
	return dec.Bool()
}
