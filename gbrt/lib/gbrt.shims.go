// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by starcgen. DO NOT EDIT.
// File: gbrt.shims.go

package gbrt

import (
	"context"
	"fmt"
	"io"
	"reflect"

	// Library imports
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
)

func init() {
	runtime.RegisterFunction(ToPixelColour)
	runtime.RegisterType(reflect.TypeOf((*CombinePixelsFn)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*CombinePixelsFn)(nil)).Elem())
	runtime.RegisterType(reflect.TypeOf((*ImageConfig)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*ImageConfig)(nil)).Elem())
	runtime.RegisterType(reflect.TypeOf((*MakeImageFn)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*MakeImageFn)(nil)).Elem())
	runtime.RegisterType(reflect.TypeOf((*Pixel)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*Pixel)(nil)).Elem())
	runtime.RegisterType(reflect.TypeOf((*PixelColour)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*PixelColour)(nil)).Elem())
	runtime.RegisterType(reflect.TypeOf((*TraceFn)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*TraceFn)(nil)).Elem())
	runtime.RegisterType(reflect.TypeOf((*Vec)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*Vec)(nil)).Elem())
	runtime.RegisterType(reflect.TypeOf((*context.Context)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*context.Context)(nil)).Elem())
	runtime.RegisterType(reflect.TypeOf((*generateRaySDFn)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*generateRaySDFn)(nil)).Elem())
	runtime.RegisterType(reflect.TypeOf((*offsetrange.Restriction)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*offsetrange.Restriction)(nil)).Elem())
	runtime.RegisterType(reflect.TypeOf((*sdf.LockRTracker)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*sdf.LockRTracker)(nil)).Elem())
	reflectx.RegisterStructWrapper(reflect.TypeOf((*CombinePixelsFn)(nil)).Elem(), wrapMakerCombinePixelsFn)
	reflectx.RegisterStructWrapper(reflect.TypeOf((*generateRaySDFn)(nil)).Elem(), wrapMakerGenerateRaySDFn)
	reflectx.RegisterStructWrapper(reflect.TypeOf((*MakeImageFn)(nil)).Elem(), wrapMakerMakeImageFn)
	reflectx.RegisterStructWrapper(reflect.TypeOf((*TraceFn)(nil)).Elem(), wrapMakerTraceFn)
	reflectx.RegisterFunc(reflect.TypeOf((*func(context.Context, typex.T, func(*PixelColour) bool) (bool, error))(nil)).Elem(), funcMakerContext۰ContextTypex۰TIterPixelColourГBoolError)
	reflectx.RegisterFunc(reflect.TypeOf((*func(context.Context, Vec, Vec) Vec)(nil)).Elem(), funcMakerContext۰ContextVecVecГVec)
	reflectx.RegisterFunc(reflect.TypeOf((*func(ImageConfig, offsetrange.Restriction) float64)(nil)).Elem(), funcMakerImageConfigOffsetrange۰RestrictionГFloat64)
	reflectx.RegisterFunc(reflect.TypeOf((*func(ImageConfig, offsetrange.Restriction) []offsetrange.Restriction)(nil)).Elem(), funcMakerImageConfigOffsetrange۰RestrictionГSliceOfOffsetrange۰Restriction)
	reflectx.RegisterFunc(reflect.TypeOf((*func(ImageConfig) offsetrange.Restriction)(nil)).Elem(), funcMakerImageConfigГOffsetrange۰Restriction)
	reflectx.RegisterFunc(reflect.TypeOf((*func(offsetrange.Restriction) *sdf.LockRTracker)(nil)).Elem(), funcMakerOffsetrange۰RestrictionГᏘSdf۰LockRTracker)
	reflectx.RegisterFunc(reflect.TypeOf((*func(Pixel, Vec) PixelColour)(nil)).Elem(), funcMakerPixelVecГPixelColour)
	reflectx.RegisterFunc(reflect.TypeOf((*func(Pixel, Vec) (Pixel, Vec))(nil)).Elem(), funcMakerPixelVecГPixelVec)
	reflectx.RegisterFunc(reflect.TypeOf((*func(Vec) Vec)(nil)).Elem(), funcMakerVecГVec)
	reflectx.RegisterFunc(reflect.TypeOf((*func())(nil)).Elem(), funcMakerГ)
	reflectx.RegisterFunc(reflect.TypeOf((*func(*sdf.LockRTracker, ImageConfig, func(Pixel, Vec)) error)(nil)).Elem(), funcMakerᏘSdf۰LockRTrackerImageConfigEmitPixelVecГError)
	exec.RegisterEmitter(reflect.TypeOf((*func(Pixel, Vec))(nil)).Elem(), emitMakerPixelVec)
	exec.RegisterInput(reflect.TypeOf((*func(*PixelColour) bool)(nil)).Elem(), iterMakerPixelColour)
}

func wrapMakerCombinePixelsFn(fn interface{}) map[string]reflectx.Func {
	dfn := fn.(*CombinePixelsFn)
	return map[string]reflectx.Func{
		"AddInput":          reflectx.MakeFunc(func(a0 context.Context, a1 Vec, a2 Vec) Vec { return dfn.AddInput(a0, a1, a2) }),
		"ExtractOutput":     reflectx.MakeFunc(func(a0 Vec) Vec { return dfn.ExtractOutput(a0) }),
		"MergeAccumulators": reflectx.MakeFunc(func(a0 context.Context, a1 Vec, a2 Vec) Vec { return dfn.MergeAccumulators(a0, a1, a2) }),
	}
}

func wrapMakerGenerateRaySDFn(fn interface{}) map[string]reflectx.Func {
	dfn := fn.(*generateRaySDFn)
	return map[string]reflectx.Func{
		"CreateInitialRestriction": reflectx.MakeFunc(func(a0 ImageConfig) offsetrange.Restriction { return dfn.CreateInitialRestriction(a0) }),
		"CreateTracker":            reflectx.MakeFunc(func(a0 offsetrange.Restriction) *sdf.LockRTracker { return dfn.CreateTracker(a0) }),
		"ProcessElement": reflectx.MakeFunc(func(a0 *sdf.LockRTracker, a1 ImageConfig, a2 func(Pixel, Vec)) error {
			return dfn.ProcessElement(a0, a1, a2)
		}),
		"RestrictionSize": reflectx.MakeFunc(func(a0 ImageConfig, a1 offsetrange.Restriction) float64 { return dfn.RestrictionSize(a0, a1) }),
		"SplitRestriction": reflectx.MakeFunc(func(a0 ImageConfig, a1 offsetrange.Restriction) []offsetrange.Restriction {
			return dfn.SplitRestriction(a0, a1)
		}),
	}
}

func wrapMakerMakeImageFn(fn interface{}) map[string]reflectx.Func {
	dfn := fn.(*MakeImageFn)
	return map[string]reflectx.Func{
		"ProcessElement": reflectx.MakeFunc(func(a0 context.Context, a1 typex.T, a2 func(*PixelColour) bool) (bool, error) {
			return dfn.ProcessElement(a0, a1, a2)
		}),
	}
}

func wrapMakerTraceFn(fn interface{}) map[string]reflectx.Func {
	dfn := fn.(*TraceFn)
	return map[string]reflectx.Func{
		"ProcessElement": reflectx.MakeFunc(func(a0 Pixel, a1 Vec) (Pixel, Vec) { return dfn.ProcessElement(a0, a1) }),
		"Setup":          reflectx.MakeFunc(func() { dfn.Setup() }),
	}
}

type callerContext۰ContextTypex۰TIterPixelColourГBoolError struct {
	fn func(context.Context, typex.T, func(*PixelColour) bool) (bool, error)
}

func funcMakerContext۰ContextTypex۰TIterPixelColourГBoolError(fn interface{}) reflectx.Func {
	f := fn.(func(context.Context, typex.T, func(*PixelColour) bool) (bool, error))
	return &callerContext۰ContextTypex۰TIterPixelColourГBoolError{fn: f}
}

func (c *callerContext۰ContextTypex۰TIterPixelColourГBoolError) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerContext۰ContextTypex۰TIterPixelColourГBoolError) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerContext۰ContextTypex۰TIterPixelColourГBoolError) Call(args []interface{}) []interface{} {
	out0, out1 := c.fn(args[0].(context.Context), args[1].(typex.T), args[2].(func(*PixelColour) bool))
	return []interface{}{out0, out1}
}

func (c *callerContext۰ContextTypex۰TIterPixelColourГBoolError) Call3x2(arg0, arg1, arg2 interface{}) (interface{}, interface{}) {
	return c.fn(arg0.(context.Context), arg1.(typex.T), arg2.(func(*PixelColour) bool))
}

type callerContext۰ContextVecVecГVec struct {
	fn func(context.Context, Vec, Vec) Vec
}

func funcMakerContext۰ContextVecVecГVec(fn interface{}) reflectx.Func {
	f := fn.(func(context.Context, Vec, Vec) Vec)
	return &callerContext۰ContextVecVecГVec{fn: f}
}

func (c *callerContext۰ContextVecVecГVec) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerContext۰ContextVecVecГVec) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerContext۰ContextVecVecГVec) Call(args []interface{}) []interface{} {
	out0 := c.fn(args[0].(context.Context), args[1].(Vec), args[2].(Vec))
	return []interface{}{out0}
}

func (c *callerContext۰ContextVecVecГVec) Call3x1(arg0, arg1, arg2 interface{}) interface{} {
	return c.fn(arg0.(context.Context), arg1.(Vec), arg2.(Vec))
}

type callerImageConfigOffsetrange۰RestrictionГFloat64 struct {
	fn func(ImageConfig, offsetrange.Restriction) float64
}

func funcMakerImageConfigOffsetrange۰RestrictionГFloat64(fn interface{}) reflectx.Func {
	f := fn.(func(ImageConfig, offsetrange.Restriction) float64)
	return &callerImageConfigOffsetrange۰RestrictionГFloat64{fn: f}
}

func (c *callerImageConfigOffsetrange۰RestrictionГFloat64) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerImageConfigOffsetrange۰RestrictionГFloat64) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerImageConfigOffsetrange۰RestrictionГFloat64) Call(args []interface{}) []interface{} {
	out0 := c.fn(args[0].(ImageConfig), args[1].(offsetrange.Restriction))
	return []interface{}{out0}
}

func (c *callerImageConfigOffsetrange۰RestrictionГFloat64) Call2x1(arg0, arg1 interface{}) interface{} {
	return c.fn(arg0.(ImageConfig), arg1.(offsetrange.Restriction))
}

type callerImageConfigOffsetrange۰RestrictionГSliceOfOffsetrange۰Restriction struct {
	fn func(ImageConfig, offsetrange.Restriction) []offsetrange.Restriction
}

func funcMakerImageConfigOffsetrange۰RestrictionГSliceOfOffsetrange۰Restriction(fn interface{}) reflectx.Func {
	f := fn.(func(ImageConfig, offsetrange.Restriction) []offsetrange.Restriction)
	return &callerImageConfigOffsetrange۰RestrictionГSliceOfOffsetrange۰Restriction{fn: f}
}

func (c *callerImageConfigOffsetrange۰RestrictionГSliceOfOffsetrange۰Restriction) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerImageConfigOffsetrange۰RestrictionГSliceOfOffsetrange۰Restriction) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerImageConfigOffsetrange۰RestrictionГSliceOfOffsetrange۰Restriction) Call(args []interface{}) []interface{} {
	out0 := c.fn(args[0].(ImageConfig), args[1].(offsetrange.Restriction))
	return []interface{}{out0}
}

func (c *callerImageConfigOffsetrange۰RestrictionГSliceOfOffsetrange۰Restriction) Call2x1(arg0, arg1 interface{}) interface{} {
	return c.fn(arg0.(ImageConfig), arg1.(offsetrange.Restriction))
}

type callerImageConfigГOffsetrange۰Restriction struct {
	fn func(ImageConfig) offsetrange.Restriction
}

func funcMakerImageConfigГOffsetrange۰Restriction(fn interface{}) reflectx.Func {
	f := fn.(func(ImageConfig) offsetrange.Restriction)
	return &callerImageConfigГOffsetrange۰Restriction{fn: f}
}

func (c *callerImageConfigГOffsetrange۰Restriction) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerImageConfigГOffsetrange۰Restriction) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerImageConfigГOffsetrange۰Restriction) Call(args []interface{}) []interface{} {
	out0 := c.fn(args[0].(ImageConfig))
	return []interface{}{out0}
}

func (c *callerImageConfigГOffsetrange۰Restriction) Call1x1(arg0 interface{}) interface{} {
	return c.fn(arg0.(ImageConfig))
}

type callerOffsetrange۰RestrictionГᏘSdf۰LockRTracker struct {
	fn func(offsetrange.Restriction) *sdf.LockRTracker
}

func funcMakerOffsetrange۰RestrictionГᏘSdf۰LockRTracker(fn interface{}) reflectx.Func {
	f := fn.(func(offsetrange.Restriction) *sdf.LockRTracker)
	return &callerOffsetrange۰RestrictionГᏘSdf۰LockRTracker{fn: f}
}

func (c *callerOffsetrange۰RestrictionГᏘSdf۰LockRTracker) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerOffsetrange۰RestrictionГᏘSdf۰LockRTracker) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerOffsetrange۰RestrictionГᏘSdf۰LockRTracker) Call(args []interface{}) []interface{} {
	out0 := c.fn(args[0].(offsetrange.Restriction))
	return []interface{}{out0}
}

func (c *callerOffsetrange۰RestrictionГᏘSdf۰LockRTracker) Call1x1(arg0 interface{}) interface{} {
	return c.fn(arg0.(offsetrange.Restriction))
}

type callerPixelVecГPixelColour struct {
	fn func(Pixel, Vec) PixelColour
}

func funcMakerPixelVecГPixelColour(fn interface{}) reflectx.Func {
	f := fn.(func(Pixel, Vec) PixelColour)
	return &callerPixelVecГPixelColour{fn: f}
}

func (c *callerPixelVecГPixelColour) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerPixelVecГPixelColour) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerPixelVecГPixelColour) Call(args []interface{}) []interface{} {
	out0 := c.fn(args[0].(Pixel), args[1].(Vec))
	return []interface{}{out0}
}

func (c *callerPixelVecГPixelColour) Call2x1(arg0, arg1 interface{}) interface{} {
	return c.fn(arg0.(Pixel), arg1.(Vec))
}

type callerPixelVecГPixelVec struct {
	fn func(Pixel, Vec) (Pixel, Vec)
}

func funcMakerPixelVecГPixelVec(fn interface{}) reflectx.Func {
	f := fn.(func(Pixel, Vec) (Pixel, Vec))
	return &callerPixelVecГPixelVec{fn: f}
}

func (c *callerPixelVecГPixelVec) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerPixelVecГPixelVec) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerPixelVecГPixelVec) Call(args []interface{}) []interface{} {
	out0, out1 := c.fn(args[0].(Pixel), args[1].(Vec))
	return []interface{}{out0, out1}
}

func (c *callerPixelVecГPixelVec) Call2x2(arg0, arg1 interface{}) (interface{}, interface{}) {
	return c.fn(arg0.(Pixel), arg1.(Vec))
}

type callerVecГVec struct {
	fn func(Vec) Vec
}

func funcMakerVecГVec(fn interface{}) reflectx.Func {
	f := fn.(func(Vec) Vec)
	return &callerVecГVec{fn: f}
}

func (c *callerVecГVec) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerVecГVec) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerVecГVec) Call(args []interface{}) []interface{} {
	out0 := c.fn(args[0].(Vec))
	return []interface{}{out0}
}

func (c *callerVecГVec) Call1x1(arg0 interface{}) interface{} {
	return c.fn(arg0.(Vec))
}

type callerГ struct {
	fn func()
}

func funcMakerГ(fn interface{}) reflectx.Func {
	f := fn.(func())
	return &callerГ{fn: f}
}

func (c *callerГ) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerГ) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerГ) Call(args []interface{}) []interface{} {
	c.fn()
	return []interface{}{}
}

func (c *callerГ) Call0x0() {
	c.fn()
}

type callerᏘSdf۰LockRTrackerImageConfigEmitPixelVecГError struct {
	fn func(*sdf.LockRTracker, ImageConfig, func(Pixel, Vec)) error
}

func funcMakerᏘSdf۰LockRTrackerImageConfigEmitPixelVecГError(fn interface{}) reflectx.Func {
	f := fn.(func(*sdf.LockRTracker, ImageConfig, func(Pixel, Vec)) error)
	return &callerᏘSdf۰LockRTrackerImageConfigEmitPixelVecГError{fn: f}
}

func (c *callerᏘSdf۰LockRTrackerImageConfigEmitPixelVecГError) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerᏘSdf۰LockRTrackerImageConfigEmitPixelVecГError) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerᏘSdf۰LockRTrackerImageConfigEmitPixelVecГError) Call(args []interface{}) []interface{} {
	out0 := c.fn(args[0].(*sdf.LockRTracker), args[1].(ImageConfig), args[2].(func(Pixel, Vec)))
	return []interface{}{out0}
}

func (c *callerᏘSdf۰LockRTrackerImageConfigEmitPixelVecГError) Call3x1(arg0, arg1, arg2 interface{}) interface{} {
	return c.fn(arg0.(*sdf.LockRTracker), arg1.(ImageConfig), arg2.(func(Pixel, Vec)))
}

type emitNative struct {
	n  exec.ElementProcessor
	fn interface{}

	ctx   context.Context
	ws    []typex.Window
	et    typex.EventTime
	value exec.FullValue
}

func (e *emitNative) Init(ctx context.Context, ws []typex.Window, et typex.EventTime) error {
	e.ctx = ctx
	e.ws = ws
	e.et = et
	return nil
}

func (e *emitNative) Value() interface{} {
	return e.fn
}

func emitMakerPixelVec(n exec.ElementProcessor) exec.ReusableEmitter {
	ret := &emitNative{n: n}
	ret.fn = ret.invokePixelVec
	return ret
}

func (e *emitNative) invokePixelVec(key Pixel, val Vec) {
	e.value = exec.FullValue{Windows: e.ws, Timestamp: e.et, Elm: key, Elm2: val}
	if err := e.n.ProcessElement(e.ctx, &e.value); err != nil {
		panic(err)
	}
}

type iterNative struct {
	s  exec.ReStream
	fn interface{}

	// cur is the "current" stream, if any.
	cur exec.Stream
}

func (v *iterNative) Init() error {
	cur, err := v.s.Open()
	if err != nil {
		return err
	}
	v.cur = cur
	return nil
}

func (v *iterNative) Value() interface{} {
	return v.fn
}

func (v *iterNative) Reset() error {
	if err := v.cur.Close(); err != nil {
		return err
	}
	v.cur = nil
	return nil
}

func iterMakerPixelColour(s exec.ReStream) exec.ReusableInput {
	ret := &iterNative{s: s}
	ret.fn = ret.readPixelColour
	return ret
}

func (v *iterNative) readPixelColour(value *PixelColour) bool {
	elm, err := v.cur.Read()
	if err != nil {
		if err == io.EOF {
			return false
		}
		panic(fmt.Sprintf("broken stream: %v", err))
	}
	*value = elm.Elm.(PixelColour)
	return true
}

// DO NOT MODIFY: GENERATED CODE