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

package internal

import (
	"bytes"
	"io"
	"reflect"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/encoding/prototext"
)

// This file retains the logic for the pardo handler

// RunnerCharacteristic holds the configuration for Runner based transforms,
// such as Impulse, GBKs, Flattens.
type RunnerCharacteristic struct {
	SDKFlatten bool // Sets whether we should force an SDK side flatten.
	SDKGBK     bool // Sets whether the GBK should be handled by the SDK, if possible.
}

func Runner(config any) *runner {
	return &runner{config: config.(RunnerCharacteristic)}
}

// runner represents an instance of the runner transform handler.
type runner struct {
	config RunnerCharacteristic
}

// ConfigURN returns the name for combine in the configuration file.
func (*runner) ConfigURN() string {
	return "runner"
}

func (*runner) ConfigCharacteristic() reflect.Type {
	return reflect.TypeOf((*RunnerCharacteristic)(nil)).Elem()
}

var _ transformExecuter = (*runner)(nil)

func (*runner) ExecuteUrns() []string {
	return []string{urnTransformImpulse, urnTransformFlatten, urnTransformGBK}
}

// ExecuteWith returns what environment the
func (h *runner) ExecuteWith(t *pipepb.PTransform) string {
	urn := t.GetSpec().GetUrn()
	if urn == urnTransformFlatten && !h.config.SDKFlatten {
		return ""
	}
	return t.GetEnvironmentId()
}

// ExecTransform handles special processing with respect to runner specific transforms
func (h *runner) ExecuteTransform(tid string, t *pipepb.PTransform, comps *pipepb.Components, parents map[string]*bundle) *bundle {
	urn := t.GetSpec().GetUrn()
	var data [][]byte
	switch urn {
	case urnTransformImpulse:
		// These will be subbed out by the pardo stage.
		data = append(data, impulseBytes())
	case urnTransformFlatten:
		// Extract the data from the parents.
		// TODO extract from the correct output.
		for _, pb := range parents {
			for _, ds := range pb.DataReceived {
				data = append(data, ds...)
			}
		}
	case urnTransformGBK:
		var parent *bundle
		// Extract the one parent.
		for _, pb := range parents {
			parent = pb
		}
		dataParentID, _ := sourceIDs(parent)

		ws := windowingStrategy(comps, tid)
		kvc := kvcoder(comps, tid)

		// TODO, support other window coders.
		wc := exec.MakeWindowDecoder(coder.NewGlobalWindow())
		// TODO assert this is a KV. It's probably fine, but we should fail anyway.
		coders := map[string]*pipepb.Coder{}
		kcID := lpUnknownCoders(kvc.GetComponentCoderIds()[0], coders, comps.GetCoders())
		ecID := lpUnknownCoders(kvc.GetComponentCoderIds()[1], coders, comps.GetCoders())
		reconcileCoders(coders, comps.GetCoders())

		kc := coders[kcID]
		ec := coders[ecID]

		data = append(data, gbkBytes(ws, wc, kc, ec, parent.DataReceived[dataParentID], coders))
	default:
		logger.Fatalf("unimplemented runner transform[%v]", urn)
	}

	// To avoid conflicts with these single transform
	// bundles, we suffix the transform IDs.
	var localID string
	for key, _ := range t.GetOutputs() {
		localID = key
	}

	if localID == "" {
		V(1).Fatalf("bad transform: %v", prototext.Format(t))
	}

	dataID := tid + "_" + localID // The ID from which the consumer will read from.
	b := &bundle{
		InputTransformID: dataID,
		DataReceived: map[string][][]byte{
			dataID: data,
		},
	}
	return b
}

var (
	impOnce  sync.Once
	impBytes []byte
)

func impulseBytes() []byte {
	impOnce.Do(func() {
		var buf bytes.Buffer
		byt, _ := exec.EncodeElement(exec.MakeElementEncoder(coder.NewBytes()), []byte("lostluck"))

		exec.EncodeWindowedValueHeader(
			exec.MakeWindowEncoder(coder.NewGlobalWindow()),
			window.SingleGlobalWindow,
			mtime.Now(),
			typex.NoFiringPane(),
			&buf,
		)
		buf.Write(byt)
		impBytes = buf.Bytes()
	})
	return impBytes
}

// windowingStrategy sources the transform's windowing strategy from a single parallel input.
func windowingStrategy(comps *pipepb.Components, tid string) *pipepb.WindowingStrategy {
	t := comps.GetTransforms()[tid]
	var inputPColID string
	for _, pcolID := range t.GetInputs() {
		inputPColID = pcolID
	}
	pcol := comps.GetPcollections()[inputPColID]
	return comps.GetWindowingStrategies()[pcol.GetWindowingStrategyId()]
}

// gbkBytes re-encodes gbk inputs in a gbk result.
func gbkBytes(ws *pipepb.WindowingStrategy, wc exec.WindowDecoder, kc, vc *pipepb.Coder, toAggregate [][]byte, coders map[string]*pipepb.Coder) []byte {
	var outputTime func(typex.Window, mtime.Time) mtime.Time
	switch ws.GetOutputTime() {
	case pipepb.OutputTime_END_OF_WINDOW:
		outputTime = func(w typex.Window, et mtime.Time) mtime.Time {
			return w.MaxTimestamp()
		}
	default:
		logger.Fatalf("unsupported OutputTime behavior: %v", ws.GetOutputTime())
	}

	type keyTime struct {
		key    []byte
		w      typex.Window
		time   mtime.Time
		values [][]byte
	}
	// Map windows to a map of keys to a map of keys to time.
	// We ultimately emit the window, the key, the time, and the iterable of elements,
	// all contained in the final value.
	windows := map[typex.Window]map[string]keyTime{}

	kd := pullDecoder(kc, coders)
	vd := pullDecoder(vc, coders)

	// Right, need to get the key coder, and the element coder.
	// Cus I'll need to pull out anything the runner knows how to deal with.
	// And repeat.
	for _, data := range toAggregate {
		// Parse out each element's data, and repeat.
		buf := bytes.NewBuffer(data)
		for {
			ws, tm, _, err := exec.DecodeWindowedValueHeader(wc, buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				logger.Fatalf("can't decode windowed value header with %v: %v", wc, err)
			}

			keyByt := kd(buf)
			key := string(keyByt)
			value := vd(buf)
			for _, w := range ws {
				ft := outputTime(w, tm)
				wk, ok := windows[w]
				if !ok {
					wk = make(map[string]keyTime)
					windows[w] = wk
				}
				kt := wk[key]
				kt.time = ft
				kt.key = keyByt
				kt.w = w
				kt.values = append(kt.values, value)
				wk[key] = kt
			}
		}
	}

	// Everything's aggregated!
	// Time to turn things into a windowed KV<K, Iterable<V>>

	var buf bytes.Buffer
	for _, w := range windows {
		for _, kt := range w {
			exec.EncodeWindowedValueHeader(
				exec.MakeWindowEncoder(coder.NewGlobalWindow()),
				[]typex.Window{kt.w},
				kt.time,
				typex.NoFiringPane(),
				&buf,
			)
			buf.Write(kt.key)
			coder.EncodeInt32(int32(len(kt.values)), &buf)
			for _, value := range kt.values {
				buf.Write(value)
			}
		}
	}
	return buf.Bytes()
}
