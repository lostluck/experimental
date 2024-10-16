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

package beam

import (
	"context"
	"reflect"
	"testing"

	"github.com/go-json-experiment/json"
	"github.com/google/go-cmp/cmp"
)

func TestCreate(t *testing.T) {
	// TODO, more types, actual validation of the types.
	pr, err := LaunchAndWait(context.TODO(), func(s *Scope) error {
		count := Create(s, 1, 2, 3, 4, 5)
		ParDo(s, count, &DiscardFn[int]{}, Name("discarded"))
		return nil
	}, pipeName(t))
	if err != nil {
		t.Error(err)
	}
	if got, want := int(pr.Counters["discarded.Processed"]), 5; got != want {
		t.Fatalf("processed didn't match bench number: got %v want %v", got, want)
	}
}

func TestMarshal(t *testing.T) {
	dofn := newCreateFn("one", "two", "three")
	wrap := dofnWrap{
		TypeName: reflect.TypeOf(dofn).Elem().Name(),
		DoFn:     dofn,
	}
	wrappedPayload, err := json.Marshal(&wrap, json.DefaultOptionsV2(), jsonDoFnMarshallers())
	if err != nil {
		t.Errorf("error wrapping payload %v", wrap)
	}
	t.Log(string(wrappedPayload))
	var wrapGot dofnWrap
	if err := json.Unmarshal(wrappedPayload, &wrapGot, json.DefaultOptionsV2(), jsonDoFnUnmarshallers(map[string]reflect.Type{wrap.TypeName: reflect.TypeOf(dofn).Elem()}, "name")); err != nil {
		t.Errorf("error unwrapping payload %v", wrap)
	}
	t.Logf("got %#v", wrapGot)
	if wrapGot.TypeName != wrap.TypeName {
		t.Error("mismatch type names")
	}
	if reflect.TypeOf(wrapGot.DoFn) != reflect.TypeOf(wrap.DoFn) {
		t.Error("mismatch dofn types")
	}
	got := wrapGot.DoFn.(*createFn[string])
	if d := cmp.Diff(dofn.Values, got.Values); d != "" {
		t.Error("did not roundtrip: diff (-want, +got)\n", d)
	}
	if got.Values[1] != "two" {
		t.Error("unexpected data", got.Values)
	}
}
