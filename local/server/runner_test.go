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

package server

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/google/go-cmp/cmp"
)

// execute will startup the server, and this will be maintained for the life of
// all the tests.
func execute(ctx context.Context, p *beam.Pipeline) (beam.PipelineResult, error) {
	return universal.Execute(ctx, p)
}

func init() {
	// Not actually being used, but explicitly registering
	// will avoid accidentally using a different runner for
	// the tests if I change things later.
	beam.RegisterRunner("testlocal", execute)
	beam.RegisterFunction(dofn1)
	beam.RegisterFunction(dofn2)
	beam.RegisterFunction(dofn3)
	beam.RegisterFunction(dofnKV)
	beam.RegisterFunction(dofnKV2)
	beam.RegisterFunction(dofnGBK)
	beam.RegisterFunction(dofnGBK2)
	beam.RegisterType(reflect.TypeOf((*int64Check)(nil)))
	beam.RegisterType(reflect.TypeOf((*stringCheck)(nil)))

	beam.RegisterType(reflect.TypeOf((*testRow)(nil)))
	beam.RegisterFunction(dofnKV3)
	beam.RegisterFunction(dofnGBK3)
}

func dofn1(imp []byte, emit func(int64)) {
	logger.Print("dofn1 impulse:", string(imp))
	emit(1)
	emit(2)
	emit(3)
}

// int64Check validates that within a single bundle,
// we received the expected int64 values.
// Returns ints, but they are unused, because we haven't
// handled ParDo0's yet.
type int64Check struct {
	Name string
	Want []int
	got  []int
}

func (fn *int64Check) ProcessElement(v int64, _ func(int64)) {
	fn.got = append(fn.got, int(v))
}

func (fn *int64Check) FinishBundle(_ func(int64)) error {
	sort.Ints(fn.got)
	sort.Ints(fn.Want)
	if d := cmp.Diff(fn.Want, fn.got); d != "" {
		return fmt.Errorf("int64Check[%v] (-want, +got): %v", fn.Name, d)
	}
	return nil
}

// int64Check validates that within a single bundle,
// we received the expected int64 values.
// Returns ints, but they are unused, because we haven't
// handled ParDo0's yet.
type stringCheck struct {
	Name string
	Want []string
	got  []string
}

func (fn *stringCheck) ProcessElement(v string, _ func(string)) {
	fn.got = append(fn.got, v)
}

func (fn *stringCheck) FinishBundle(_ func(string)) error {
	sort.Strings(fn.got)
	sort.Strings(fn.Want)
	if d := cmp.Diff(fn.Want, fn.got); d != "" {
		return fmt.Errorf("stringCheck[%v] (-want, +got): %v", fn.Name, d)
	}
	return nil
}

func dofn2(v int64, emit func(int64)) {
	logger.Printf("dofn2(%v)", v)
	emit(v + 1)
}

func dofn3(v int64, emit func(int64)) {
	logger.Printf("dofn2(%v)", v)
	emit(v + 1)
}

func dofnKV(imp []byte, emit func(string, int64)) {
	emit("a", 1)
	emit("b", 2)
	emit("a", 3)
	emit("b", 4)
	emit("a", 5)
	emit("b", 6)
}

func dofnKV2(imp []byte, emit func(int64, string)) {
	emit(1, "a")
	emit(2, "b")
	emit(1, "a")
	emit(2, "b")
	emit(1, "a")
	emit(2, "b")
}

func dofnGBK(k string, vs func(*int64) bool, emit func(int64)) {
	var v, sum int64
	for vs(&v) {
		sum += v
	}
	emit(sum)
}

func dofnGBK2(k int64, vs func(*string) bool, emit func(string)) {
	var v, sum string
	for vs(&v) {
		sum += v
	}
	emit(sum)
}

type testRow struct {
	A string
	B int64
}

func dofnKV3(imp []byte, emit func(testRow, testRow)) {
	emit(testRow{"a", 1}, testRow{"a", 1})
}

func dofnGBK3(k testRow, vs func(*testRow) bool, emit func(string)) {
	var v testRow
	vs(&v)
	emit(fmt.Sprintf("%v: %v", k, v))
}

func TestRunner(t *testing.T) {
	if *jobopts.Endpoint == "" {
		s := NewServer(0)
		*jobopts.Endpoint = s.Endpoint()
		go s.Serve()
		t.Cleanup(func() { s.Stop() })
	}
	if !jobopts.IsLoopback() {
		*jobopts.EnvironmentType = "loopback"
	}
	// Since we force loopback, avoid cross-compilation.
	f, err := os.CreateTemp("", "dummy")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	*jobopts.WorkerBinary = f.Name()
	// TODO: Explicit DoFn Failure case.
	t.Run("simple", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col := beam.ParDo(s, dofn1, imp)
		beam.ParDo(s, &int64Check{
			Name: "simple",
			Want: []int{1, 2, 3},
		}, col)

		if _, err := execute(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("sequence", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		beam.Seq(s, imp, dofn1, dofn2, dofn2, dofn2, &int64Check{Name: "sequence", Want: []int{4, 5, 6}})
		if _, err := execute(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("gbk", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col := beam.ParDo(s, dofnKV, imp)
		gbk := beam.GroupByKey(s, col)
		beam.Seq(s, gbk, dofnGBK, &int64Check{Name: "gbk", Want: []int{9, 12}})
		if _, err := execute(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("gbk2", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col := beam.ParDo(s, dofnKV2, imp)
		gbk := beam.GroupByKey(s, col)
		beam.Seq(s, gbk, dofnGBK2, &stringCheck{Name: "gbk2", Want: []string{"aaa", "bbb"}})
		if _, err := execute(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("gbk3", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col := beam.ParDo(s, dofnKV3, imp)
		gbk := beam.GroupByKey(s, col)
		beam.Seq(s, gbk, dofnGBK3, &stringCheck{Name: "gbk3", Want: []string{"{a 1}: {a 1}"}})
		if _, err := execute(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMain(m *testing.M) {
	ptest.MainWithDefault(m, "testlocal")
}
