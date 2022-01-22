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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

// execute will startup the server, and this will be maintained for the life of
// all the tests.
func execute(ctx context.Context, p *beam.Pipeline) (beam.PipelineResult, error) {
	return universal.Execute(ctx, p)
}

func init() {
	beam.RegisterRunner("testlocal", execute)
	beam.RegisterFunction(dofn1)
	beam.RegisterFunction(dofn2)
}

func dofn1(imp []byte, emit func(int64)) {
	logger.Print("dofn1 impulse:", string(imp))
	emit(1)
	emit(2)
	emit(3)
}

func dofn2(v int64, emit func(int64)) {
	logger.Printf("dofn2(%v)", v)
	emit(v + 1)
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
	t.Run("simple", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		beam.ParDo(s, dofn1, imp)

		if _, err := execute(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("sequence", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		beam.Seq(s, imp, dofn1, dofn2, dofn2, dofn2)
		if _, err := execute(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMain(m *testing.M) {
	ptest.MainWithDefault(m, "testlocal")
}
