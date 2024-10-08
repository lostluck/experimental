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
	"testing"
)

func TestCreate(t *testing.T) {
	// TODO, more types, actual validation of the types.
	pr, err := Run(context.TODO(), func(s *Scope) error {
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
