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
	"container/heap"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
)

func TestElementHeap(t *testing.T) {
	elements := elementHeap{
		element{timestamp: mtime.EndOfGlobalWindowTime},
		element{timestamp: mtime.MaxTimestamp},
		element{timestamp: 3},
		element{timestamp: mtime.MinTimestamp},
		element{timestamp: 2},
		element{timestamp: mtime.ZeroTimestamp},
		element{timestamp: 1},
	}
	heap.Init(&elements)

	if got, want := elements.Len(), len(elements); got != want {
		t.Errorf("elements.Len() = %v, want %v", got, want)
	}
	if got, want := elements[0].timestamp, mtime.MinTimestamp; got != want {
		t.Errorf("elements[0].timestamp = %v, want %v", got, want)
	}

	wanted := []mtime.Time{mtime.MinTimestamp, mtime.ZeroTimestamp, 1, 2, 3, mtime.EndOfGlobalWindowTime, mtime.MaxTimestamp}
	for i, want := range wanted {
		if got := heap.Pop(&elements).(element).timestamp; got != want {
			t.Errorf("[%d] heap.Pop(&elements).(element).timestamp = %v, want %v", i, got, want)
		}
	}
}

// TODO: add tests for updating watermarks
// TODO: add tests for persisting bundles
// TODO: add tests for producing bundles.
