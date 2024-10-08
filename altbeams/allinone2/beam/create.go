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

// TODO(lostluck): Turn this into an SDF.
// TODO(lostluck): Properly deal with serialization instead of leaning on Javascript's coding.
// Consider compression perhaps?
type createFn[E Element] struct {
	Values []E

	Out Output[E]
}

func (fn *createFn[E]) ProcessBundle(dfc *DFC[[]byte]) error {
	return dfc.Process(func(ec ElmC, _ []byte) error {
		for _, v := range fn.Values {
			fn.Out.Emit(ec, v)
		}
		return nil
	})
}

func newCreateFn[E Element](values ...E) *createFn[E] {
	return &createFn[E]{
		Values: values,
	}
}

// Create static output for a transform, for simple initialization, and testing.
//
// Values in create are serialzied as part of the Beam Pipeline graph, so it is
// not suitable for large elements, or large numbers of elements.
func Create[E Element](s *Scope, values ...E) Output[E] {
	return ParDo(
		s,
		Impulse(s),
		newCreateFn(values...),
		Name("beam.Create"),
	).Out
}
