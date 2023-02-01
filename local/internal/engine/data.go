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

package engine

// data.go homes the data management handler for all data that comes and goes
// from the SDKs. If there were a place to add reliability & restart persistence
// it would be here.

// The dataService needs to support outputs from different bundles, which means
// that we can't put the ouput data from a given bundle into the main service
// until that bundle has terminated successfully.
//
// This implies we can have a "tentativeData" structure that is used by the
// bundle, and the worker to store the data. Once the wait is complete,
// all the output data can be committed at once.
//
// This mechanism will also allow for bundle retries to be added later, so
// tentative outputs can be disgarded.

type TentativeData struct {
	Raw map[string][][]byte
}

// WriteData adds data to a given global collectionID.
func (d *TentativeData) WriteData(colID string, data []byte) {
	if d.Raw == nil {
		d.Raw = map[string][][]byte{}
	}
	d.Raw[colID] = append(d.Raw[colID], data)
}
