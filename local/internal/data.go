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

type tentativeData struct {
	raw map[string][][]byte
}

// WriteData adds data to a given global collectionID.
func (d *tentativeData) WriteData(colID string, data []byte) {
	if d.raw == nil {
		d.raw = map[string][][]byte{}
	}
	d.raw[colID] = append(d.raw[colID], data)
}

type dataService struct {
	// TODO actually quick process the data to windows here as well.
	raw map[string]map[int][][]byte
}

// Commit tentative data to the datastore.
func (d *dataService) Commit(gen int, tent tentativeData) {
	if d.raw == nil {
		d.raw = map[string]map[int][][]byte{}
	}
	for colID, data := range tent.raw {
		c, ok := d.raw[colID]
		if !ok {
			c = map[int][][]byte{}
			d.raw[colID] = c
		}
		c[gen] = append(c[gen], data...)
	}
}

// WriteData adds data to a given global collectionID.
// Currently only used directly by runner based transforms.
func (d *dataService) WriteData(colID string, gen int, data []byte) {
	if d.raw == nil {
		d.raw = map[string]map[int][][]byte{}
	}
	c, ok := d.raw[colID]
	if !ok {
		c = map[int][][]byte{}
		d.raw[colID] = c
	}
	c[gen] = append(c[gen], data)
}

func (d *dataService) GetData(colID string, gen int) [][]byte {
	return d.raw[colID][gen]
}

// Hack for GBK and Side Inputs until watermarks are sorted out.
func (d *dataService) GetAllData(colID string) [][]byte {
	var ret [][]byte
	for gen, data := range d.raw[colID] {
		V(3).Logf("getting All data for %v, gen %v", colID, gen)
		ret = append(ret, data...)
	}
	return ret
}
