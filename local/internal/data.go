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

type dataService struct {
	// TODO actually quick process the data to windows here as well.
	rawData map[string][][]byte
}

// WriteData adds data to a given global collectionID.
func (d *dataService) WriteData(colID string, data []byte) {
	d.rawData[colID] = append(d.rawData[colID], data)
}

func (d *dataService) GetData(colID string) [][]byte {
	return d.rawData[colID]
}
