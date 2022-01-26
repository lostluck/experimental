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
	"bytes"
	"io"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/ioutilx"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/encoding/prototext"
)

// leafCoders lists coder urns the runner knows how to manipulate.
// In particular, ones that won't be a problem to parse.
var leafCoders = map[string]struct{}{
	"beam:coder:bytes:v1":         {},
	"beam:coder:string_utf8:v1":   {},
	"beam:coder:length_prefix:v1": {},
	"beam:coder:varint:v1":        {},
	"beam:coder:double:v1":        {},
	"beam:coder:bool:v1":          {},
}

func isLeafCoder(c *pipepb.Coder) bool {
	_, ok := leafCoders[c.GetSpec().GetUrn()]
	return ok
}

func makeWindowedValueCoder(t *pipepb.PTransform, puts func(*pipepb.PTransform) map[string]string, pipeline *pipepb.Pipeline, coders map[string]*pipepb.Coder) (string, string) {
	if len(puts(t)) > 1 {
		logger.Fatalf("side inputs/outputs not implemented, can't build bundle for: %v", prototext.Format(t))
	}

	var pID string
	for _, c := range puts(t) {
		pID = c
	}
	col := pipeline.GetComponents().GetPcollections()[pID]
	cID := lpUnknownCoders(col.GetCoderId(), coders, pipeline.GetComponents().GetCoders())
	wcID := pipeline.GetComponents().GetWindowingStrategies()[col.GetWindowingStrategyId()].GetWindowCoderId()

	// The runner needs to be defensive, and tell the SDK to Length Prefix
	// any coders that it doesn't understand.
	// So here, we look at the coder and it's components, and produce
	// new coders that we know how to deal with.

	// Produce ID for the Windowed Value Coder
	wvcID := "cwv_" + pID
	wInC := &pipepb.Coder{
		Spec: &pipepb.FunctionSpec{
			Urn: "beam:coder:windowed_value:v1",
		},
		ComponentCoderIds: []string{cID, wcID},
	}
	// Populate the coders to send with the new windowed value coder.
	coders[wvcID] = wInC
	return pID, wvcID
}

// lpUnknownCoders takes a coder, and populates coders with any new coders
// coders that the runner needs to be safe, and speedy.
// It returns either the passed in coder id, or the new safe coder id.
func lpUnknownCoders(cID string, coders, base map[string]*pipepb.Coder) string {
	// First check if we've already added the LP version of this coder to coders already.
	lpcID := cID + "_lp"
	// Check if we've done this one before.
	if _, ok := coders[lpcID]; ok {
		return lpcID
	}
	// All coders in the coders map have been processed.
	if _, ok := coders[cID]; ok {
		return cID
	}
	// Look up the cannon location.
	c, ok := base[cID]
	if !ok {
		// We messed up somewhere.
		logger.Fatal("unknown coder id:", cID)
	}
	// Add the original coder to the coders map.
	coders[cID] = c
	// If we don't know this coder, and it has no sub components,
	// we must LP it, and we return the LP'd version.
	if len(c.GetComponentCoderIds()) == 0 && !isLeafCoder(c) {
		lpc := &pipepb.Coder{
			Spec: &pipepb.FunctionSpec{
				Urn: "beam:coder:length_prefix:v1",
			},
			ComponentCoderIds: []string{cID},
		}
		coders[lpcID] = lpc
		return lpcID
	}
	var needNewComposite bool
	var comps []string
	for _, cc := range c.GetComponentCoderIds() {
		rcc := lpUnknownCoders(cc, coders, base)
		if cc != rcc {
			needNewComposite = true
		}
		comps = append(comps, rcc)
	}
	if needNewComposite {
		lpc := &pipepb.Coder{
			Spec:              c.GetSpec(),
			ComponentCoderIds: comps,
		}
		coders[lpcID] = lpc
		return lpcID
	}
	return cID
}

// reconcileCoders, has coders is primed with initial coders.
func reconcileCoders(coders, base map[string]*pipepb.Coder) {
	for {
		var comps []string
		for _, c := range coders {
			for _, ccid := range c.GetComponentCoderIds() {
				if _, ok := coders[ccid]; !ok {
					// We don't have the coder yet, so in we go.
					comps = append(comps, ccid)
				}
			}
		}
		if len(comps) == 0 {
			return
		}
		for _, ccid := range comps {
			c, ok := base[ccid]
			if !ok {
				logger.Fatalf("unknown coder id during reconciliation")
			}
			coders[ccid] = c
		}
	}
}

func kvcoder(pipeline *pipepb.Pipeline, tid string) *pipepb.Coder {
	comp := pipeline.GetComponents()
	t := comp.GetTransforms()[tid]
	var inputPColID string
	for _, pcolID := range t.GetInputs() {
		inputPColID = pcolID
	}
	pcol := comp.GetPcollections()[inputPColID]
	return comp.GetCoders()[pcol.GetCoderId()]
}

// pullDecoder return a function that will extract the bytes
// for the associated coder.
func pullDecoder(c *pipepb.Coder) func(io.Reader) []byte {
	urn := c.GetSpec().GetUrn()
	switch urn {
	// Anything length prefixed can be treated as opaque.
	case "beam:coder:bytes:v1", "beam:coder:string_utf8:v1", "beam:coder:length_prefix:v1":
		return func(r io.Reader) []byte {
			var buf bytes.Buffer
			tr := io.TeeReader(r, &buf)
			l, _ := coder.DecodeVarInt(tr)
			ioutilx.ReadN(tr, int(l))
			return buf.Bytes()
		}
	case "beam:coder:varint:v1":
		return func(r io.Reader) []byte {
			var buf bytes.Buffer
			tr := io.TeeReader(r, &buf)
			coder.DecodeVarInt(tr)
			return buf.Bytes()
		}
	case "beam:coder:bool:v1":
		return func(r io.Reader) []byte {
			if v, _ := coder.DecodeBool(r); v {
				return []byte{1}
			}
			return []byte{0}
		}
	case "beam:coder:double:v1":
		return func(r io.Reader) []byte {
			var buf bytes.Buffer
			tr := io.TeeReader(r, &buf)
			coder.DecodeDouble(tr)
			return buf.Bytes()
		}
	case "beam:coder:row:v1":
		logger.Fatalf("Runner forgot to LP this Row Coder.")
	default:
		logger.Fatalf("unknown coder urn key: %v", urn)
	}
	return nil
}
