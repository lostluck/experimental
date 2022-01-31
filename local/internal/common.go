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
	"log"
	"os"
	"strings"
)

// The logger for the local runner.
var logger = log.New(os.Stderr, "[local] ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)

// trimRightTo trims the right of the string until the
// seperator run is reached. If it isn't reached, returns
// the original string.
func trimRightTo(s string, sep rune) string {
	var trimmed bool
	out := strings.TrimRightFunc(s, func(r rune) bool {
		if trimmed {
			return false
		}
		if r == sep {
			trimmed = true
		}
		return true
	})
	if !trimmed {
		return s
	}
	return out
}
