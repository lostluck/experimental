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
	"fmt"
	"os"
	"sync/atomic"

	"golang.org/x/exp/slog"
)

// The logger for this runner.
var logger = &runnerLog{
	disabled: 2,
}

type runnerLog struct {
	disabled int32 // if > level, disable log output.
}

// SetMaxV sets the maximum V log level that will be printed.
func SetMaxV(level int32) {
	atomic.StoreInt32(&logger.disabled, level)
}

// V indicates the level of the log.
//
// Higher levels indicate finer granularity of detail.
//
// As a rule of thumb.
//
//	0 -> Should always be printed
//	1 -> Job Specific
//	2 -> Bundle Specific, Unimplemented features
//	3 -> Element Specific, Fine Grain Debug
func V(level int32) *runnerLog {
	if level > atomic.LoadInt32(&logger.disabled) {
		return nil
	}
	return logger
}

func (r *runnerLog) Logf(format string, v ...any) {
	if r == nil {
		return
	}
	slog.Default().LogDepth(2, slog.LevelInfo, fmt.Sprintf(format, v...))
}

func (r *runnerLog) Log(v ...any) {
	if r == nil {
		return
	}
	slog.Default().LogDepth(2, slog.LevelInfo, fmt.Sprint(v...))
}

// Fatalf is equivalent to l.Logf() followed by a call to os.Exit(1)
func (r *runnerLog) Fatalf(format string, v ...any) {
	if r == nil {
		return
	}
	slog.Default().LogDepth(2, slog.LevelError, fmt.Sprintf(format, v...))
	os.Exit(1)
}
