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
	"sync/atomic"
)

// The logger for this runner.
var logger = &runnerLog{
	disabled: 1,
	logger:   log.New(os.Stderr, "[local] ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile),
}

type runnerLog struct {
	disabled int32 // if > level, disable log output.
	logger   logIface
}

// SetMaxV sets the maximum V log level that will be printed.
func SetMaxV(level int32) {
	atomic.StoreInt32(&logger.disabled, level)
}

type logIface interface {
	Printf(format string, v ...any)
	Println(v ...any)
	Fatalf(format string, v ...any)
}

type noopLogger struct{}

func (noopLogger) Printf(format string, v ...any) {}
func (noopLogger) Println(v ...any)               {}
func (noopLogger) Fatalf(format string, v ...any) {}

// V indicates the level of the log.
//
// Higher levels indicate finer granularity of detail.
//
// As a rule of thumb.
//  0 -> Should always be printed
//  1 -> Job Specific
//  2 -> Bundle Specific, Unimplemented features
//  3 -> Element Specific, Fine Grain Debug
func V(level int32) *runnerLog {
	if level > atomic.LoadInt32(&logger.disabled) {
		return nil
	}
	return logger
}

// Logf calls l.Output to print to the logger. Arguments are handled in the manner of fmt.Println.
func (r *runnerLog) Logf(format string, v ...any) {
	if r == nil {
		return
	}
	r.logger.Printf(format, v...)
}

// Log calls l.Output to print to the logger. Arguments are handled in the manner of fmt.Log.
func (r *runnerLog) Log(v ...any) {
	if r == nil {
		return
	}
	r.logger.Println(v...)
}

// Fatalf is equivalent to l.Logf() followed by a call to os.Exit(1)
func (r *runnerLog) Fatalf(format string, v ...any) {
	if r == nil {
		return
	}
	r.logger.Fatalf(format, v...)
}
