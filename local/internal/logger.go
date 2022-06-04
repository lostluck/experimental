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
)

// The logger for the local runner.
var logger = &runnerLog{
	//log.New(os.Stderr, "[local] ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile),
}

type runnerLog struct {
	logger *log.Logger
}

func (r *runnerLog) Printf(format string, v ...any) {
	if r.logger == nil {
		return
	}
	r.logger.Printf(format, v...)
}

func (r *runnerLog) Println(v ...any) {
	if r.logger == nil {
		return
	}
	r.logger.Println(v...)
}

func (r *runnerLog) Print(v ...any) {
	if r.logger == nil {
		return
	}
	r.logger.Print(v...)
}

func (r *runnerLog) Fatalf(format string, v ...any) {
	if r.logger == nil {
		return
	}
	r.logger.Fatalf(format, v...)
}
func (r *runnerLog) Fatal(v ...any) {
	if r.logger == nil {
		return
	}
	r.logger.Fatal(v...)
}