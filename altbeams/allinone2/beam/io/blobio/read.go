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

package blobio

import (
	"fmt"
	"strings"

	"github.com/lostluck/experimental/altbeams/allinone2/beam"
)

// directoryTreatment controls how paths to directories are treated when reading matches.
type directoryTreatment int

const (
	// directorySkip skips directories.
	directorySkip directoryTreatment = iota
	// directoryDisallow disallows directories.
	directoryDisallow
)

type readOption struct {
	Compression        compressionType
	DirectoryTreatment directoryTreatment
}

// ReadOptionFn is a function that can be passed to ReadMatches to configure options for
// reading files.
type ReadOptionFn func(*readOption)

// ReadAutoCompression specifies that the compression type of files should be auto-detected.
func ReadAutoCompression() ReadOptionFn {
	return func(o *readOption) {
		o.Compression = compressionAuto
	}
}

// ReadGzip specifies that files have been compressed using gzip.
func ReadGzip() ReadOptionFn {
	return func(o *readOption) {
		o.Compression = compressionGzip
	}
}

// ReadUncompressed specifies that files have not been compressed.
func ReadUncompressed() ReadOptionFn {
	return func(o *readOption) {
		o.Compression = compressionUncompressed
	}
}

// ReadDirectorySkip specifies that directories are skipped.
func ReadDirectorySkip() ReadOptionFn {
	return func(o *readOption) {
		o.DirectoryTreatment = directorySkip
	}
}

// ReadDirectoryDisallow specifies that directories are not allowed.
func ReadDirectoryDisallow() ReadOptionFn {
	return func(o *readOption) {
		o.DirectoryTreatment = directoryDisallow
	}
}

// ReadMatches accepts the result of MatchFiles, MatchAll or MatchContinuously as a
// beam.Output[BlobMetadata] and converts it to a beam.Output[ReadableBlob]. The ReadableBlob can be
// used to retrieve blob metadata, open the blob for reading or read the entire blob into memory.
// ReadMatches accepts a variadic number of ReadOptionFn that can be used to configure the
// compression type of the blobs and treatment of directories. By default, the compression type is
// determined by the blob extension and directories are skipped.
func ReadMatches(s *beam.Scope, col beam.Output[BlobMetadata], opts ...ReadOptionFn) beam.Output[ReadableBlob] {
	option := &readOption{
		Compression:        compressionAuto,
		DirectoryTreatment: directorySkip,
	}

	for _, opt := range opts {
		opt(option)
	}

	return beam.ParDo(s, col, newReadFn(option), beam.Name("blobio.ReadMatches")).Output
}

type readFn struct {
	Compression        compressionType
	DirectoryTreatment directoryTreatment

	Output beam.Output[ReadableBlob]
}

func newReadFn(option *readOption) *readFn {
	return &readFn{
		Compression:        option.Compression,
		DirectoryTreatment: option.DirectoryTreatment,
	}
}

func (fn *readFn) ProcessBundle(dfc *beam.DFC[BlobMetadata]) error {
	return dfc.Process(func(ec beam.ElmC, metadata BlobMetadata) error {
		if isDirectory(metadata.Key) {
			if fn.DirectoryTreatment == directoryDisallow {
				return fmt.Errorf("path to directory not allowed: bucket=%q, key=%q",
					metadata.Bucket, metadata.Key)
			}
			return nil
		}

		file := ReadableBlob{
			Metadata:    metadata,
			Compression: fn.Compression,
		}
		fn.Output.Emit(ec, file)
		return nil
	})
}

func isDirectory(path string) bool {
	if strings.HasSuffix(path, "/") || strings.HasSuffix(path, "\\") {
		return true
	}
	return false
}
