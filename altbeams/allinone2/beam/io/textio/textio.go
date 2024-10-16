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

// Package textio contains transforms for reading and writing text blobs.
package textio

import (
	"bufio"
	"io"
	"iter"
	"maps"
	"os"
	"strings"

	"github.com/lostluck/experimental/altbeams/allinone2/beam"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/io/blobio"
	"github.com/pkg/errors"

	"gocloud.dev/blob"
)

type readOption struct {
	FileOpts []blobio.ReadOptionFn
}

// ReadOptionFn is a function that can be passed to Read or ReadAll to configure options for
// reading files.
type ReadOptionFn func(*readOption)

// ReadAutoCompression specifies that the compression type of files should be auto-detected.
func ReadAutoCompression() ReadOptionFn {
	return func(o *readOption) {
		o.FileOpts = append(o.FileOpts, blobio.ReadAutoCompression())
	}
}

// ReadGzip specifies that files have been compressed using gzip.
func ReadGzip() ReadOptionFn {
	return func(o *readOption) {
		o.FileOpts = append(o.FileOpts, blobio.ReadGzip())
	}
}

// ReadUncompressed specifies that files have not been compressed.
func ReadUncompressed() ReadOptionFn {
	return func(o *readOption) {
		o.FileOpts = append(o.FileOpts, blobio.ReadUncompressed())
	}
}

// Read reads a set of files indicated by the glob pattern and returns
// the lines as a PCollection<string>. The newlines are not part of the lines.
// Read accepts a variadic number of ReadOptionFn that can be used to configure the compression
// type of the file. By default, the compression type is determined by the file extension.
func Read(s *beam.Scope, bucket, glob string, opts ...ReadOptionFn) beam.PCol[string] {
	// s = s.Scope("textio.Read")

	// filesystem.ValidateScheme(glob)
	return read(s, &readFn{}, beam.Create(s, beam.Pair(bucket, glob)), opts...).Lines
}

// ReadAll expands and reads the filename given as globs by the incoming
// PCollection<string>. It returns the lines of all files as a single
// PCollection<string>. The newlines are not part of the lines.
// ReadAll accepts a variadic number of ReadOptionFn that can be used to configure the compression
// type of the files. By default, the compression type is determined by the file extension.
func ReadAll(s *beam.Scope, col beam.PCol[beam.KV[string, string]], opts ...ReadOptionFn) beam.PCol[string] {
	// s = s.Scope("textio.ReadAll")
	return read(s, &readFn{}, col, opts...).Lines
}

// ReadWithFilename reads a set of files indicated by the glob pattern and returns
// a PCollection<KV<string, string>> of each filename and line. The newlines are not part of the lines.
// ReadWithFilename accepts a variadic number of ReadOptionFn that can be used to configure the compression
// type of the files. By default, the compression type is determined by the file extension.
func ReadWithFilename(s *beam.Scope, bucket, glob string, opts ...ReadOptionFn) beam.PCol[beam.KV[string, string]] {
	//s = s.Scope("textio.ReadWithFilename")

	// filesystem.ValidateScheme(glob)
	return read(s, &readWNameFn{}, beam.Create(s, beam.Pair(bucket, glob)), opts...).Lines
}

// read takes a PCollection of globs, finds all matching files, and applies
// the given DoFn on the files.
func read[T beam.Transform[blobio.ReadableBlob]](s *beam.Scope, dofn T, col beam.PCol[beam.KV[string, string]], opts ...ReadOptionFn) T {
	option := &readOption{}
	for _, opt := range opts {
		opt(option)
	}

	matches := blobio.MatchAll(s, col, blobio.MatchEmptyAllow())
	files := blobio.ReadMatches(s, matches, option.FileOpts...)
	return beam.ParDo(s, files, dofn)
}

// consumer is an interface for consuming a string value.
type consumer interface {
	Consume(e blobio.ReadableBlob, ec beam.ElmC, value string)
}

// emitter emits a string element.
type emitter struct {
	Emit beam.PCol[string]
}

func (e *emitter) Consume(_ blobio.ReadableBlob, ec beam.ElmC, value string) {
	e.Emit.Emit(ec, value)
}

// kvEmitter emits a KV<string, string> element.
type kvEmitter struct {
	Key  string
	Emit beam.PCol[beam.KV[string, string]]
}

func (e *kvEmitter) Consume(b blobio.ReadableBlob, ec beam.ElmC, value string) {
	e.Emit.Emit(ec, beam.Pair(b.Metadata.Key, value))
}

// restFac is used by readBaseFn as part of SDK handling.
type restFac struct{}

// This doesn't work, because we want a pointer, not a value type
// So we want to restrict it to be a pointer type.
func (restFac) Setup() error { return nil }

const (
	// blockSize is the desired size of each block for initial splits.
	blockSize int64 = 64 * 1024 * 1024 // 64 MB
	// tooSmall is the size limit for a block. If the last block is smaller than
	// this, it gets merged with the previous block.
	tooSmall = blockSize / 4
)

func (restFac) InitialSplit(e blobio.ReadableBlob, r beam.OffsetRange) iter.Seq2[beam.OffsetRange, float64] {
	// splits := rest.SizedSplits(blockSize)
	// numSplits := len(splits)
	// if numSplits > 1 {
	// 	last := splits[numSplits-1]
	// 	if last.End-last.Start <= tooSmall {
	// 		// Last restriction is too small, so merge it with previous one.
	// 		splits[numSplits-2].End = last.End
	// 		splits = splits[:numSplits-1]
	// 	}
	// }
	return maps.All(map[beam.OffsetRange]float64{
		r: float64(r.Max - r.Min),
	})
}

func (restFac) Produce(e blobio.ReadableBlob) beam.OffsetRange {
	return beam.OffsetRange{Min: 0, Max: e.Metadata.Size}
}

func processBundle[C consumer](base readBaseFn, dfc *beam.DFC[blobio.ReadableBlob], consumer C) error {
	// TODO switch with SDF version.
	ctx := dfc.Context()
	return base.Process(dfc,
		func(rest beam.OffsetRange) *beam.ORTracker {
			return &beam.ORTracker{
				Rest: rest,
			}
		},
		func(ec beam.ElmC, rb blobio.ReadableBlob, or beam.OffsetRange, tc beam.TryClaim[int64]) error {
			bucket, err := blob.OpenBucket(ctx, rb.Metadata.Bucket)
			if err != nil {
				return err
			}
			defer bucket.Close()
			rr, err := bucket.NewReader(ctx, rb.Metadata.Key, nil)

			if err != nil {
				return err
			}
			dfc.Logger().InfoContext(ctx, "textio.Read opening file", "file", rb.Metadata.Bucket+"/"+rb.Metadata.Key)

			rd := bufio.NewReader(rr)
			initialOffset := int64(0)

			i := or.Start()
			if i > 0 {
				// If restriction's starts after 0, we cannot assume a new line starts
				// at the beginning of the restriction, so we must search for the first
				// line beginning at or after restriction.Start. This is done by
				// scanning to the byte just before the restriction and then reading
				// until the next newline, leaving the reader at the start of a new
				// line past restriction.Start.
				i--
				n, err := rd.Discard(int(i)) // Scan to just before restriction.
				if err == io.EOF {
					return errors.Errorf("TextIO restriction lies outside the file being read. "+
						"Restriction begins at %v bytes, but file is only %v bytes.", i+1, n)
				}
				line, err := rd.ReadString('\n')
				if err == io.EOF {
					// No lines start in the restriction but it's still valid, so
					// finish claiming before returning to avoid errors.
					tc(func(p int64) (int64, error) {
						return or.Max, nil
					})
					return nil
				}
				if err != nil {
					return err
				}
				initialOffset = int64(len(line))
			}

			err = tc(func(p int64) (int64, error) {
				defer func() { initialOffset = 0 }()

				line, err := rd.ReadString('\n')
				if err == io.EOF {
					if len(line) != 0 {
						consumer.Consume(rb, ec, strings.TrimSuffix(line, "\n"))
					}
					return or.Max, nil
				}
				if err != nil {
					return 0, err
				}
				consumer.Consume(rb, ec, strings.TrimSuffix(line, "\n"))
				return p + initialOffset + int64(len(line)), nil
			})
			if err != nil {
				return err
			}
			return rr.Close()
		})
}

type readBaseFn = beam.BoundedSDF[restFac, blobio.ReadableBlob, *beam.ORTracker, beam.OffsetRange, int64, bool]

// readFn is an SDF that emits individual lines from a text file.
type readFn struct {
	beam.BoundedSDF[restFac, blobio.ReadableBlob, *beam.ORTracker, beam.OffsetRange, int64, bool]

	Lines beam.PCol[string]
}

func (fn *readFn) ProcessBundle(dfc *beam.DFC[blobio.ReadableBlob]) error {
	return processBundle(fn.BoundedSDF, dfc, &emitter{Emit: fn.Lines})
}

// readWNameFn is an SDF that emits individual lines from a text file with
// the filename as a key.
type readWNameFn struct {
	beam.BoundedSDF[restFac, blobio.ReadableBlob, *beam.ORTracker, beam.OffsetRange, int64, bool]

	Lines beam.PCol[beam.KV[string, string]]
}

func (fn *readWNameFn) ProcessBundle(dfc *beam.DFC[blobio.ReadableBlob]) error {
	return processBundle(fn.BoundedSDF, dfc, &kvEmitter{Emit: fn.Lines})
}

// WriteSingle writes a PCollection<string> to a single blob key as separate lines. The
// writer add a newline after each element.
//
// Intended for very small scale writes, as it doesn't shard large files.
//
// Emits written files.
func WriteSingle(s *beam.Scope, bucket, filename string, col beam.PCol[string]) beam.PCol[string] {
	// s = s.Scope("textio.Write")

	// TODO: enable urlmux override.
	// blob.DefaultURLMux().ValidBucketScheme(bucket)

	pre := beam.Map(s, col, func(line string) beam.KV[beam.KV[string, string], string] {
		return beam.Pair(beam.Pair(bucket, filename), line)
	})
	post := beam.GBK(s, pre)
	return beam.ParDo(s, post, &writeFilesFn{}).WrittenPaths
}

type writeFilesFn struct {
	WrittenPaths beam.PCol[string]
}

func (w *writeFilesFn) ProcessBundle(dfc *beam.DFC[beam.KV[beam.KV[string, string], beam.Iter[string]]]) error {
	ctx := dfc.Context()
	return dfc.Process(func(ec beam.ElmC, e beam.KV[beam.KV[string, string], beam.Iter[string]]) error {
		k := e.Key
		dfc.Logger().InfoContext(ctx, "textio.Write opening file", "file", k.Key+"/"+k.Value)
		bucket, err := blob.OpenBucket(ctx, k.Key)
		if err != nil {
			return err
		}
		defer bucket.Close()

		fd, err := bucket.NewWriter(ctx, k.Value, nil)
		if err != nil {
			return err
		}
		buf := bufio.NewWriterSize(fd, 1<<20) // use 1MB buffer
		for line := range e.Value.All() {
			if _, err := buf.WriteString(line); err != nil {
				return err
			}
			if _, err := buf.Write([]byte{'\n'}); err != nil {
				return err
			}
		}

		if err := buf.Flush(); err != nil {
			return err
		}
		if err := fd.Close(); err != nil {
			return err
		}
		w.WrittenPaths.Emit(ec, k.Key+"/"+k.Value)
		return nil
	})
}

// Immediate reads a local file at pipeline construction-time and embeds the
// data into a I/O-free pipeline source. Should be used for small files only.
func Immediate(s *beam.Scope, filename string) (beam.PCol[string], error) {
	// s = s.Scope("textio.Immediate")

	var data []string

	file, err := os.Open(filename)
	if err != nil {
		return beam.PCol[string]{}, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		data = append(data, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return beam.PCol[string]{}, err
	}
	return beam.Create(s, data...), nil
}
