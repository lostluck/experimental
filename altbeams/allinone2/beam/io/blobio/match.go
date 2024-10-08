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
	"io"
	"strings"

	"github.com/lostluck/experimental/altbeams/allinone2/beam"
	"gocloud.dev/blob"
)

// emptyTreatment controls how empty matches of a pattern are treated.
type emptyTreatment int

const (
	// emptyAllow allows empty matches.
	emptyAllow emptyTreatment = iota
	// emptyDisallow disallows empty matches.
	emptyDisallow
	// emptyAllowIfWildcard allows empty matches if the pattern contains a wildcard.
	emptyAllowIfWildcard
)

type matchOption struct {
	EmptyTreatment emptyTreatment
}

// MatchOptionFn is a function that can be passed to MatchFiles or MatchAll to configure options for
// matching files.
type MatchOptionFn func(*matchOption)

// MatchEmptyAllowIfWildcard specifies that empty matches are allowed if the pattern contains a
// wildcard.
func MatchEmptyAllowIfWildcard() MatchOptionFn {
	return func(o *matchOption) {
		o.EmptyTreatment = emptyAllowIfWildcard
	}
}

// MatchEmptyAllow specifies that empty matches are allowed.
func MatchEmptyAllow() MatchOptionFn {
	return func(o *matchOption) {
		o.EmptyTreatment = emptyAllow
	}
}

// MatchEmptyDisallow specifies that empty matches are not allowed.
func MatchEmptyDisallow() MatchOptionFn {
	return func(o *matchOption) {
		o.EmptyTreatment = emptyDisallow
	}
}

type matchFiles struct {
	Input beam.Output[string]

	Options []MatchOptionFn
}

func (mf *matchFiles) Expand(s *beam.Scope) beam.Output[BlobMetadata] {
	option := &matchOption{
		EmptyTreatment: emptyAllowIfWildcard,
	}

	for _, opt := range mf.Options {
		opt(option)
	}

	return beam.ParDo(s, mf.Input, newMatchFn(option)).Blobs
}

// MatchFiles finds all files matching the glob pattern and returns a PCollection<FileMetadata> of
// the matching files. MatchFiles accepts a variadic number of MatchOptionFn that can be used to
// configure the treatment of empty matches. By default, empty matches are allowed if the pattern
// contains a wildcard.
func MatchFiles(s *beam.Scope, glob string, opts ...MatchOptionFn) beam.Output[BlobMetadata] {
	scheme := glob
	// TODO allow overriding URLMux here.
	blob.DefaultURLMux().ValidBucketScheme(scheme)

	return beam.Expand(s, "blobio.MatchFiles", &matchFiles{
		Input:   beam.Create(s, glob),
		Options: opts,
	})
}

// MatchAll finds all files matching the glob patterns given by the incoming PCollection<string> and
// returns a beam.Output[BlobMetadata] of the matching files. MatchAll accepts a variadic number of
// MatchOptionFn that can be used to configure the treatment of empty matches. By default, empty
// matches are allowed if the pattern contains a wildcard.
func MatchAll(s *beam.Scope, col beam.Output[string], opts ...MatchOptionFn) beam.Output[BlobMetadata] {
	return beam.Expand(s, "blobio.MatchAll", &matchFiles{
		Input:   col,
		Options: opts,
	})
}

type matchFn struct {
	EmptyTreatment emptyTreatment

	Blobs beam.Output[BlobMetadata]
}

func newMatchFn(option *matchOption) *matchFn {
	return &matchFn{
		EmptyTreatment: option.EmptyTreatment,
	}
}

func (fn *matchFn) ProcessBundle(dfc *beam.DFC[string]) error {
	ctx := dfc.Context()

	// TODO: Allow user overriding of the URL mux.
	urlMux := blob.DefaultURLMux()

	return dfc.Process(func(ec beam.ElmC, glob string) error {
		if strings.TrimSpace(glob) == "" {
			return nil
		}
		// TODO: parse the glob to the bucket prefix.
		bucketRoot := glob

		bucket, err := urlMux.OpenBucket(ctx, bucketRoot)
		if err != nil {
			return err
		}
		defer bucket.Close()

		listIter := bucket.List(nil)
		count := 0
		for {
			lo, err := listIter.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			count++
			fn.Blobs.Emit(ec, BlobMetadata{
				Bucket:       bucketRoot,
				Key:          lo.Key,
				Size:         lo.Size,
				LastModified: lo.ModTime,
			})
		}

		if count == 0 && !allowEmptyMatch(glob, fn.EmptyTreatment) {
			return fmt.Errorf("no files matching pattern %q", glob)
		}
		return nil
	})
}

func allowEmptyMatch(glob string, treatment emptyTreatment) bool {
	switch treatment {
	case emptyDisallow:
		return false
	case emptyAllowIfWildcard:
		return strings.Contains(glob, "*")
	default:
		return true
	}
}

// TODO Implement Continous Matching once . Below is what's copied from the current SDK.

// duplicateTreatment controls how duplicate matches are treated.
// type duplicateTreatment int

// const (
// 	// duplicateAllow allows duplicate matches.
// 	duplicateAllow duplicateTreatment = iota
// 	// duplicateAllowIfModified allows duplicate matches only if the file has been modified since it
// 	// was last observed.
// 	duplicateAllowIfModified
// 	// duplicateSkip skips duplicate matches.
// 	duplicateSkip
// )

// type matchContOption struct {
// 	Start              time.Time
// 	End                time.Time
// 	DuplicateTreatment duplicateTreatment
// 	ApplyWindow        bool
// }

// // MatchContOptionFn is a function that can be passed to MatchContinuously to configure options for
// // matching files.
// type MatchContOptionFn func(*matchContOption)

// // MatchStart specifies the start time for matching files.
// func MatchStart(start time.Time) MatchContOptionFn {
// 	return func(o *matchContOption) {
// 		o.Start = start
// 	}
// }

// // MatchEnd specifies the end time for matching files.
// func MatchEnd(end time.Time) MatchContOptionFn {
// 	return func(o *matchContOption) {
// 		o.End = end
// 	}
// }

// // MatchDuplicateAllow specifies that file path matches will not be deduplicated.
// func MatchDuplicateAllow() MatchContOptionFn {
// 	return func(o *matchContOption) {
// 		o.DuplicateTreatment = duplicateAllow
// 	}
// }

// // MatchDuplicateAllowIfModified specifies that file path matches will be deduplicated unless the
// // file has been modified since it was last observed.
// func MatchDuplicateAllowIfModified() MatchContOptionFn {
// 	return func(o *matchContOption) {
// 		o.DuplicateTreatment = duplicateAllowIfModified
// 	}
// }

// // MatchDuplicateSkip specifies that file path matches will be deduplicated.
// func MatchDuplicateSkip() MatchContOptionFn {
// 	return func(o *matchContOption) {
// 		o.DuplicateTreatment = duplicateSkip
// 	}
// }

// // MatchApplyWindow specifies that each element will be assigned to an individual window.
// func MatchApplyWindow() MatchContOptionFn {
// 	return func(o *matchContOption) {
// 		o.ApplyWindow = true
// 	}
// }

// MatchContinuously finds all files matching the glob pattern at the given interval and returns a
// PCollection<FileMetadata> of the matching files. MatchContinuously accepts a variadic number of
// MatchContOptionFn that can be used to configure:
//
//   - Start: start time for matching files. Defaults to the current timestamp
//   - End: end time for matching files. Defaults to the maximum timestamp
//   - DuplicateAllow: allow emitting matches that have already been observed. Defaults to false
//   - DuplicateAllowIfModified: allow emitting matches that have already been observed if the file
//     has been modified since the last observation. Defaults to false
//   - DuplicateSkip: skip emitting matches that have already been observed. Defaults to true
//   - ApplyWindow: assign each element to an individual window with a fixed size equivalent to the
//     interval. Defaults to false, i.e. all elements will reside in the global window
// func MatchContinuously(
// 	s beam.Scope,
// 	glob string,
// 	interval time.Duration,
// 	opts ...MatchContOptionFn,
// ) beam.PCollection {
// 	s = s.Scope("fileio.MatchContinuously")

// 	filesystem.ValidateScheme(glob)

// 	option := &matchContOption{
// 		Start:              mtime.Now().ToTime(),
// 		End:                mtime.MaxTimestamp.ToTime(),
// 		ApplyWindow:        false,
// 		DuplicateTreatment: duplicateSkip,
// 	}

// 	for _, opt := range opts {
// 		opt(option)
// 	}

// 	imp := periodic.Impulse(s, option.Start, option.End, interval, false)
// 	globs := beam.ParDo(s, &matchContFn{Glob: glob}, imp)
// 	matches := MatchAll(s, globs, MatchEmptyAllow())

// 	out := dedupIfRequired(s, matches, option.DuplicateTreatment)

// 	if option.ApplyWindow {
// 		return beam.WindowInto(s, window.NewFixedWindows(interval), out)
// 	}
// 	return out
// }

// func dedupIfRequired(
// 	s beam.Scope,
// 	col beam.PCollection,
// 	treatment duplicateTreatment,
// ) beam.PCollection {
// 	if treatment == duplicateAllow {
// 		return col
// 	}

// 	keyed := beam.ParDo(s, keyByPath, col)

// 	if treatment == duplicateAllowIfModified {
// 		return beam.ParDo(s, &dedupUnmodifiedFn{}, keyed)
// 	}

// 	return beam.ParDo(s, &dedupFn{}, keyed)
// }

// type matchContFn struct {
// 	Glob string
// }

// func (fn *matchContFn) ProcessElement(_ []byte, emit func(string)) {
// 	emit(fn.Glob)
// }

// func keyByPath(md BlobMetadata) (string, BlobMetadata) {
// 	return md.Path, md
// }

// type dedupFn struct {
// 	State state.Value[struct{}]
// }

// func (fn *dedupFn) ProcessElement(
// 	sp state.Provider,
// 	_ string,
// 	md BlobMetadata,
// 	emit func(BlobMetadata),
// ) error {
// 	_, ok, err := fn.State.Read(sp)
// 	if err != nil {
// 		return fmt.Errorf("error reading state: %v", err)
// 	}

// 	if !ok {
// 		emit(md)
// 		if err := fn.State.Write(sp, struct{}{}); err != nil {
// 			return fmt.Errorf("error writing state: %v", err)
// 		}
// 	}

// 	return nil
// }

// type dedupUnmodifiedFn struct {
// 	State state.Value[int64]
// }

// func (fn *dedupUnmodifiedFn) ProcessElement(
// 	sp state.Provider,
// 	_ string,
// 	md BlobMetadata,
// 	emit func(BlobMetadata),
// ) error {
// 	prevMTime, ok, err := fn.State.Read(sp)
// 	if err != nil {
// 		return fmt.Errorf("error reading state: %v", err)
// 	}

// 	mTime := md.LastModified.UnixMilli()

// 	if !ok || mTime > prevMTime {
// 		emit(md)
// 		if err := fn.State.Write(sp, mTime); err != nil {
// 			return fmt.Errorf("error writing state: %v", err)
// 		}
// 	}

// 	return nil
// }
