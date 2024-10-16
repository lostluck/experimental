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

package main

import (
	"context"
	"flag"
	"fmt"
	"regexp"
	"strings"

	"github.com/lostluck/experimental/altbeams/allinone2/beam"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/io/textio"
	"golang.org/x/exp/constraints"

	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
)

var (
	wordRE          = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
	smallWordLength = flag.Int("small_word_length", 9, "length of small words (default: 9)")
	// TODO: Add helper for parsing paths into buckets and files.
)

func wordcountPipeline() func(s *beam.Scope) error {
	return func(s *beam.Scope) error {
		lines := textio.Read(s, "gs://apache-beam-samples/", "shakespeare/kinglear.txt")
		wordcount := CountWords(s, lines, *smallWordLength)

		formatted := beam.Map(s, wordcount, func(count beam.KV[string, int]) string {
			return fmt.Sprintf("%s: %d", count.Key, count.Value)
		})
		textio.WriteSingle(s, "file:///tmp/wordcount/", "counts.txt", formatted)
		return nil
	}
}

// extractFn is a structural DoFn that emits the words in a given line and keeps
// a count for small words. Its ProcessElement function will be invoked on each
// element in the input PCollection.
type extractFn struct {
	SmallWordLength int

	SmallWords beam.CounterInt64
	LineLen    beam.DistributionInt64
	EmptyLines beam.CounterInt64

	Words beam.Output[string]
}

func (fn *extractFn) ProcessBundle(dfc *beam.DFC[string]) error {
	return dfc.Process(func(ec beam.ElmC, line string) error {
		fn.LineLen.Update(dfc, int64(len(line)))

		if len(strings.TrimSpace(line)) == 0 {
			fn.EmptyLines.Inc(dfc, 1)
		}
		for _, word := range wordRE.FindAllString(line, -1) {
			// increment the counter for small words if length of words is
			// less than small_word_length
			if len(word) < fn.SmallWordLength {
				fn.SmallWords.Inc(dfc, 1)
			}
			fn.Words.Emit(ec, word)
		}
		return nil
	})
}

type countWords struct {
	Lines beam.Output[string]

	SmallWordLength int
}

func (cw *countWords) Expand(s *beam.Scope) beam.Output[beam.KV[string, int]] {
	extract := beam.ParDo(s, cw.Lines, &extractFn{
		SmallWordLength: cw.SmallWordLength,
	}, beam.Name("extract"))

	paired := beam.Map(s, extract.Words, func(word string) beam.KV[string, int] {
		return beam.Pair(word, 1)
	})

	return beam.CombinePerKey(s, paired,
		beam.SimpleMerge(sum[int]{}))
}

type sum[A constraints.Float | constraints.Integer] struct{}

func (sum[A]) MergeAccumulators(a A, b A) A {
	return a + b
}

func CountWords(s *beam.Scope, lines beam.Output[string], smallWordLength int) beam.Output[beam.KV[string, int]] {
	return beam.Expand(s, "CountWords", &countWords{
		Lines:           lines,
		SmallWordLength: smallWordLength,
	})
}

func main() {
	pr, err := beam.LaunchAndWait(context.Background(), wordcountPipeline(), beam.Name("wordcount"))
	if err != nil {
		fmt.Println("error", err)
	}
	// TODO: Improve Metrics experience.
	fmt.Println(pr.Counters)
}
