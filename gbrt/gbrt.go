// gbrt is initially a quick re-write of the postcard pathtracer
// from c++ to Go, without the minification, as described at
// http://fabiensanglard.net/postcard_pathtracer/index.html
//
// DevLog:
// 2019/03/27: Got a working ray tracer!
//
// 2019/03/29:
// The quick re-write then expanded as I deconstructed it into
// more modular parts. I separated out the letter logic, and
// began a portable scene graph system, and "model" system
// for re-use of geometry.
// I'll eventually need to handle matrix multiplcation to
// transform the position vectors from WorldSpace to
// scene space, but for now, simple subtraction will do.
// The letter code is interesting since it was not optimized
// in a precomputed sense. There's a pile of repeated operations
// that can be elided. Similarly, I can reduce some work by
// adding axis aligned bounding boxes, so things work faster.
// This will be critical for actual geometry later on, when
// the triangles strike.
// Fun fact: Moving the "curve" math into the letters
// loop initially slowed down the rendering.
// Initially it was ~1m20s for 2 samples per pixel, but now it's 3m40s.
// Except when it's 2m44s (probably due to avoiding the letter data copy now Woo pointers.)
// Gonna need them AABBs next time.
//
// 2019/03/29 11am:
// AABBs implemented for the Letters finally. For Ray Marching it's important to
// always provide the distance to the bounding box properly, or it all falls aparte
// with weird artifaces.
// Once implemented it ended up with 20s for a 2 sample image, and 3m for 16 sample.
//
// I tried naive goroutine paralelization (go-routine per pixel) and that made the 16 sample run
// take twice as long. I need to do the N Worker Threads approach
// 1:30pm:
// I now have repositional width respecting letters, that can be driven by the command line.
// TODO - I need to finish the rest of the keyboard.
// TODO - I need to model and bounding box specifc areas of the room (ceiling slats specifically)
// TODO - Fix Bouncing so it doesn't make everything so bright (the only cure might be more samples)
// TODO - Might need to convert from Path Tracer to Bi-Directional Ray Tracer for iteration purposes
//     (would be faster?) - but less "authentic".
// TODO - Explicit Materials -> colours, reflections, speculars
// TODO - Additional Lights
// TODO - Refractive surfaces.
// TODO - Triangle meshes
// TODO - "Camera" Viewpoint and Matrix transforms and rotations.
// TODO - Move letter point data to a file that can be read in.
// TODO - Perhaps subdivide the scene into a grid, and associate each model with the boxes they intersect. Would need to change how Ray Marching works though.
//
// Beam Conversion Notes:
// The Batch Pipeline will start with a simple beam.Create(s, *word)
// This introduces adding static data to the pipeline.
// The characters will be de-duplicated.
// The letter "models" will be read in, filtered by the characters of
// the provided word, as a side input, and positioned in a scene.
// The scene will then be passed to the main rendering loop.
// - For streaming the input will be from PubSub, but almost
// everything else is the same.
// Then everything comes through the rendering structure.
// Resolution is configured as StructuralDoFn data, which generates
// centered origin pixels from the camera information of the scene.
// And then each pixel is fanned out as Value - Key{X,Y}- Value{Colour,Origin,Ray}, which
// has the random jitter. This is then re-shuffled (or SDFed) out, with the XY as key, which
// is passed throughout the scene.
// Ideally this is tossed straight into the shuffle sink.
// Then the rendering begins.
//  - number of bounces are just iterations of the DoFn, just passing in the new {Colour,Origin,Ray}
// triple down. The scene is fed in via a side inputs, and remains static. The keys are propagated
// through each layer.
// Might keep the bounce loop initially as a single DoFn.
// But additional rays might be worthwhile (additional reshuffles/SDFs)
// In the end everything is tossed into a CombineFn and aggregated by Pixel{X,Y}.
// The number of contributions summed, and precombined in add input, and then
// accumulators merged.
// Everything is then *further* combined, into the single image ( using beam.FixedKey)
// Elements are the now-single X,Y Colour values
// Then they are put into an image, and finally written out to some
// preconfigured destination, likely based on the input: eg  WORD.Nspp.WIDTHxHEIGHT.png
//
// Most of the values won't really need custom coders, except for performance
// but Scenes, and Models will for sure, since  they will be tossed around
// between stages as side inputs.
//
// 2020-07-31: Revived this to test schemas and SDFs. Added loopback mode to
// the SDK to make it easier to run against local portable runners.
//
// 2021-07-27: Started working on this again for Beam Summit 2021.
// 1:30pm- prepping to put this at github.com/lostluck/experimental.
// Splitting things into several files, and getting it working with go modules.
// Now can write to both GCS and Local file systems (assuming local access)
// Added a few more materials, and adjusted the letter G to look nicer.
//
// Still a fair pile of TODOs and other ideas. There's likely a goodly number
// of optimizations to do for a Dataflow pipeline WRT fanout and aggregation.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	gbrt "github.com/lostluck/experimental/gbrt/lib"

	// Register runners for use with beamx.
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/direct"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/universal"

	// Be able to write to GCS and local systems.
	"github.com/apache/beam/sdks/go/pkg/beam/io/filesystem"
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/gcs"
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
)

const (
	// TODO make these flags.
	w, h = 960, 540 // 960, 540 by default
)

var (
	samplesCount = flag.Int("samples", 16, "The number of ray samples per pixel")
	word         = flag.String("word", "PIXAR", "The word you'd like to render.")

	useBeam    = flag.Bool("use_beam", false, "Whether to use the beam version or not.")
	runner     = flag.String("runner", "direct", "Which runner to use")
	outputDir  = flag.String("output_dir", "/tmp/gbrt", "The destination file to write the png image.")
	cpuProfile = flag.String("cpu_profile", "", "The filename to write a cpuprofile to in the output directory.")
	bounces    = flag.Int("bounces", 3, "The number of bounces for each path traced")
)

func main() {
	flag.Parse()
	beam.Init()
	if *cpuProfile != "" {
		f, err := os.Create(path.Join(*outputDir, *cpuProfile))
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	filesystem.ValidateScheme(*outputDir)

	*word = strings.ToUpper(*word) // We only have upper case letters.

	// Validate character set of word to render.
	if err := gbrt.ValidateWord(*word); err != nil {
		log.Printf("invalid word: %v", err)
		return
	}

	// Set up the camera.
	position := gbrt.Vec{X: -22, Y: 5, Z: 25}                     // Where the camera actually is, notionally.
	goal := gbrt.Vec{X: -3, Y: 4, Z: 0}.Minus(position).InvSqrt() // The bottom right corner.
	invW := 1.0 / w
	left := gbrt.Vec{X: goal.Z, Y: 0, Z: -goal.X}.InvSqrt().Times(gbrt.MonoVec(invW))

	// Cross-product to get the up vector
	up := goal.Cross(left)

	cfg := gbrt.ImageConfig{
		Width: w, Height: h,
		Samples: int64(*samplesCount),
		Bounces: int64(*bounces),
		Goal:    goal, Left: left, Up: up,
	}

	// Timing.
	start := time.Now()
	defer func() {
		delta := time.Since(start)
		fmt.Fprintln(os.Stderr, " ", delta)
	}()

	log.Printf("bounces%d.samples%d.%s.\n", *bounces, *samplesCount, *word)
	if *useBeam {
		p := gbrt.BeamTracer(position, cfg, *word, *outputDir)
		pipelineResult, err := beam.Run(context.Background(), *runner, p)
		if err != nil {
			log.Printf("Pipeline execution failed: %v", err)
			return
		}
		if pipelineResult != nil {
			log.Printf("Printing %v counters", len(pipelineResult.Metrics().AllMetrics().Counters()))
			for _, c := range pipelineResult.Metrics().AllMetrics().Counters() {
				log.Printf("%v.%v = %v", c.Key.Name, c.Key.Namespace, c.Result())
			}
		}
	} else {
		gbrt.OrdinaryTracer(position, cfg, *word, *outputDir)
	}
	log.Printf("image written to: %s", gbrt.OutputPath(*outputDir, *word, *samplesCount))
}
