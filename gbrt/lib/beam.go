package gbrt

import (
	"context"
	"image"
	"image/color"
	"math"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

//go:generate go install github.com/apache/beam/sdks/go/cmd/starcgen
//go:generate starcgen --package=grbt --identifiers=generateRaySDFn,CombinePixelsFn,MakeImageFn,TraceFn,ToPixelColour
//go:generate go fmt

// Setup to do the static initializing of the camera instead?

// Pixel is an x,y coordinate in an image. Used as a key in
// the pipeline.
type Pixel struct {
	X, Y int
}

// generateRaySDFn is a splittable DoFn that maps XY samples to the number line.
// Element Restrictions are then offset ranges into the number line, where each
// index maps to a specific sample for a specific pixel. Samples for a given
// pixel are contiguous to increase opportunities for combiner lifting to
// reduce shuffled data.
type generateRaySDFn struct {
}

// CreateInitialRestriction creates an offset range restriction representing
// the number of rays to cast.
func (fn *generateRaySDFn) CreateInitialRestriction(config ImageConfig) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: 1,
		End:   int64(config.Width*config.Height) * config.Samples,
	}
}

// SplitRestriction splits the restriction into 1024 chunks.
func (fn *generateRaySDFn) SplitRestriction(config ImageConfig, rest offsetrange.Restriction) (splits []offsetrange.Restriction) {
	return rest.EvenSplits(1024)
}

// RestrictionSize outputs the size of the restriction as the number of elements
// that restriction will output.
func (fn *generateRaySDFn) RestrictionSize(_ ImageConfig, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

// CreateTracker just creates an offset range restriction tracker for the
// restriction.
func (fn *generateRaySDFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

// ProcessElement creates a sample ray vector paired with the pixel it's
// contributing to.
func (fn *generateRaySDFn) ProcessElement(rt *sdf.LockRTracker, cfg ImageConfig, emit func(Pixel, Vec)) error {
	// Sample aligned indexing to preserve pixel locality.
	// Increases likelyhood that pixels are in the same bundle
	// improving combiner lifting effectiveness.
	stride := cfg.Width * float64(cfg.Samples)
	for i := rt.GetRestriction().(offsetrange.Restriction).Start; rt.TryClaim(i); i++ {
		Y := math.Floor(float64(i) / stride)
		sample := math.Mod(float64(i), stride)
		X := math.Floor(sample / float64(cfg.Samples))
		px := Pixel{int(X), int(Y)}
		ray := subPixelJitter(px.X, px.Y, cfg)
		emit(px, ray)
	}
	return nil
}

// TraceFn creates rays from the pixels.
type TraceFn struct {
	// TODO retype Vec to Position or something.
	// requires redoing doing all the math for type safety.
	Position Vec
	Bounces  int64
	// TODO move this to a side input
	Word string

	scene *Scene
}

// Setup do the one time setup for the scene.
func (f *TraceFn) Setup() {
	f.scene = populateScene(f.Word)
	f.scene.Initialize()
}

// ProcessElement actualy traces the scene of the image and returns the colour contribution of this sample.
// TODO retype the returned thing to a colour, instead of a vec.
func (f *TraceFn) ProcessElement(k Pixel, ray Vec) (Pixel, Vec) {
	// TODO break this up so bounces are contributed separately.
	colour := Trace(f.Position, ray, f.scene, f.Bounces)
	return k, colour
}

// CombinePixelsFn combines the contributions from multiple pixels.
type CombinePixelsFn struct {
	SamplesCount int
}

var (
	pixelAddInputCount = beam.NewCounter("gbrt", "pixelAddInput")
	pixelMergesCount   = beam.NewCounter("gbrt", "pixelMerges")
)

// AddInput sums together the colour contributions for a pixel.
// Typically on the lifted side of a CombineFn
func (fn *CombinePixelsFn) AddInput(ctx context.Context, a, b Vec) Vec {
	pixelAddInputCount.Inc(ctx, 1)
	return a.Plus(b)
}

// MergeAccumulators sums together the colour contributions for a pixel.
func (fn *CombinePixelsFn) MergeAccumulators(ctx context.Context, a, b Vec) Vec {
	pixelMergesCount.Inc(ctx, 1)
	return a.Plus(b)
}

// ExtractOutput does the Reinhard tone mapping for this pixel.
func (fn *CombinePixelsFn) ExtractOutput(colour Vec) Vec {
	// Attenuate the combined sample colours.
	colour = colour.Times(MonoVec(1. / float64(fn.SamplesCount))).Plus(MonoVec(14. / 241.))
	o := colour.Plus(MonoVec(1))
	colour = Vec{colour.X / o.X, colour.Y / o.Y, colour.Z / o.Z}.Times(MonoVec(255))
	return colour
}

// PixelColour combines a pixel with its colour.
type PixelColour struct {
	K Pixel
	C Vec
}

// ToPixelColour combines pixels with it's colour.
func ToPixelColour(k Pixel, colour Vec) PixelColour {
	return PixelColour{k, colour}
}

// Col is a column of pixels in the final image.
type Col struct {
	Col []PixelColour
}

// MakeImageFn writes the image to wherever.
type MakeImageFn struct {
	Width, Height int
	Out           string
}

// ProcessElement iterates over all the functions and writes
// Writes the file to the designated spot.
func (f *MakeImageFn) ProcessElement(ctx context.Context, _ beam.T, iter func(*PixelColour) bool) (bool, error) {
	img := image.NewRGBA(image.Rect(0, 0, f.Width, f.Height))
	var pc PixelColour
	for iter(&pc) {
		img.Set(f.Width-pc.K.X-1, f.Height-pc.K.Y-1, color.RGBA{uint8(pc.C.X), uint8(pc.C.Y), uint8(pc.C.Z), 255})
	}
	if err := writeToFile(ctx, f.Out, img); err != nil {
		log.Infof(ctx, "ERROR:", err)
		return false, err
	}
	return true, nil
}

// BeamTracer runs the ray tracer as a Apache Beam Pipeline on the runner of choice.
func BeamTracer(position Vec, img ImageConfig, word, dir string) *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	rays := generateRays(s, img)

	trace := beam.ParDo(s.Scope("Trace"), &TraceFn{Position: position, Bounces: img.Bounces, Word: word}, rays)
	finalPixels := beam.CombinePerKey(s.Scope("MergeRays"), &CombinePixelsFn{int(img.Samples)}, trace)

	output := OutputPath(dir, word, int(img.Samples))
	toImage(s, finalPixels, img, output)
	return p
}

func generateRays(s beam.Scope, img ImageConfig) beam.PCollection {
	s = s.Scope("GenerateRays")
	cfg := beam.Create(s, img)
	return beam.ParDo(s, &generateRaySDFn{}, cfg)
}

func toImage(s beam.Scope, finalPixels beam.PCollection, img ImageConfig, output string) {
	s = s.Scope("ToImage")
	pixelColours := beam.ParDo(s, ToPixelColour, finalPixels)
	fixedKeyPixelColours := beam.AddFixedKey(s, pixelColours)
	// Get everything onto a single machine again.
	groupedPixelColours := beam.GroupByKey(s, fixedKeyPixelColours)
	beam.ParDo(s, &MakeImageFn{Width: int(img.Width), Height: int(img.Height), Out: output}, groupedPixelColours)
}
