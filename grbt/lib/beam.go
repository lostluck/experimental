package grbt

import (
	"context"
	"image"
	"image/color"
	"math"
	"math/rand"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

//go:generate go install github.com/apache/beam/sdks/go/cmd/starcgen
//go:generate starcgen --package=grbt --identifiers=generateRaySDFn,CombinePixelsFn,GeneratePixelsFn,GenerateSampleRaysFn,MakeImageFromColFn,TraceFn,AddRandomKey,ConcatPixels,KeyByX,RePair,RePairPixels,ToPixelColour
//go:generate go fmt

// GeneratePixelsFn creates the initial pixels that we want colours for.
type GeneratePixelsFn struct {
	// Width and Height of the image.
	Width, Height int
}

// Setup to do the static initializing of the camera instead?

// Pixel is an x,y coordinate in an image.
type Pixel struct {
	X, Y int
}

// ProcessElement generates a key for each pixel in the image.
func (f *GeneratePixelsFn) ProcessElement(_ []byte, emit func(Pixel)) {
	for y := 0; y < f.Height; y++ {
		for x := 0; x < f.Width; x++ {
			emit(Pixel{X: x, Y: y})
		}
	}
}

// GenerateSampleRaysFn creates rays from the pixels.
type GenerateSampleRaysFn struct {
	Cfg ImageConfig
}

// ProcessElement sample rays for each   for each pixel in the image.
func (f *GenerateSampleRaysFn) ProcessElement(k Pixel, emit func(Pixel, Vec)) {
	for p := 0; p < int(f.Cfg.Samples); p++ {
		emit(k, subPixelJitter(k.X, k.Y, f.Cfg))
	}
}

// GenerateSampleRaysFromIntsFn creates rays from the pixels.
type GenerateSampleRaysFromIntsFn struct {
	Cfg ImageConfig

	perSample float64 // W*H ,
}

// Setup avoids repeating the common math
func (f *GenerateSampleRaysFromIntsFn) Setup() {
	f.perSample = f.Cfg.Width * f.Cfg.Height
}

// ProcessElement sample rays for each for each pixel in the image.
func (f *GenerateSampleRaysFromIntsFn) ProcessElement(i int64) (Pixel, Vec) {
	slice := math.Mod(float64(i), f.perSample)
	X := math.Mod(slice, f.Cfg.Width)
	Y := math.Floor(slice / f.Cfg.Width)
	k := Pixel{int(X), int(Y)}
	return k, subPixelJitter(k.X, k.Y, f.Cfg)
}

// TraceFn creates rays from the pixels.
type TraceFn struct {
	// TODO retype Vec to Position or something.
	// requires redoing doing all the math for type safety.
	Position Vec
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
	colour := Trace(f.Position, ray, f.scene)
	return k, colour
}

// AddRandomKey adds a random int64 key.
func AddRandomKey(v beam.T) (int64, beam.T) {
	return rand.Int63(), v
}

// RePairPixels takes the Value iterators for this key, and re-pairs them together
// for later processing.
func RePairPixels(_ beam.T, iter func(*Pixel) bool, emit func(Pixel)) {
	var px Pixel
	for iter(&px) {
		emit(px)
	}
}

// RePair takes the Value iterators for this key, and re-pairs them together
// for later processing.
func RePair(k Pixel, iter func(*Vec) bool, emit func(Pixel, Vec)) {
	var ray Vec
	for iter(&ray) {
		emit(k, ray)
	}
}

// CombinePixelsFn combines the contributions from multiple pixels.
type CombinePixelsFn struct {
	SamplesCount int
}

// MergeAccumulators sums together the colour contributions for a pixel.
func (fn *CombinePixelsFn) MergeAccumulators(a, b Vec) Vec {
	return a.Plus(b)
}

// ExtractOutput does the Reinhard tone mapping for this pixel.
// TODO: can I drop the key here? to avoid the extra step?
func (fn *CombinePixelsFn) ExtractOutput(colour Vec) Vec {
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

// KeyByX keys each PixelColour by its X coordinate.
func KeyByX(v PixelColour) (int, PixelColour) {
	return v.K.X, v
}

// Col is a column of pixels in the final image.
type Col struct {
	Col []PixelColour
}

// ConcatPixels combines individual columns together.
func ConcatPixels(_ beam.T, iter func(*PixelColour) bool) Col {
	var p PixelColour
	var ps Col
	for iter(&p) {
		ps.Col = append(ps.Col, p)
	}
	return ps
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

// MakeImageFromColFn writes the image to wherever.
type MakeImageFromColFn struct {
	Width, Height int
	Out           string
}

// ProcessElement iterates over all the functions and writes
// Writes the file to the designated spot.
func (f *MakeImageFromColFn) ProcessElement(ctx context.Context, _ beam.T, iter func(*Col) bool) (bool, error) {
	img := image.NewRGBA(image.Rect(0, 0, f.Width, f.Height))
	var pcs Col
	for iter(&pcs) {
		for _, pc := range pcs.Col {
			img.Set(f.Width-pc.K.X-1, f.Height-pc.K.Y-1, color.RGBA{uint8(pc.C.X), uint8(pc.C.Y), uint8(pc.C.Z), 255})
		}
	}
	if err := writeToFile(ctx, f.Out, img); err != nil {
		log.Infof(ctx, "ERROR:", err)
		return false, err
	}
	return true, nil
}

// BeamTracer runs the ray tracer as a Apache Beam Pipeline on the runner of choice.
func BeamTracer(position Vec, img ImageConfig, word, dir string, samples int) *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	rays := generateRays(s, img)

	// Portable version
	// Generate the initial pixels and sample rays.
	// pixels := generatePixels(s)
	// rays := beam.ParDo(s, &GenerateSampleRaysFn{goal, left, up, samples}, pixels)

	// "Reshuffle"
	// gbp2 := beam.GroupByKey(s, rays)
	// rays = beam.ParDo(s, RePair, gbp2)

	// Actually Trace the image.
	trace1 := beam.ParDo(s, &TraceFn{Position: position, Word: word}, rays)
	finalPixels := beam.CombinePerKey(s, &CombinePixelsFn{samples}, trace1)
	// "Drop" the pixel keys colours.
	fixedKeyPixelColours := toColourColumns(s, finalPixels)

	// Get everything onto a single machine again.
	groupedPixelColours := beam.GroupByKey(s, fixedKeyPixelColours)

	output := OutputPath(dir, word, samples)
	beam.ParDo(s, &MakeImageFromColFn{Width: int(img.Width), Height: int(img.Height), Out: output}, groupedPixelColours)
	return p
}

func generateRays(s beam.Scope, img ImageConfig) beam.PCollection {
	s = s.Scope("generateRays")
	cfg := beam.Create(s, img)
	return beam.ParDo(s, &generateRaySDFn{}, cfg)
}

func generatePixels(s beam.Scope, img ImageConfig) beam.PCollection {
	s = s.Scope("generatePixels")
	// Generate the initial pixels and sample rays.
	imp := beam.Impulse(s)
	pixels := beam.ParDo(s, &GeneratePixelsFn{Width: int(img.Width), Height: int(img.Height)}, imp)
	// "Reshuffle"
	pixelsRand := beam.ParDo(s, AddRandomKey, pixels)
	gbp := beam.GroupByKey(s, pixelsRand)
	return beam.ParDo(s, RePairPixels, gbp)
}

func toColourColumns(s beam.Scope, finalPixels beam.PCollection) beam.PCollection {
	s = s.Scope("toColours")
	pixelColours := beam.ParDo(s, ToPixelColour, finalPixels)
	pixelColoursX := beam.ParDo(s, KeyByX, pixelColours)
	foo := beam.GroupByKey(s, pixelColoursX)
	columnColours := beam.ParDo(s, ConcatPixels, foo)
	return beam.AddFixedKey(s, columnColours)
}

// ImageConfig contains properties of the generated image, used to generate initial rays.
type ImageConfig struct {
	Width, Height float64
	Samples       int64

	Goal, Left, Up Vec
}

// generateRaySDFn is a splittable DoFn implementing behavior similar to
// the Dax RangeIO source.
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

// SplitRestriction splits the image config into a set of offsetrange restrictions, mapping
// each initial ray onto the number line. Splits the image by the image width * samples.
func (fn *generateRaySDFn) SplitRestriction(config ImageConfig, rest offsetrange.Restriction) (splits []offsetrange.Restriction) {
	return rest.EvenSplits(int64(config.Width) * config.Samples)
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

// ProcessElement creates it's assigned integer elements based on the restriction
// tracker received. SourceConfig is ignored at this stage since the restriction
// fully defines the legal output.
func (fn *generateRaySDFn) ProcessElement(rt *sdf.LockRTracker, cfg ImageConfig, emit func(Pixel, Vec)) error {
	perSample := cfg.Width * cfg.Height
	for i := rt.GetRestriction().(offsetrange.Restriction).Start; rt.TryClaim(i) == true; i++ {
		slice := math.Mod(float64(i), perSample)
		X := math.Mod(slice, cfg.Width)
		Y := math.Floor(slice / cfg.Width)
		px := Pixel{int(X), int(Y)}
		ray := subPixelJitter(px.X, px.Y, cfg)
		emit(px, ray)
	}
	return nil
}
