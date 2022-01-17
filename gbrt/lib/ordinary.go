package gbrt

import (
	"context"
	"fmt"
	"image"
	"image/color"
	"log"
	"os"
)

// OrdinaryTracer is a standard implementation of a ray tracer for
// validation and performance comparison purposes.
func OrdinaryTracer(origin Vec, cfg ImageConfig, word, dir string) {
	// Code for progress tracking via pixels being worked on.
	maxProgress := cfg.Height * cfg.Width
	curProgress := int32(0)
	printed := false
	markProgress := func() {
		curProgress++
		pcnt := float64(curProgress) / float64(maxProgress)
		iPcnt := int(pcnt * 100)
		m := iPcnt % 10
		b := iPcnt % 2
		if m == 0 && !printed {
			fmt.Fprintf(os.Stderr, "%d", iPcnt)
			printed = true
		} else if b == 0 && !printed {
			fmt.Fprint(os.Stderr, ".")
			printed = true
		} else if b == 1 {
			printed = false
		}
	}

	scene := populateScene(word)
	scene.Initialize()

	img := image.NewRGBA(image.Rect(0, 0, int(cfg.Width), int(cfg.Height)))
	for y := 0; y < int(cfg.Height); y++ {
		for x := 0; x < int(cfg.Width); x++ {
			var colour Vec
			for p := 0; p < int(cfg.Samples); p++ {
				ray := subPixelJitter(x, y, cfg)
				result := Trace(origin, ray, scene, cfg.Bounces)
				colour = colour.Plus(result)
			}

			// Reinhard tone mapping
			colour = colour.Times(MonoVec(1. / float64(cfg.Samples))).Plus(MonoVec(14. / 241.))
			o := colour.Plus(MonoVec(1))
			colour = Vec{colour.X / o.X, colour.Y / o.Y, colour.Z / o.Z}.Times(MonoVec(255))

			img.Set(int(cfg.Width)-x-1, int(cfg.Height)-y-1, color.RGBA{uint8(colour.X), uint8(colour.Y), uint8(colour.Z), 255})
			markProgress()
		}
	}
	fmt.Fprint(os.Stderr, "\n")
	ctx := context.Background()
	output := OutputPath(dir, word, int(cfg.Samples))
	if err := writeToFile(ctx, output, img); err != nil {
		log.Panicf("Unable to create image file at %v: %v", output, err)
	}
}
