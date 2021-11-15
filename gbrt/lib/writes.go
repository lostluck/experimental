package gbrt

import (
	"bufio"
	"context"
	"fmt"
	"image"
	"image/png"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

func writeToFile(ctx context.Context, path string, img image.Image) error {
	fs, err := filesystem.New(ctx, path)
	if err != nil {
		return err
	}
	defer fs.Close()

	fd, err := fs.OpenWrite(ctx, path)
	if err != nil {
		return fmt.Errorf("Unable to create image file at %v: %v", path, err)
	}
	buf := bufio.NewWriterSize(fd, 1<<20) // use 1MB buffer

	log.Infof(ctx, "Writing to %v", path)
	if err := png.Encode(buf, img); err != nil {
		return fmt.Errorf("Unable to encode image to buffer at: %v", err)
	}
	if err := buf.Flush(); err != nil {
		return fmt.Errorf("Unable to flush buffer at %v: %v", path, err)
	}
	return fd.Close()
}
