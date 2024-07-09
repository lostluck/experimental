// Package prism downloads, unzips, boots up a prism binary to run a pipeline against.
package prism

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
)

// TODO: Allow configuration of port/ return of auto selected ports.
// TODO: Allow multiple jobs to hit the same process, but clean up when
// they are all done (so not with contextcommand.)

// Initialize cache directory.
// TODO move this to a central location.
func init() {
	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		panic("os.UserCacheDir: " + err.Error())
	}
	prismCache = path.Join(userCacheDir, "apache_beam/prism")
	prismBinCache = path.Join(prismCache, "bin")
}

var (
	prismCache    string
	prismBinCache string
)

const (
	beamVersion  = "v2.57.0"
	tagRoot      = "https://github.com/apache/beam/releases/tag"
	downloadRoot = "https://github.com/apache/beam/releases/download"
)

func constructDownloadPath(rootTag, version string) string {
	arch := runtime.GOARCH
	opsys := runtime.GOOS

	filename := fmt.Sprintf("apache_beam-%s-prism-%s-%s.zip", version, opsys, arch)

	return fmt.Sprintf("%s/%s/%s", downloadRoot, rootTag, filename)
}

func downloadToCache(url, local string) error {
	out, err := os.Create(local)
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check server response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// Writer the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

// unzipCachedFile extracts the output file from the zip file.
func unzipCachedFile(zipfile, outputDir string) (string, error) {
	zr, err := zip.OpenReader(zipfile)
	if err != nil {
		return "", err
	}
	defer zr.Close()

	br, err := zr.File[0].Open()
	if err != nil {
		return "", err
	}
	defer br.Close()

	output := path.Join(outputDir, zr.File[0].Name)

	out, err := os.Create(output)
	if err != nil {
		return "", err
	}
	defer out.Close()
	out.Chmod(0777) // Make file executable.

	if _, err := io.Copy(out, br); err != nil {
		return "", err
	}
	return output, nil
}

type Options struct {
	Location string // if specified, indicates where a prism binary or zip of the binary can be found.
}

// TODO handle multiple configurations of prism.
var (
	active  sync.WaitGroup
	started atomic.Bool
)

// Start downloads and begins a prism process.
//
// TODO return port or other info back up? Receive other options?
func Start(ctx context.Context, opts Options) error {
	active.Add(1)
	go func() {
		select {
		case <-ctx.Done():
			active.Done()
		}
	}()
	if started.Load() {
		// We're done here.
		return nil
	}

	localPath := opts.Location
	if localPath == "" {
		url := constructDownloadPath(beamVersion, beamVersion)

		// Ensure the cache exists.
		if err := os.MkdirAll(prismBinCache, 0777); err != nil {
			return err
		}
		basename := path.Base(url)
		localPath = filepath.Join(prismBinCache, basename)
		// Check if the zip is already in the cache.
		if _, err := os.Stat(localPath); err != nil {
			// Assume the file doesn't exist.
			if err := downloadToCache(url, localPath); err != nil {
				return fmt.Errorf("couldn't download %v to cache %s: %w", url, localPath, err)
			}
		}
	}
	bin, err := unzipCachedFile(localPath, prismBinCache)
	if err != nil {
		if !errors.Is(err, zip.ErrFormat) {
			return fmt.Errorf("couldn't unzip %q: %w", localPath, err)
		}
		// If it's a format error, assume it's an executable.
		bin = localPath
	}

	cmd := exec.Command(bin)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("couldn't start command %q: %w", bin, err)
	}
	fmt.Println("started prism")
	started.Store(true)
	go func() {
		active.Wait()
		cmd.Process.Kill()
		started.Store(false)
		fmt.Println("killed prism")
	}()
	return nil
}
