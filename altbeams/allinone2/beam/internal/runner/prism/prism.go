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
	beamVersion  = "v2.59.0-RC1"
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

// Start downloads and begins a prism process.
//
// Returns a cancellation function to be called once the process is no
// longer needed.
func Start(ctx context.Context, opts Options) (func(), error) {
	localPath := opts.Location
	if localPath == "" {
		url := constructDownloadPath(beamVersion, beamVersion)

		// Ensure the cache exists.
		if err := os.MkdirAll(prismBinCache, 0777); err != nil {
			return nil, err
		}
		basename := path.Base(url)
		localPath = filepath.Join(prismBinCache, basename)
		// Check if the zip is already in the cache.
		if _, err := os.Stat(localPath); err != nil {
			// Assume the file doesn't exist.
			if err := downloadToCache(url, localPath); err != nil {
				return nil, fmt.Errorf("couldn't download %v to cache %s: %w", url, localPath, err)
			}
		}
	}
	bin, err := unzipCachedFile(localPath, prismBinCache)
	if err != nil {
		if !errors.Is(err, zip.ErrFormat) {
			return nil, fmt.Errorf("couldn't unzip %q: %w", localPath, err)
		}
		// If it's a format error, assume it's an executable.
		bin = localPath
	}

	cmd := exec.Command(bin)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("couldn't start command %q: %w", bin, err)
	}
	fmt.Println("started prism")
	return func() {
		cmd.Process.Kill()
		fmt.Println("killed prism")
	}, nil
}
