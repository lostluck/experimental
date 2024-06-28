// beamgo is a convenience builder and launcher for Beam Go WASI SDK pipelines jobs.
//
// In particular it it properly configures the go toolchain to build a wasm binary
// for the WASI environment container.
//
// When targeting go code, it will invoke the go toolchain to build the worker binary.
// It then loads that binary into a wasi host to execute per the command line parameters.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
)

// Config handles configuring the launcher
type Config struct {
	// Launch
	Launch string
}

func initFlags() *Config {
	var cfg Config
	flag.StringVar(&cfg.Launch, "launch", "", "used to specify a wasm binary to execute")
	return &cfg
}

func main() {
	cfg := initFlags()
	flag.Parse()

	ctx := context.Background()

	// Compile the go binary as wasm if necessary.
	if cfg.Launch == "" {
		cfg.Launch = "pipeline.wasm"
		goCmd := exec.CommandContext(ctx, "go", "build", "-o "+cfg.Launch, "*.go")
		goCmd.Env = append(goCmd.Env, "GOOS=wasip1", "GOARCH=wasm")
		if err := goCmd.Run(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	// Start the local wasi host
	// TODO, remember how to do that
}
