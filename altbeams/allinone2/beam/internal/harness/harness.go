package harness

import (
	"context"
	"fmt"
)

// Options for harness.Main that affect execution of the harness, such as runner capabilities.
type Options struct {
	RunnerCapabilities []string // URNs for what runners are able to understand over the FnAPI.
	LoggingEndpoint    string   // Endpoint for remote logging.
	StatusEndpoint     string   // Endpoint for worker status reporting.
}

func Main(ctx context.Context, control string, opts Options) error {
	fmt.Printf("HARNESS STARTED: control %v; opts %v\n", control, opts)
	return nil
}
