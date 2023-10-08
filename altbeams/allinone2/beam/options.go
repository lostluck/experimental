package beam

import "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"

// Options configure Run, ParDo, and Combine with specific features.
// Each function takes a variadic list of options, where properties
// set in later options override the value of previously set properties.
type Options = beamopts.Options

// Name sets the name of the pipeline or transform in question, typically
// to make it easier to refer to.
func Name(name string) Options {
	return &beamopts.Struct{
		Name: name,
	}
}

// Endpoint sets the url when applicable, such as the JobManagement endpoint for submitting jobs
// or for configuring a target for expansion services.
func Endpoint(endpoint string) Options {
	return &beamopts.Struct{
		Endpoint: endpoint,
	}
}
