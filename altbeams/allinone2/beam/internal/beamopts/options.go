package beamopts

import "github.com/lostluck/experimental/altbeams/allinone2/beam/internal"

// Options is the common options type shared across beam packages.
type Options interface {
	// JSONOptions is exported so related beam packages can implement Options.
	BeamOptions(internal.NotForPublicUse)
}

// Struct is the combination of all options in struct form.
// This is efficient to pass down the call stack and to query.
type Struct struct {
	Name     string // The configured name of the options target. Otherwise it's autogeneerated.
	Endpoint string // The configured URL for a service.
}

func (dst *Struct) BeamOptions(internal.NotForPublicUse) {}

func (dst *Struct) Join(srcs ...Options) {
	for _, src := range srcs {
		switch src := src.(type) {
		case *Struct:
			if src.Name != "" {
				dst.Name = src.Name
			}
			if src.Endpoint != "" {
				dst.Endpoint = src.Endpoint
			}
		}
	}
}
