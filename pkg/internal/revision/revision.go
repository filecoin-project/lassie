// Package revision provides the vsc revision, embedded by the compiler, as a
// global variable.
package revision

import (
	"runtime/debug"
)

// Revision returns the revision embedded by the compiler during build.
// Suffixed with "-dirty" if modified.
var Revision string

func init() {
	revision := "unknown"
	var dirty bool

	// Get the revision from the build info
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		Revision = revision
		return
	}

	for _, bs := range bi.Settings {
		switch bs.Key {
		case "vcs.revision":
			revision = bs.Value
			if len(bs.Value) > 7 {
				revision = bs.Value[:7]
			}
		case "vcs.modified":
			if bs.Value == "true" {
				dirty = true
			}
		}
	}

	if dirty {
		revision += "-dirty"
	}

	Revision = revision
}
