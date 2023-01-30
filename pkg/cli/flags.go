package cli

import "github.com/urfave/cli/v2"

// IsVerbose is a global var signaling if the CLI is running in
// verbose mode or not (default: false).
var IsVerbose bool

// FlagVerbose enables verbose mode, which shows verbose information about
// operations invoked in the CLI. It should be included as a flag on the
// top-level command (e.g. lassie -v).
var FlagVerbose = &cli.BoolFlag{
	Name:        "verbose",
	Aliases:     []string{"v"},
	Usage:       "enable verbose mode for logging",
	Destination: &IsVerbose,
}

// IsVeryVerbose is a global var signaling if the CLI is running in
// very verbose mode or not (default: false).
var IsVeryVerbose bool

// FlagVerbose enables verbose mode, which shows verbose information about
// operations invoked in the CLI. It should be included as a flag on the
// top-level command (e.g. lassie -v).
var FlagVeryVerbose = &cli.BoolFlag{
	Name:        "very-verbose",
	Aliases:     []string{"vv"},
	Usage:       "enable very verbose mode for debugging",
	Destination: &IsVeryVerbose,
}
