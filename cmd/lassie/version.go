package cmd

import (
	"fmt"
	"runtime/debug"

	"github.com/urfave/cli/v2"
)

var versionCmd = &cli.Command{
	Name:      "version",
	Before:    before,
	Usage:     "Prints the version and exits",
	UsageText: "lassie version",
	Action:    versionCommand,
}

func versionCommand(cctx *cli.Context) error {
	// buildVersion will be populated if we're running an official built binary
	if buildVersion != "" {
		fmt.Printf("lassie version %s\n", buildVersion)
		return nil
	}

	// build info main version will be populated if installed from commit hash
	info, ok := debug.ReadBuildInfo()
	if ok && info.Main.Version != "(devel)" {
		fmt.Printf("lassie version %s\n", info.Main.Version)
		return nil
	}

	// otherwise we don't know the version
	fmt.Println("lassie version (devel)")
	return nil
}
