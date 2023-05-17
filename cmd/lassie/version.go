package main

import (
	"fmt"

	"github.com/filecoin-project/lassie/pkg/build"
	"github.com/urfave/cli/v2"
)

var versionCmd = &cli.Command{
	Name:      "version",
	Before:    before,
	Usage:     "Prints the version and exits",
	UsageText: "lassie version",
	Flags: []cli.Flag{
		FlagVerbose,
	},
	Action: versionCommand,
}

func versionCommand(cctx *cli.Context) error {
	fmt.Printf("lassie version %s\n", build.Version)
	return nil
}
