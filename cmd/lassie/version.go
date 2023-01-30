package main

import (
	"fmt"

	lcli "github.com/filecoin-project/lassie/pkg/cli"
	"github.com/urfave/cli/v2"
)

var version string // supplied during build with `go build -ldflags="-X main.version=v0.0.0"`

type Version struct {
	Version string `json:"version"`
}

var versionCmd = &cli.Command{
	Name:      "version",
	Before:    before,
	Usage:     "Prints the version and exits",
	UsageText: "lassie version",
	Flags: []cli.Flag{
		lcli.FlagVerbose,
	},
	Action: versionCommand,
}

func versionCommand(cctx *cli.Context) error {
	if version == "" {
		log.Warn("executable built without a version")
		log.Warn("set version with `go build -ldflags=\"-X main.version=v0.0.0\"")
		version = "[not set]"
	}

	fmt.Printf("lassie version %s\n", version)
	return nil
}
