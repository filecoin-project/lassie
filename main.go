package main

import cmd "github.com/filecoin-project/lassie/cmd/lassie"

var version string // supplied during build with `go build -ldflags="-X main.version=v0.0.0"`

func main() {
	cmd.Run(version)
}
