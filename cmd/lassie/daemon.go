package main

import (
	"fmt"

	httpserver "github.com/filecoin-project/lassie/pkg/server/http"
	"github.com/urfave/cli/v2"
)

var daemonFlags = []cli.Flag{
	&cli.StringFlag{
		Name:        "address",
		Aliases:     []string{"a"},
		Usage:       "the address the http server listens on",
		Value:       "127.0.0.1",
		DefaultText: "127.0.0.1",
		EnvVars:     []string{"LASSIE_ADDRESS"},
	},
	&cli.UintFlag{
		Name:        "port",
		Aliases:     []string{"p"},
		Usage:       "the port the http server listens on",
		Value:       0,
		DefaultText: "random",
		EnvVars:     []string{"LASSIE_PORT"},
	},
	FlagVerbose,
	FlagVeryVerbose,
}

var daemonCmd = &cli.Command{
	Name:   "daemon",
	Usage:  "Starts a lassie daemon, accepting http requests",
	Before: before,
	Flags:  daemonFlags,
	Action: daemonCommand,
}

func daemonCommand(cctx *cli.Context) error {
	address := cctx.String("address")
	port := cctx.Uint("port")
	httpServer, err := httpserver.NewHttpServer(cctx.Context, address, port)
	if err != nil {
		log.Errorw("failed to create http server", "err", err)
		return err
	}

	serverErrChan := make(chan error, 1)
	go func() {
		fmt.Printf("Lassie daemon listening on address %s\n", httpServer.Addr())
		fmt.Println("Hit CTRL-C to stop the daemon")
		serverErrChan <- httpServer.Start()
	}()

	select {
	case <-cctx.Done(): // command was cancelled
	case err = <-serverErrChan: // error from server
		log.Errorw("failed to start http server", "err", err)
	}

	fmt.Println("Shutting down Lassie daemon")
	if err = httpServer.Close(); err != nil {
		log.Errorw("failed to close http server", "err", err)
	}

	fmt.Println("Lassie daemon stopped")
	if err != nil {
		return cli.Exit(err, 1)
	}

	return nil
}
