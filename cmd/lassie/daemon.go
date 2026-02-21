package main

import (
	"context"
	"fmt"

	"github.com/filecoin-project/lassie/pkg/aggregateeventrecorder"
	"github.com/filecoin-project/lassie/pkg/lassie"
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
	&cli.Uint64Flag{
		Name:        "maxblocks",
		Aliases:     []string{"mb"},
		Usage:       "maximum number of blocks sent before closing connection",
		Value:       0,
		DefaultText: "no limit",
		EnvVars:     []string{"LASSIE_MAX_BLOCKS_PER_REQUEST"},
	},
	FlagDelegatedRoutingEndpoint,
	FlagEventRecorderAuth,
	FlagEventRecorderInstanceId,
	FlagEventRecorderUrl,
	FlagVerbose,
	FlagVeryVerbose,
	FlagAllowProviders,
	FlagExcludeProviders,
	FlagTempDir,
	FlagGlobalTimeout,
	&cli.StringFlag{
		Name:  "access-token",
		Usage: "require HTTP clients to authorize using Bearer scheme and given access token",
		Value: "",
	},
}

var daemonCmd = &cli.Command{
	Name:   "daemon",
	Usage:  "Starts a lassie daemon, accepting http requests",
	After:  after,
	Flags:  daemonFlags,
	Action: daemonAction,
}

// daemonAction is the cli action for the daemon command. This function is
// called by the cli framework when the daemon command is invoked. It translates
// the cli context into the appropriate config objects and then calls the
// daemonRun function.
func daemonAction(cctx *cli.Context) error {
	lassieOpts := []lassie.LassieOption{}

	lassieCfg, err := buildLassieConfigFromCLIContext(cctx, lassieOpts)
	if err != nil {
		return err
	}

	// http server config
	address := cctx.String("address")
	port := cctx.Uint("port")
	tempDir := cctx.String("tempdir")
	maxBlocks := cctx.Uint64("maxblocks")
	accessToken := cctx.String("access-token")
	httpServerCfg := getHttpServerConfigForDaemon(address, port, tempDir, maxBlocks, accessToken)

	// event recorder config
	eventRecorderURL := cctx.String("event-recorder-url")
	authToken := cctx.String("event-recorder-auth")
	instanceID := cctx.String("event-recorder-instance-id")
	eventRecorderCfg := getEventRecorderConfig(eventRecorderURL, authToken, instanceID)

	err = daemonRun(
		cctx.Context,
		lassieCfg,
		httpServerCfg,
		eventRecorderCfg,
	)
	if err != nil {
		return cli.Exit(err, 1)
	}

	return nil
}

// daemonRunFunc is the function signature for the daemonRun function.
type daemonRunFunc func(
	ctx context.Context,
	lassieCfg *lassie.LassieConfig,
	httpServerCfg httpserver.HttpServerConfig,
	eventRecorderCfg *aggregateeventrecorder.EventRecorderConfig,
) error

// daemonRun is the instance of a daemonRunFunc function that will
// execute when running the daemon command. It is set to
// defaultDaemonRun by default, but can be replaced for testing.
var daemonRun daemonRunFunc = defaultDaemonRun

// defaultDaemonRun is the default implementation for the daemonRun function.
func defaultDaemonRun(
	ctx context.Context,
	lassieCfg *lassie.LassieConfig,
	httpServerCfg httpserver.HttpServerConfig,
	eventRecorderCfg *aggregateeventrecorder.EventRecorderConfig,
) error {
	s, err := lassie.NewLassieWithConfig(ctx, lassieCfg)
	if err != nil {
		return err
	}

	// create and subscribe an event recorder API if an endpoint URL is set
	if eventRecorderCfg.EndpointURL != "" {
		setupLassieEventRecorder(ctx, eventRecorderCfg, s)
	}

	httpServer, err := httpserver.NewHttpServer(ctx, s, httpServerCfg)
	if err != nil {
		logger.Errorw("failed to create http server", "err", err)
		return err
	}

	serverErrChan := make(chan error, 1)
	go func() {
		fmt.Printf("Lassie daemon listening on address %s\n", httpServer.Addr())
		fmt.Println("Hit CTRL-C to stop the daemon")
		serverErrChan <- httpServer.Start()
	}()

	select {
	case <-ctx.Done(): // command was cancelled
	case err = <-serverErrChan: // error from server
		logger.Errorw("failed to start http server", "err", err)
	}

	fmt.Println("Shutting down Lassie daemon")
	if err = httpServer.Close(); err != nil {
		logger.Errorw("failed to close http server", "err", err)
	}

	fmt.Println("Lassie daemon stopped")
	return err
}

// getHttpServerConfigForDaemon returns a HttpServerConfig for the daemon command.
func getHttpServerConfigForDaemon(address string, port uint, tempDir string, maxBlocks uint64, accessToken string) httpserver.HttpServerConfig {
	return httpserver.HttpServerConfig{
		Address:             address,
		Port:                port,
		TempDir:             tempDir,
		MaxBlocksPerRequest: maxBlocks,
		AccessToken:         accessToken,
	}
}
