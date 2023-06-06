package main

import (
	"fmt"
	"os"
	"time"

	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/net/host"
	"github.com/filecoin-project/lassie/pkg/retriever"
	httpserver "github.com/filecoin-project/lassie/pkg/server/http"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
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
	&cli.IntFlag{
		Name:        "libp2p-conns-lowwater",
		Aliases:     []string{"lw"},
		Usage:       "lower limit of libp2p connections",
		Value:       0,
		DefaultText: "libp2p default",
		EnvVars:     []string{"LASSIE_LIBP2P_CONNECTIONS_LOWWATER"},
	},
	&cli.IntFlag{
		Name:        "libp2p-conns-highwater",
		Aliases:     []string{"hw"},
		Usage:       "upper limit of libp2p connections",
		Value:       0,
		DefaultText: "libp2p default",
		EnvVars:     []string{"LASSIE_LIBP2P_CONNECTIONS_HIGHWATER"},
	},
	&cli.UintFlag{
		Name:        "concurrent-sp-retrievals",
		Aliases:     []string{"cr"},
		Usage:       "max number of simultaneous SP retrievals",
		Value:       0,
		DefaultText: "no limit",
		EnvVars:     []string{"LASSIE_CONCURRENT_SP_RETRIEVALS"},
	},
	&cli.DurationFlag{
		Name:    "provider-timeout",
		Aliases: []string{"pt"},
		Usage:   "consider it an error after not receiving a response from a storage provider after this amount of time",
		Value:   20 * time.Second,
		EnvVars: []string{"LASSIE_PROVIDER_TIMEOUT"},
	},
	&cli.DurationFlag{
		Name:    "global-timeout",
		Aliases: []string{"gt"},
		Usage:   "consider it an error after not completing a retrieval after this amount of time",
		Value:   0,
		EnvVars: []string{"LASSIE_GLOBAL_TIMEOUT"},
	},
	FlagEventRecorderAuth,
	FlagEventRecorderInstanceId,
	FlagEventRecorderUrl,
	FlagVerbose,
	FlagVeryVerbose,
	FlagProtocols,
	FlagAllowProviders,
	FlagExcludeProviders,
	FlagTempDir,
	FlagBitswapConcurrency,
}

var daemonCmd = &cli.Command{
	Name:   "daemon",
	Usage:  "Starts a lassie daemon, accepting http requests",
	Before: before,
	Flags:  daemonFlags,
	Action: daemonCommand,
}

func daemonCommand(cctx *cli.Context) error {
	ctx := cctx.Context

	address := cctx.String("address")
	port := cctx.Uint("port")
	tempDir := cctx.String("tempdir")
	maxBlocks := cctx.Uint64("maxblocks")
	libp2pLowWater := cctx.Int("libp2p-conns-lowwater")
	libp2pHighWater := cctx.Int("libp2p-conns-highwater")
	concurrentSPRetrievals := cctx.Uint("concurrent-sp-retrievals")
	providerTimeout := cctx.Duration("provider-timeout")
	globalTimeout := cctx.Duration("global-timeout")
	bitswapConcurrency := cctx.Int("bitswap-concurrency")
	eventRecorderURL := cctx.String("event-recorder-url")
	authToken := cctx.String("event-recorder-auth")
	instanceID := cctx.String("event-recorder-instance-id")

	lassieOpts := []lassie.LassieOption{lassie.WithProviderTimeout(providerTimeout)}
	if globalTimeout > 0 {
		lassieOpts = append(lassieOpts, lassie.WithGlobalTimeout(globalTimeout))
	}
	if libp2pHighWater != 0 || libp2pLowWater != 0 {
		connManager, err := connmgr.NewConnManager(libp2pLowWater, libp2pHighWater)
		if err != nil {
			return err
		}
		lassieOpts = append(
			lassieOpts,
			lassie.WithLibp2pOpts(libp2p.ConnectionManager(connManager)),
			lassie.WithConcurrentSPRetrievals(concurrentSPRetrievals),
		)
	}
	if len(protocols) > 0 {
		lassieOpts = append(lassieOpts, lassie.WithProtocols(protocols))
	}
	if len(fetchProviderAddrInfos) > 0 {
		host, err := host.InitHost(ctx, []libp2p.Option{})
		if err != nil {
			return err
		}
		finderOpt := lassie.WithFinder(retriever.NewDirectCandidateFinder(host, fetchProviderAddrInfos))
		lassieOpts = append(lassieOpts, finderOpt)
	}
	if len(providerBlockList) > 0 {
		lassieOpts = append(lassieOpts, lassie.WithProviderBlockList(providerBlockList))
	}
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	if bitswapConcurrency > 0 {
		lassieOpts = append(lassieOpts, lassie.WithBitswapConcurrency(bitswapConcurrency))
	}
	// create a lassie instance
	lassie, err := lassie.NewLassie(ctx, lassieOpts...)
	if err != nil {
		return err
	}

	// create and subscribe an event recorder API if configured
	setupLassieEventRecorder(ctx, eventRecorderURL, authToken, instanceID, lassie)

	httpServer, err := httpserver.NewHttpServer(ctx, lassie, httpserver.HttpServerConfig{
		Address:             address,
		Port:                port,
		TempDir:             tempDir,
		MaxBlocksPerRequest: maxBlocks,
	})

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
	case <-cctx.Done(): // command was cancelled
	case err = <-serverErrChan: // error from server
		logger.Errorw("failed to start http server", "err", err)
	}

	fmt.Println("Shutting down Lassie daemon")
	if err = httpServer.Close(); err != nil {
		logger.Errorw("failed to close http server", "err", err)
	}

	fmt.Println("Lassie daemon stopped")
	if err != nil {
		return cli.Exit(err, 1)
	}

	return nil
}
