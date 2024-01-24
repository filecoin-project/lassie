package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/filecoin-project/lassie/pkg/aggregateeventrecorder"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/net/host"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/google/uuid"
	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/config"
	"github.com/urfave/cli/v2"
)

var logger = log.Logger("lassie/main")

func main() {
	// set up a context that is canceled when a command is interrupted
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// set up a signal handler to cancel the context
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)

		select {
		case <-interrupt:
			fmt.Println()
			logger.Info("received interrupt signal")
			cancel()
		case <-ctx.Done():
		}

		// Allow any further SIGTERM or SIGINT to kill process
		signal.Stop(interrupt)
	}()

	app := &cli.App{
		Name:    "lassie",
		Usage:   "Utility for retrieving content from the Filecoin network",
		Suggest: true,
		Flags: []cli.Flag{
			FlagVerbose,
			FlagVeryVerbose,
		},
		Commands: []*cli.Command{
			daemonCmd,
			fetchCmd,
			versionCmd,
		},
	}

	if err := app.RunContext(ctx, os.Args); err != nil {
		logger.Fatal(err)
	}
}

func after(cctx *cli.Context) error {
	ResetGlobalFlags()
	return nil
}

func buildLassieConfigFromCLIContext(cctx *cli.Context, lassieOpts []lassie.LassieOption, libp2pOpts []config.Option) (*lassie.LassieConfig, error) {
	providerTimeout := cctx.Duration("provider-timeout")
	globalTimeout := cctx.Duration("global-timeout")
	bitswapConcurrency := cctx.Int("bitswap-concurrency")
	bitswapConcurrencyPerRetrieval := cctx.Int("bitswap-concurrency-per-retrieval")

	lassieOpts = append(lassieOpts, lassie.WithProviderTimeout(providerTimeout))

	if globalTimeout > 0 {
		lassieOpts = append(lassieOpts, lassie.WithGlobalTimeout(globalTimeout))
	}

	if len(protocols) > 0 {
		lassieOpts = append(lassieOpts, lassie.WithProtocols(protocols))
	}

	host, err := host.InitHost(cctx.Context, libp2pOpts)
	if err != nil {
		return nil, err
	}
	lassieOpts = append(lassieOpts, lassie.WithHost(host))

	if len(fetchProviderAddrInfos) > 0 {
		finderOpt := lassie.WithCandidateSource(retriever.NewDirectCandidateSource(host, fetchProviderAddrInfos))
		if cctx.IsSet("ipni-endpoint") {
			logger.Warn("Ignoring ipni-endpoint flag since direct provider is specified")
		}
		lassieOpts = append(lassieOpts, finderOpt)
	} else if cctx.IsSet("ipni-endpoint") {
		endpoint := cctx.String("ipni-endpoint")
		endpointUrl, err := url.ParseRequestURI(endpoint)
		if err != nil {
			logger.Errorw("Failed to parse IPNI endpoint as URL", "err", err)
			return nil, fmt.Errorf("cannot parse given IPNI endpoint %s as valid URL: %w", endpoint, err)
		}
		finder, err := indexerlookup.NewCandidateSource(indexerlookup.WithHttpEndpoint(endpointUrl))
		if err != nil {
			logger.Errorw("Failed to instantiate IPNI candidate finder", "err", err)
			return nil, err
		}
		lassieOpts = append(lassieOpts, lassie.WithCandidateSource(finder))
		logger.Debug("Using explicit IPNI endpoint to find candidates", "endpoint", endpoint)
	}

	if len(providerBlockList) > 0 {
		lassieOpts = append(lassieOpts, lassie.WithProviderBlockList(providerBlockList))
	}

	if bitswapConcurrency > 0 {
		lassieOpts = append(lassieOpts, lassie.WithBitswapConcurrency(bitswapConcurrency))
	}

	if bitswapConcurrencyPerRetrieval > 0 {
		lassieOpts = append(lassieOpts, lassie.WithBitswapConcurrencyPerRetrieval(bitswapConcurrencyPerRetrieval))
	} else if bitswapConcurrency > 0 {
		lassieOpts = append(lassieOpts, lassie.WithBitswapConcurrencyPerRetrieval(bitswapConcurrency))
	}

	return lassie.NewLassieConfig(lassieOpts...), nil
}

func getEventRecorderConfig(endpointURL string, authToken string, instanceID string) *aggregateeventrecorder.EventRecorderConfig {
	return &aggregateeventrecorder.EventRecorderConfig{
		InstanceID:            instanceID,
		EndpointURL:           endpointURL,
		EndpointAuthorization: authToken,
	}
}

// setupLassieEventRecorder creates and subscribes an EventRecorder if an event recorder URL is given
func setupLassieEventRecorder(
	ctx context.Context,
	cfg *aggregateeventrecorder.EventRecorderConfig,
	lassie *lassie.Lassie,
) {
	if cfg.EndpointURL != "" {
		if cfg.InstanceID == "" {
			uuid, err := uuid.NewRandom()
			if err != nil {
				logger.Warnw("failed to generate default event recorder instance ID UUID, no instance ID will be provided", "err", err)
			}
			cfg.InstanceID = uuid.String() // returns "" if uuid is invalid
		}

		eventRecorder := aggregateeventrecorder.NewAggregateEventRecorder(ctx, *cfg)
		lassie.RegisterSubscriber(eventRecorder.RetrievalEventSubscriber())
		logger.Infow("Reporting retrieval events to event recorder API", "url", cfg.EndpointURL, "instance_id", cfg.InstanceID)
	}
}
