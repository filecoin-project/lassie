package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/filecoin-project/lassie/pkg/aggregateeventrecorder"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/google/uuid"
	"github.com/ipfs/go-log/v2"
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

func before(cctx *cli.Context) error {
	// Determine logging level
	subsystems := []string{
		"lassie",
		"lassie/httpserver",
		"indexerlookup",
		"lassie/bitswap",
	}

	level := "WARN"
	if IsVerbose {
		level = "INFO"
	}
	if IsVeryVerbose {
		level = "DEBUG"
	}

	// don't over-ride logging if set in the environment.
	if os.Getenv("GOLOG_LOG_LEVEL") == "" {
		for _, name := range subsystems {
			_ = log.SetLogLevel(name, level)
		}
	}

	return nil
}

// setupLassieEventRecorder creates and subscribes an EventRecorder if an event recorder URL is given
func setupLassieEventRecorder(
	ctx context.Context,
	eventRecorderURL string,
	authToken string,
	instanceID string,
	lassie *lassie.Lassie,
) {
	if eventRecorderURL != "" {
		if instanceID == "" {
			uuid, err := uuid.NewRandom()
			if err != nil {
				logger.Warnw("failed to generate default event recorder instance ID UUID, no instance ID will be provided", "err", err)
			}
			instanceID = uuid.String() // returns "" if uuid is invalid
		}

		eventRecorder := aggregateeventrecorder.NewAggregateEventRecorder(ctx, aggregateeventrecorder.EventRecorderConfig{
			InstanceID:            instanceID,
			EndpointURL:           eventRecorderURL,
			EndpointAuthorization: authToken,
		})
		lassie.RegisterSubscriber(eventRecorder.RetrievalEventSubscriber())
		logger.Infow("Reporting retrieval events to event recorder API", "url", eventRecorderURL, "instance_id", instanceID)
	}
}
