package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/lassie/pkg/aggregateeventrecorder"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/storage"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/urfave/cli/v2"
)

const stdoutFileString string = "-" // a string representing stdout

var fetchFlags = []cli.Flag{
	&cli.StringFlag{
		Name:      "output",
		Aliases:   []string{"o"},
		Usage:     "the CAR file to write to, may be an existing or a new CAR, or use '-' to write to stdout",
		TakesFile: true,
	},
	&cli.BoolFlag{
		Name:    "progress",
		Aliases: []string{"p"},
		Usage:   "print progress output",
	},
	&cli.StringFlag{
		Name:        "dag-scope",
		Usage:       "describes the fetch behavior at the end of the traversal path. Valid values include [all, entity, block].",
		DefaultText: "defaults to all, the entire DAG at the end of the path will be fetched",
		Value:       "all",
		Action: func(cctx *cli.Context, v string) error {
			switch v {
			case string(types.DagScopeAll):
			case string(types.DagScopeEntity):
			case string(types.DagScopeBlock):
			default:
				return fmt.Errorf("invalid dag-scope parameter, must be of value [all, entity, block]")
			}

			return nil
		},
	},
	FlagIPNIEndpoint,
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
	FlagGlobalTimeout,
	FlagProviderTimeout,
}

var fetchCmd = &cli.Command{
	Name:   "fetch",
	Usage:  "Fetches content from the IPFS and Filecoin network",
	After:  after,
	Action: fetchAction,
	Flags:  fetchFlags,
}

func fetchAction(cctx *cli.Context) error {
	if cctx.Args().Len() != 1 {
		return fmt.Errorf("usage: lassie fetch [-o <CAR file>] [-t <timeout>] <CID>[/path/to/content]")
	}

	msgWriter := cctx.App.ErrWriter
	dataWriter := cctx.App.Writer

	dagScope := cctx.String("dag-scope")
	tempDir := cctx.String("tempdir")
	progress := cctx.Bool("progress")

	rootCid, path, err := parseCidPath(cctx.Args().Get(0))
	if err != nil {
		return err
	}

	output := cctx.String("output")
	outfile := fmt.Sprintf("%s.car", rootCid)
	if output != "" {
		outfile = output
	}

	lassieCfg, err := buildLassieConfigFromCLIContext(cctx, nil, nil)
	if err != nil {
		return err
	}

	eventRecorderURL := cctx.String("event-recorder-url")
	authToken := cctx.String("event-recorder-auth")
	instanceID := cctx.String("event-recorder-instance-id")
	eventRecorderCfg := getEventRecorderConfig(eventRecorderURL, authToken, instanceID)

	err = fetchRun(
		cctx.Context,
		lassieCfg,
		eventRecorderCfg,
		msgWriter,
		dataWriter,
		rootCid,
		path,
		dagScope,
		tempDir,
		progress,
		outfile,
	)
	if err != nil {
		return cli.Exit(err, 1)
	}

	return nil
}

func parseCidPath(cpath string) (cid.Cid, string, error) {
	cstr := strings.Split(cpath, "/")[0]
	path := strings.TrimPrefix(cpath, cstr)
	rootCid, err := cid.Parse(cstr)
	if err != nil {
		return cid.Undef, "", err
	}
	return rootCid, path, nil
}

type progressPrinter struct {
	candidatesFound int
	writer          io.Writer
}

func (pp *progressPrinter) subscriber(event types.RetrievalEvent) {
	switch ret := event.(type) {
	case events.StartedFindingCandidatesEvent:
		fmt.Fprintf(pp.writer, "\rQuerying indexer for %s...\n", ret.PayloadCid())
	case events.StartedRetrievalEvent:
		fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", events.Identifier(ret), ret.Code())
	case events.ConnectedToProviderEvent:
		fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", events.Identifier(ret), ret.Code())
	case events.GraphsyncProposedEvent:
		fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", events.Identifier(ret), ret.Code())
	case events.GraphsyncAcceptedEvent:
		fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", events.Identifier(ret), ret.Code())
	case events.FirstByteEvent:
		fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", events.Identifier(ret), ret.Code())
	case events.CandidatesFoundEvent:
		pp.candidatesFound = len(ret.Candidates())
	case events.CandidatesFilteredEvent:
		if len(fetchProviderAddrInfos) == 0 {
			fmt.Fprintf(pp.writer, "Found %d storage provider candidate(s) in the indexer:\n", pp.candidatesFound)
		} else {
			fmt.Fprintf(pp.writer, "Using the specified storage provider(s):\n")
		}
		for _, candidate := range ret.Candidates() {
			fmt.Fprintf(pp.writer, "\r\t%s, Protocols: %v\n", candidate.MinerPeer.ID, candidate.Metadata.Protocols())
		}
	case events.FailedEvent:
		fmt.Fprintf(pp.writer, "\rRetrieval failure from indexer: %s\n", ret.ErrorMessage())
	case events.FailedRetrievalEvent:
		fmt.Fprintf(pp.writer, "\rRetrieval failure for [%s]: %s\n", events.Identifier(ret), ret.ErrorMessage())
	case events.SucceededEvent:
		// noop, handled at return from Retrieve()
	}
}

type onlyWriter struct {
	w io.Writer
}

func (ow *onlyWriter) Write(p []byte) (n int, err error) {
	return ow.w.Write(p)
}

type fetchRunFunc func(
	ctx context.Context,
	lassieCfg *lassie.LassieConfig,
	eventRecorderCfg *aggregateeventrecorder.EventRecorderConfig,
	msgWriter io.Writer,
	dataWriter io.Writer,
	rootCid cid.Cid,
	path string,
	dagScope string,
	tempDir string,
	progress bool,
	outfile string,
) error

var fetchRun fetchRunFunc = defaultFetchRun

// defaultFetchRun is the handler for the fetch command.
// This abstraction allows the fetch command to be invoked
// programmatically for testing.
func defaultFetchRun(
	ctx context.Context,
	lassieCfg *lassie.LassieConfig,
	eventRecorderCfg *aggregateeventrecorder.EventRecorderConfig,
	msgWriter io.Writer,
	dataWriter io.Writer,
	rootCid cid.Cid,
	path string,
	dagScope string,
	tempDir string,
	progress bool,
	outfile string,
) error {
	lassie, err := lassie.NewLassieWithConfig(ctx, lassieCfg)
	if err != nil {
		return err
	}

	// create and subscribe an event recorder API if an endpoint URL is set
	if eventRecorderCfg.EndpointURL != "" {
		setupLassieEventRecorder(ctx, eventRecorderCfg, lassie)
	}

	if len(fetchProviderAddrInfos) == 0 {
		fmt.Fprintf(msgWriter, "Fetching %s", rootCid.String()+path)
	} else {
		fmt.Fprintf(msgWriter, "Fetching %s from specified provider(s)", rootCid.String()+path)
	}
	if progress {
		fmt.Fprintln(msgWriter)
		pp := &progressPrinter{writer: msgWriter}
		lassie.RegisterSubscriber(pp.subscriber)
	}

	var carWriter *storage.DeferredCarWriter
	if outfile == stdoutFileString {
		// we need the onlyWriter because stdout is presented as an os.File, and
		// therefore pretend to support seeks, so feature-checking in go-car
		// will make bad assumptions about capabilities unless we hide it
		carWriter = storage.NewDeferredCarWriterForStream(rootCid, &onlyWriter{dataWriter})
	} else {
		carWriter = storage.NewDeferredCarWriterForPath(rootCid, outfile)
	}

	tempStore := storage.NewDeferredStorageCar(tempDir, rootCid)
	carStore := storage.NewCachingTempStore(carWriter.BlockWriteOpener(), tempStore)
	defer carStore.Close()

	var blockCount int
	var byteLength uint64
	carWriter.OnPut(func(putBytes int) {
		blockCount++
		byteLength += uint64(putBytes)
		if !progress {
			fmt.Fprint(msgWriter, ".")
		} else {
			fmt.Fprintf(msgWriter, "\rReceived %d blocks / %s...", blockCount, humanize.IBytes(byteLength))
		}
	}, false)

	request, err := types.NewRequestForPath(carStore, rootCid, path, types.DagScope(dagScope))
	if err != nil {
		return err
	}
	// setup preload storage for bitswap, the temporary CAR store can set up a
	// separate preload space in its storage
	request.PreloadLinkSystem = cidlink.DefaultLinkSystem()
	preloadStore := carStore.PreloadStore()
	request.PreloadLinkSystem.SetReadStorage(preloadStore)
	request.PreloadLinkSystem.SetWriteStorage(preloadStore)
	request.PreloadLinkSystem.TrustedStorage = true

	stats, err := lassie.Fetch(ctx, request, func(types.RetrievalEvent) {})
	if err != nil {
		fmt.Fprintln(msgWriter)
		return err
	}
	spid := stats.StorageProviderId
	if spid == "" {
		spid = types.BitswapIndentifier
	}
	fmt.Fprintf(msgWriter, "\nFetched [%s] from [%s]:\n"+
		"\tDuration: %s\n"+
		"\t  Blocks: %d\n"+
		"\t   Bytes: %s\n",
		rootCid,
		spid,
		stats.Duration,
		blockCount,
		humanize.IBytes(stats.Size),
	)

	return nil
}
