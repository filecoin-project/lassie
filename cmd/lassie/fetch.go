package main

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"runtime/pprof"
	"strings"
	"sync/atomic"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/lassie/pkg/aggregateeventrecorder"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/extractor"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/storage"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage/deferred"
	"github.com/ipld/go-ipld-prime/datamodel"
	trustlessutils "github.com/ipld/go-trustless-utils"
	trustlesshttp "github.com/ipld/go-trustless-utils/http"
	"github.com/urfave/cli/v2"
)

const stdoutFileString string = "-" // a string representing stdout

var fetchFlags = []cli.Flag{
	&cli.StringFlag{
		Name:    "output",
		Aliases: []string{"o"},
		Usage: "the CAR file to write to, may be an existing or a new CAR, " +
			"or use '-' to write to stdout",
		TakesFile: true,
	},
	&cli.BoolFlag{
		Name:    "progress",
		Aliases: []string{"p"},
		Usage:   "print verbose progress including provider events",
	},
	&cli.BoolFlag{
		Name:    "quiet",
		Aliases: []string{"q"},
		Usage:   "suppress progress output",
	},
	&cli.StringFlag{
		Name: "dag-scope",
		Usage: "describes the fetch behavior at the end of the traversal " +
			"path. Valid values include [all, entity, block].",
		DefaultText: "defaults to all, the entire DAG at the end of the path will " +
			"be fetched",
		Value: "all",
		Action: func(cctx *cli.Context, v string) error {
			switch v {
			case string(trustlessutils.DagScopeAll):
			case string(trustlessutils.DagScopeEntity):
			case string(trustlessutils.DagScopeBlock):
			default:
				return fmt.Errorf("invalid dag-scope parameter, must be of value " +
					"[all, entity, block]")
			}
			return nil
		},
	},
	&cli.StringFlag{
		Name: "entity-bytes",
		Usage: "describes the byte range to consider when selecting the blocks " +
			"from a sharded file. Valid values should be of the form from:to, where " +
			"from and to are byte offsets and to may be '*'",
		DefaultText: "defaults to the entire file, 0:*",
		Action: func(cctx *cli.Context, v string) error {
			if _, err := trustlessutils.ParseByteRange(v); err != nil {
				return fmt.Errorf("invalid entity-bytes parameter, must be of the " +
					"form from:to, where from and to are byte offsets and to may be '*'")
			}
			return nil
		},
	},
	&cli.BoolFlag{
		Name:    "stream",
		Usage:   "stream blocks directly to output; disable to use temp files for deduplication",
		Value:   true,
		Aliases: []string{"s"},
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
	FlagSkipBlockVerification,
	&cli.StringFlag{
		Name:  "cpuprofile",
		Usage: "write cpu profile to file",
	},
	&cli.BoolFlag{
		Name:  "extract",
		Usage: "extract UnixFS content to files instead of CAR output",
	},
	&cli.StringFlag{
		Name:  "extract-to",
		Usage: "directory to extract files to (default: current directory)",
		Value: ".",
	},
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
		// "help" becomes a subcommand, clear it to deal with a urfave/cli bug
		// Ref: https://github.com/urfave/cli/blob/v2.25.7/help.go#L253-L255
		cctx.Command.Subcommands = nil
		cli.ShowCommandHelpAndExit(cctx, "fetch", 0)
		return nil
	}

	if cpuprofile := cctx.String("cpuprofile"); cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			return fmt.Errorf("could not create CPU profile: %w", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return fmt.Errorf("could not start CPU profile: %w", err)
		}
		defer pprof.StopCPUProfile()
	}

	msgWriter := cctx.App.ErrWriter
	dataWriter := cctx.App.Writer

	root, path, scope, byteRange, stream, err := parseCidPath(cctx.Args().Get(0))
	if err != nil {
		return err
	}

	if cctx.IsSet("dag-scope") {
		if scope, err = trustlessutils.ParseDagScope(cctx.String("dag-scope")); err != nil {
			return err
		}
	}

	if cctx.IsSet("entity-bytes") {
		if entityBytes, err := trustlessutils.ParseByteRange(cctx.String("entity-bytes")); err != nil {
			return err
		} else if entityBytes.IsDefault() {
			byteRange = nil
		} else {
			byteRange = &entityBytes
		}
	}

	if cctx.IsSet("stream") {
		stream = cctx.Bool("stream")
	}

	tempDir := cctx.String("tempdir")
	progress := cctx.Bool("progress")
	quiet := cctx.Bool("quiet")

	output := cctx.String("output")
	outfile := fmt.Sprintf("%s.car", root.String())
	if output != "" {
		outfile = output
	}

	extractMode := cctx.Bool("extract")
	extractTo := cctx.String("extract-to")

	// validate flags
	if extractMode && cctx.IsSet("output") {
		return fmt.Errorf("--extract and --output are mutually exclusive")
	}

	lassieCfg, err := buildLassieConfigFromCLIContext(cctx, nil)
	if err != nil {
		return err
	}

	eventRecorderURL := cctx.String("event-recorder-url")
	authToken := cctx.String("event-recorder-auth")
	instanceID := cctx.String("event-recorder-instance-id")
	eventRecorderCfg := getEventRecorderConfig(eventRecorderURL, authToken, instanceID)

	if extractMode {
		err = extractRun(
			cctx.Context,
			lassieCfg,
			eventRecorderCfg,
			msgWriter,
			root,
			path,
			scope,
			byteRange,
			progress,
			quiet,
			extractTo,
		)
	} else {
		err = fetchRun(
			cctx.Context,
			lassieCfg,
			eventRecorderCfg,
			msgWriter,
			dataWriter,
			root,
			path,
			scope,
			byteRange,
			stream,
			tempDir,
			progress,
			quiet,
			outfile,
		)
	}
	if err != nil {
		return cli.Exit(err, 1)
	}

	return nil
}

func parseCidPath(spec string) (
	root cid.Cid,
	path datamodel.Path,
	scope trustlessutils.DagScope,
	byteRange *trustlessutils.ByteRange,
	stream bool,
	err error,
) {
	scope = trustlessutils.DagScopeAll // default
	stream = true                      // default to streaming mode

	if !strings.HasPrefix(spec, "/ipfs/") {
		cstr := strings.Split(spec, "/")[0]
		path = datamodel.ParsePath(strings.TrimPrefix(spec, cstr))
		if root, err = cid.Parse(cstr); err != nil {
			return cid.Undef, datamodel.Path{}, trustlessutils.DagScopeAll, nil, true, err
		}
		return root, path, scope, byteRange, stream, err
	} else {
		specParts := strings.Split(spec, "?")
		spec = specParts[0]

		if root, path, err = trustlesshttp.ParseUrlPath(spec); err != nil {
			return cid.Undef, datamodel.Path{}, trustlessutils.DagScopeAll, nil, true, err
		}

		switch len(specParts) {
		case 1:
		case 2:
			query, err := url.ParseQuery(specParts[1])
			if err != nil {
				return cid.Undef, datamodel.Path{}, trustlessutils.DagScopeAll, nil, true, err
			}
			scope, err = trustlessutils.ParseDagScope(query.Get("dag-scope"))
			if err != nil {
				return cid.Undef, datamodel.Path{}, trustlessutils.DagScopeAll, nil, true, err
			}
			if query.Get("entity-bytes") != "" {
				br, err := trustlessutils.ParseByteRange(query.Get("entity-bytes"))
				if err != nil {
					return cid.Undef, datamodel.Path{}, trustlessutils.DagScopeAll, nil, true, err
				}
				byteRange = &br
			}
			if query.Has("dups") {
				stream = query.Get("dups") == "y"
			}
		default:
			return cid.Undef, datamodel.Path{}, trustlessutils.DagScopeAll, nil, true, fmt.Errorf("invalid query: %s", spec)
		}

		return root, path, scope, byteRange, stream, nil
	}
}

type progressPrinter struct {
	candidatesFound int
	writer          io.Writer
}

func (pp *progressPrinter) subscriber(event types.RetrievalEvent) {
	switch ret := event.(type) {
	case events.StartedFindingCandidatesEvent:
		fmt.Fprintf(pp.writer, "\rQuerying indexer for %s...\n", ret.RootCid())
	case events.StartedRetrievalEvent:
		fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", events.Identifier(ret), ret.Code())
	case events.ConnectedToProviderEvent:
		fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", events.Identifier(ret), ret.Code())
	case events.FirstByteEvent:
		fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", events.Identifier(ret), ret.Code())
	case events.CandidatesFoundEvent:
		pp.candidatesFound = len(ret.Candidates())
	case events.CandidatesFilteredEvent:
		if len(fetchProviders) == 0 {
			fmt.Fprintf(pp.writer, "Found %d provider candidate(s) in the indexer:\n", pp.candidatesFound)
		} else {
			fmt.Fprintf(pp.writer, "Using the specified provider(s):\n")
		}
		for _, candidate := range ret.Candidates() {
			fmt.Fprintf(pp.writer, "\r\t%s\n", candidate.Endpoint())
		}
	case events.FailedEvent:
		fmt.Fprintf(pp.writer, "\rRetrieval failure from indexer: %s\n", ret.ErrorMessage())
	case events.FailedRetrievalEvent:
		fmt.Fprintf(pp.writer, "\rRetrieval failure for [%s]: %s\n", events.Identifier(ret), ret.ErrorMessage())
	case events.SucceededEvent:
		// noop, handled at return from Retrieve()
	case events.ExtractionStartedEvent:
		fmt.Fprintf(pp.writer, "Streaming from [%s]...\n", ret.Endpoint())
	case events.ExtractionSucceededEvent:
		// noop, handled at return from Extract()
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
	path datamodel.Path,
	dagScope trustlessutils.DagScope,
	entityBytes *trustlessutils.ByteRange,
	stream bool,
	tempDir string,
	progress bool,
	quiet bool,
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
	path datamodel.Path,
	dagScope trustlessutils.DagScope,
	entityBytes *trustlessutils.ByteRange,
	stream bool,
	tempDir string,
	progress bool,
	quiet bool,
	outfile string,
) error {
	s, err := lassie.NewLassieWithConfig(ctx, lassieCfg)
	if err != nil {
		return err
	}

	// create and subscribe an event recorder API if an endpoint URL is set
	if eventRecorderCfg.EndpointURL != "" {
		setupLassieEventRecorder(ctx, eventRecorderCfg, s)
	}

	printPath := path.String()
	if printPath != "" {
		printPath = "/" + printPath
	}
	if len(fetchProviders) == 0 {
		fmt.Fprintf(msgWriter, "Fetching %s\n", rootCid.String()+printPath)
	} else {
		fmt.Fprintf(msgWriter, "Fetching %s from specified provider(s)\n", rootCid.String()+printPath)
	}
	if progress {
		pp := &progressPrinter{writer: msgWriter}
		s.RegisterSubscriber(pp.subscriber)
	}

	carOpts := []car.Option{
		car.WriteAsCarV1(true),
		car.StoreIdentityCIDs(false),
		car.UseWholeCIDs(false),
	}

	var carStore types.ReadableWritableStorage
	var carWriter storage.DeferredWriter

	if stream {
		var deferredWriter *deferred.DeferredCarWriter
		streamOpts := []car.Option{
			car.WriteAsCarV1(true),
			car.AllowDuplicatePuts(true),
			car.StoreIdentityCIDs(false),
			car.UseWholeCIDs(true),
		}
		if outfile == stdoutFileString {
			deferredWriter = deferred.NewDeferredCarWriterForStream(&onlyWriter{dataWriter}, []cid.Cid{rootCid}, streamOpts...)
		} else {
			deferredWriter = deferred.NewDeferredCarWriterForPath(outfile, []cid.Cid{rootCid}, streamOpts...)
		}
		carWriter = deferredWriter
		carStore = storage.NewStreamingStore(deferredWriter.BlockWriteOpener())
	} else {
		tempStore := storage.NewDeferredStorageCar(tempDir, rootCid)
		if outfile == stdoutFileString {
			carWriter = deferred.NewDeferredCarWriterForStream(&onlyWriter{dataWriter}, []cid.Cid{rootCid}, carOpts...)
		} else {
			carWriter = deferred.NewDeferredCarWriterForPath(outfile, []cid.Cid{rootCid}, carOpts...)
		}
		carStore = storage.NewCachingTempStore(carWriter.BlockWriteOpener(), tempStore)
	}
	defer carWriter.Close()
	defer func() {
		if closer, ok := carStore.(interface{ Close() error }); ok {
			closer.Close()
		}
	}()

	var blockCount int
	var byteLength uint64
	carWriter.OnPut(func(putBytes int) {
		blockCount++
		byteLength += uint64(putBytes)
		if !quiet && !progress {
			fmt.Fprintf(msgWriter, "\rReceived %d blocks / %s...", blockCount, humanize.IBytes(byteLength))
		}
	}, false)

	request, err := types.NewRequestForPath(carStore, rootCid, path.String(), dagScope, entityBytes)
	if err != nil {
		return err
	}
	request.Duplicates = stream

	stats, err := s.Fetch(ctx, request)
	if err != nil {
		fmt.Fprintln(msgWriter)
		return err
	}
	providers := stats.Providers
	if len(providers) == 0 && stats.StorageProviderId.String() != "" {
		providers = []string{stats.StorageProviderId.String()}
	}
	providerStr := "Unknown"
	if len(providers) > 0 {
		providerStr = strings.Join(providers, ", ")
	}
	fmt.Fprintf(msgWriter, "\nFetched [%s] from [%s]:\n"+
		"\tDuration: %s\n"+
		"\t  Blocks: %d\n"+
		"\t   Bytes: %s\n",
		rootCid,
		providerStr,
		stats.Duration,
		blockCount,
		humanize.IBytes(stats.Size),
	)

	return nil
}

// extractRun handles extraction mode - extracting UnixFS content to files
func extractRun(
	ctx context.Context,
	lassieCfg *lassie.LassieConfig,
	eventRecorderCfg *aggregateeventrecorder.EventRecorderConfig,
	msgWriter io.Writer,
	rootCid cid.Cid,
	path datamodel.Path,
	dagScope trustlessutils.DagScope,
	entityBytes *trustlessutils.ByteRange,
	progress bool,
	quiet bool,
	extractTo string,
) error {
	// path and scope/byteRange not yet supported in streaming extraction
	if path.Len() > 0 {
		return fmt.Errorf("path traversal not yet supported in streaming extraction")
	}
	if dagScope != trustlessutils.DagScopeAll {
		return fmt.Errorf("dag-scope other than 'all' not yet supported in streaming extraction")
	}
	if entityBytes != nil {
		return fmt.Errorf("entity-bytes not yet supported in streaming extraction")
	}

	s, err := lassie.NewLassieWithConfig(ctx, lassieCfg)
	if err != nil {
		return err
	}

	if eventRecorderCfg.EndpointURL != "" {
		setupLassieEventRecorder(ctx, eventRecorderCfg, s)
	}

	// create extractor
	ext, err := extractor.New(extractTo)
	if err != nil {
		return fmt.Errorf("failed to create extractor: %w", err)
	}
	defer ext.Close()

	// set root path context
	ext.SetRootPath(rootCid, rootCid.String())

	printPath := path.String()
	if printPath != "" {
		printPath = "/" + printPath
	}
	if len(fetchProviders) == 0 {
		fmt.Fprintf(msgWriter, "Extracting %s to %s\n", rootCid.String()+printPath, extractTo)
	} else {
		fmt.Fprintf(msgWriter, "Extracting %s to %s from specified provider(s)\n", rootCid.String()+printPath, extractTo)
	}
	if progress {
		pp := &progressPrinter{writer: msgWriter}
		s.RegisterSubscriber(pp.subscriber)
	}

	var blockCount int64
	var byteLength uint64
	onBlock := func(putBytes int) {
		bc := atomic.AddInt64(&blockCount, 1)
		bl := atomic.AddUint64(&byteLength, uint64(putBytes))
		if !quiet && !progress {
			fmt.Fprintf(msgWriter, "\rReceived %d blocks / %s...", bc, humanize.IBytes(bl))
		}
	}

	stats, err := s.Extract(ctx, rootCid, ext, nil, onBlock)
	if err != nil {
		fmt.Fprintln(msgWriter)
		return err
	}

	providers := stats.Providers
	if len(providers) == 0 && stats.StorageProviderId.String() != "" {
		providers = []string{stats.StorageProviderId.String()}
	}
	providerStr := "Unknown"
	if len(providers) > 0 {
		providerStr = strings.Join(providers, ", ")
	}
	fmt.Fprintf(msgWriter, "\nExtracted [%s] from [%s]:\n"+
		"\tDuration: %s\n"+
		"\t  Blocks: %d\n"+
		"\t   Bytes: %s\n"+
		"\t      To: %s\n",
		rootCid,
		providerStr,
		stats.Duration,
		atomic.LoadInt64(&blockCount),
		humanize.IBytes(atomic.LoadUint64(&byteLength)),
		extractTo,
	)

	return nil
}
