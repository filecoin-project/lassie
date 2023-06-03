package main

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/net/host"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/storage"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
)

var fetchProviderAddrInfos []peer.AddrInfo

var fetchCmd = &cli.Command{
	Name:   "fetch",
	Usage:  "Fetches content from the IPFS and Filecoin network",
	Before: before,
	Action: Fetch,
	Flags: []cli.Flag{
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
		&cli.StringFlag{
			Name:        "providers",
			Aliases:     []string{"provider"},
			DefaultText: "Providers will be discovered automatically",
			Usage:       "Addresses of providers, including peer IDs, to use instead of automatic discovery, seperated by a comma. All protocols will be attempted when connecting to these providers. Example: /ip4/1.2.3.4/tcp/1234/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
			Action: func(cctx *cli.Context, v string) error {
				// Do nothing if given an empty string
				if v == "" {
					return nil
				}

				var err error
				fetchProviderAddrInfos, err = types.ParseProviderStrings(v)
				return err
			},
		},
		&cli.StringFlag{
			Name:        "ipni-endpoint",
			Aliases:     []string{"ipni"},
			DefaultText: "Defaults to https://cid.contact",
			Usage:       "HTTP endpoint of the IPNI instance used to discover providers.",
		},
		FlagEventRecorderAuth,
		FlagEventRecorderInstanceId,
		FlagEventRecorderUrl,
		FlagVerbose,
		FlagVeryVerbose,
		FlagProtocols,
		FlagExcludeProviders,
		FlagTempDir,
		FlagBitswapConcurrency,
		FlagGlobalTimeout,
		FlagProviderTimeout,
	},
}

func Fetch(cctx *cli.Context) error {
	if cctx.Args().Len() != 1 {
		return fmt.Errorf("usage: lassie fetch [-o <CAR file>] [-t <timeout>] <CID>[/path/to/content]")
	}

	ctx := cctx.Context
	msgWriter := cctx.App.ErrWriter
	dataWriter := cctx.App.Writer

	dagScope := cctx.String("dag-scope")
	tempDir := cctx.String("tempdir")
	eventRecorderURL := cctx.String("event-recorder-url")
	authToken := cctx.String("event-recorder-auth")
	instanceID := cctx.String("event-recorder-instance-id")
	progress := cctx.Bool("progress")

	rootCid, path, err := parseCidPath(cctx.Args().Get(0))
	if err != nil {
		return err
	}

	lassieOpts := []lassie.LassieOption{}

	host, err := host.InitHost(ctx, []libp2p.Option{})
	if err != nil {
		return err
	}
	lassieOpts = append(lassieOpts, lassie.WithHost(host))

	if len(fetchProviderAddrInfos) > 0 {
		finderOpt := lassie.WithFinder(retriever.NewDirectCandidateFinder(host, fetchProviderAddrInfos))
		if cctx.IsSet("ipni-endpoint") {
			logger.Warn("Ignoring ipni-endpoint flag since direct provider is specified")
		}
		lassieOpts = append(lassieOpts, finderOpt)
	} else if cctx.IsSet("ipni-endpoint") {
		endpoint := cctx.String("ipni-endpoint")
		endpointUrl, err := url.Parse(endpoint)
		if err != nil {
			logger.Errorw("Failed to parse IPNI endpoint as URL", "err", err)
			return fmt.Errorf("cannot parse given IPNI endpoint %s as valid URL: %w", endpoint, err)
		}
		finder, err := indexerlookup.NewCandidateFinder(indexerlookup.WithHttpEndpoint(endpointUrl))
		if err != nil {
			logger.Errorw("Failed to instantiate IPNI candidate finder", "err", err)
			return err
		}
		lassieOpts = append(lassieOpts, lassie.WithFinder(finder))
		logger.Debug("Using explicit IPNI endpoint to find candidates", "endpoint", endpoint)
	}

	if tempDir != "" {
		lassieOpts = append(lassieOpts, lassie.WithTempDir(tempDir))
	} else {
		tempDir = os.TempDir()
	}

	lassie, err := BuildLassieFromCLIContext(cctx, lassieOpts)
	if err != nil {
		return err
	}

	// create and subscribe an event recorder API if configured
	setupLassieEventRecorder(ctx, eventRecorderURL, authToken, instanceID, lassie)

	if len(fetchProviderAddrInfos) == 0 {
		fmt.Fprintf(msgWriter, "Fetching %s", rootCid.String()+path)
	} else {
		fmt.Fprintf(msgWriter, "Fetching %s from %v", rootCid.String()+path, fetchProviderAddrInfos)
	}
	if progress {
		fmt.Fprintln(msgWriter)
		pp := &progressPrinter{writer: msgWriter}
		lassie.RegisterSubscriber(pp.subscriber)
	}

	outfile := fmt.Sprintf("%s.car", rootCid)
	if cctx.IsSet("output") {
		outfile = cctx.String("output")
	}

	var carWriter *storage.DeferredCarWriter
	if outfile == "-" { // stdout
		// we need the onlyWriter because stdout is presented as an os.File, and
		// therefore pretend to support seeks, so feature-checking in go-car
		// will make bad assumptions about capabilities unless we hide it
		carWriter = storage.NewDeferredCarWriterForStream(rootCid, &onlyWriter{dataWriter})
	} else {
		carWriter = storage.NewDeferredCarWriterForPath(rootCid, outfile)
	}
	tempStore := storage.NewDeferredStorageCar(tempDir)
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
	fmt.Fprintf(msgWriter, "\nFetched [%s] from [%s]:\n"+
		"\tDuration: %s\n"+
		"\t  Blocks: %d\n"+
		"\t   Bytes: %s\n",
		rootCid,
		stats.StorageProviderId,
		stats.Duration,
		blockCount,
		humanize.IBytes(stats.Size),
	)

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
	case events.RetrievalEventStarted:
		switch ret.Phase() {
		case types.IndexerPhase:
			fmt.Fprintf(pp.writer, "\rQuerying indexer for %s...\n", ret.PayloadCid())
		case types.QueryPhase:
			fmt.Fprintf(pp.writer, "\rQuerying [%s] (%s)...\n", types.Identifier(ret), ret.Code())
		case types.RetrievalPhase:
			fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", types.Identifier(ret), ret.Code())
		}
	case events.RetrievalEventConnected:
		switch ret.Phase() {
		case types.QueryPhase:
			fmt.Fprintf(pp.writer, "\rQuerying [%s] (%s)...\n", types.Identifier(ret), ret.Code())
		case types.RetrievalPhase:
			fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", types.Identifier(ret), ret.Code())
		}
	case events.RetrievalEventProposed:
		fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", types.Identifier(ret), ret.Code())
	case events.RetrievalEventAccepted:
		fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", types.Identifier(ret), ret.Code())
	case events.RetrievalEventFirstByte:
		fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", types.Identifier(ret), ret.Code())
	case events.RetrievalEventCandidatesFound:
		pp.candidatesFound = len(ret.Candidates())
	case events.RetrievalEventCandidatesFiltered:
		num := "all of them"
		if pp.candidatesFound != len(ret.Candidates()) {
			num = fmt.Sprintf("%d of them", len(ret.Candidates()))
		} else if pp.candidatesFound == 1 {
			num = "it"
		}
		if len(fetchProviderAddrInfos) > 0 {
			fmt.Fprintf(pp.writer, "Found %d storage providers candidates from the indexer, querying %s:\n", pp.candidatesFound, num)
		} else {
			fmt.Fprintf(pp.writer, "Using the explicitly specified storage provider(s), querying %s:\n", num)
		}
		for _, candidate := range ret.Candidates() {
			fmt.Fprintf(pp.writer, "\r\t%s, Protocols: %v\n", candidate.MinerPeer.ID, candidate.Metadata.Protocols())
		}
	case events.RetrievalEventFailed:
		if ret.Phase() == types.IndexerPhase {
			fmt.Fprintf(pp.writer, "\rRetrieval failure from indexer: %s\n", ret.ErrorMessage())
		} else {
			fmt.Fprintf(pp.writer, "\rRetrieval failure for [%s]: %s\n", types.Identifier(ret), ret.ErrorMessage())
		}
	case events.RetrievalEventSuccess:
		// noop, handled at return from Retrieve()
	}
}

type onlyWriter struct {
	w io.Writer
}

func (ow *onlyWriter) Write(p []byte) (n int, err error) {
	return ow.w.Write(p)
}
