package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/lassie/pkg/events"
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
		&cli.DurationFlag{
			Name:    "provider-timeout",
			Aliases: []string{"pt"},
			Usage:   "consider it an error after not receiving a response from a storage provider after this amount of time",
			Value:   20 * time.Second,
		},
		&cli.DurationFlag{
			Name:    "global-timeout",
			Aliases: []string{"gt"},
			Usage:   "consider it an error after not completing the retrieval after this amount of time",
			Value:   0,
		},
		&cli.BoolFlag{
			Name:    "progress",
			Aliases: []string{"p"},
			Usage:   "print progress output",
		},
		&cli.BoolFlag{
			Name:        "shallow",
			Usage:       "only fetch the content at the end of the path",
			DefaultText: "false, the entire DAG at the end of the path will be fetched",
			Value:       false,
		},
		&cli.StringFlag{
			Name:        "providers",
			Aliases:     []string{"provider"},
			DefaultText: "Providers will be discovered automatically",
			Usage:       "Provider addresses including its peer ID, seperated by a comma. Example: /ip4/1.2.3.4/tcp/1234/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
			Action: func(cctx *cli.Context, v string) error {
				vs := strings.Split(v, ",")
				for _, v := range vs {
					fetchProviderAddrInfo, err := peer.AddrInfoFromString(v)
					if err != nil {
						return err
					}
					fetchProviderAddrInfos = append(fetchProviderAddrInfos, *fetchProviderAddrInfo)
				}
				return nil
			},
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
	},
}

func Fetch(cctx *cli.Context) error {
	if cctx.Args().Len() != 1 {
		return fmt.Errorf("usage: lassie fetch [-o <CAR file>] [-t <timeout>] <CID>[/path/to/content]")
	}

	ctx := cctx.Context
	msgWriter := cctx.App.ErrWriter
	dataWriter := cctx.App.Writer

	progress := cctx.Bool("progress")
	providerTimeout := cctx.Duration("provider-timeout")
	globalTimeout := cctx.Duration("global-timeout")
	shallow := cctx.Bool("shallow")
	tempDir := cctx.String("tempdir")
	bitswapConcurrency := cctx.Int("bitswap-concurrency")
	eventRecorderURL := cctx.String("event-recorder-url")
	authToken := cctx.String("event-recorder-auth")
	instanceID := cctx.String("event-recorder-instance-id")

	rootCid, path, err := parseCidPath(cctx.Args().Get(0))
	if err != nil {
		return err
	}

	providerTimeoutOpt := lassie.WithProviderTimeout(providerTimeout)

	host, err := host.InitHost(ctx, []libp2p.Option{})
	if err != nil {
		return err
	}
	hostOpt := lassie.WithHost(host)
	var lassieOpts = []lassie.LassieOption{providerTimeoutOpt, hostOpt}

	if len(fetchProviderAddrInfos) > 0 {
		finderOpt := lassie.WithFinder(retriever.NewDirectCandidateFinder(host, fetchProviderAddrInfos))
		lassieOpts = append(lassieOpts, finderOpt)
	}

	if len(providerBlockList) > 0 {
		opts = append(opts, lassie.WithProviderBlockList(providerBlockList))
	}

	if len(protocols) > 0 {
		lassieOpts = append(lassieOpts, lassie.WithProtocols(protocols))
	}

	if globalTimeout > 0 {
		lassieOpts = append(lassieOpts, lassie.WithGlobalTimeout(globalTimeout))
	}

	if tempDir != "" {
		lassieOpts = append(lassieOpts, lassie.WithTempDir(tempDir))
	} else {
		tempDir = os.TempDir()
	}

	if bitswapConcurrency > 0 {
		lassieOpts = append(lassieOpts, lassie.WithBitswapConcurrency(bitswapConcurrency))
	}

	lassie, err := lassie.NewLassie(ctx, lassieOpts...)
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
		carWriter = storage.NewDeferredCarWriterForStream(rootCid, &onlyWriter{dataWriter})
	} else {
		carWriter = storage.NewDeferredCarWriterForPath(rootCid, outfile)
	}
	carStore := storage.NewTeeingTempReadWrite(carWriter.BlockWriteOpener(), tempDir)
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

	request, err := types.NewRequestForPath(carStore, rootCid, path, !shallow)
	if err != nil {
		return err
	}
	// setup preload storage for bitswap, the temporary CAR store can set up a
	// separate preload space in its storage
	request.PreloadLinkSystem = cidlink.DefaultLinkSystem()
	preloadStore := carStore.PreloadStore()
	request.PreloadLinkSystem.SetReadStorage(preloadStore)
	request.PreloadLinkSystem.SetWriteStorage(preloadStore)

	stats, err := lassie.Fetch(ctx, request)
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
