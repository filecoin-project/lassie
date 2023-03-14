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
	},
}

func Fetch(c *cli.Context) error {
	if c.Args().Len() != 1 {
		return fmt.Errorf("usage: lassie fetch [-o <CAR file>] [-t <timeout>] <CID>[/path/to/content]")
	}
	progress := c.Bool("progress")

	cpath := c.Args().Get(0)
	cstr := strings.Split(cpath, "/")[0]
	path := strings.TrimPrefix(cpath, cstr)
	rootCid, err := cid.Parse(cstr)
	if err != nil {
		return err
	}

	providerTimeout := c.Duration("provider-timeout")
	providerTimeoutOpt := lassie.WithProviderTimeout(providerTimeout)

	host, err := host.InitHost(c.Context, []libp2p.Option{})
	if err != nil {
		return err
	}
	hostOpt := lassie.WithHost(host)
	var opts = []lassie.LassieOption{providerTimeoutOpt, hostOpt}

	if len(fetchProviderAddrInfos) > 0 {
		finderOpt := lassie.WithFinder(retriever.NewDirectCandidateFinder(host, fetchProviderAddrInfos))
		opts = append(opts, finderOpt)
	}

	if len(providerBlockList) > 0 {
		opts = append(opts, lassie.WithProviderBlockList(providerBlockList))
	}

	if len(protocols) > 0 {
		opts = append(opts, lassie.WithProtocols(protocols))
	}

	globalTimeout := c.Duration("global-timeout")
	if globalTimeout > 0 {
		opts = append(opts, lassie.WithGlobalTimeout(globalTimeout))
	}

	lassie, err := lassie.NewLassie(c.Context, opts...)
	if err != nil {
		return err
	}

	// create and subscribe an event recorder API if configured
	setupLassieEventRecorder(c, lassie)

	if len(fetchProviderAddrInfos) == 0 {
		fmt.Fprintf(c.App.ErrWriter, "Fetching %s", rootCid.String()+path)
	} else {
		fmt.Fprintf(c.App.ErrWriter, "Fetching %s from %v", rootCid.String()+path, fetchProviderAddrInfos)
	}
	if progress {
		fmt.Fprintln(c.App.ErrWriter)
		pp := &progressPrinter{writer: c.App.ErrWriter}
		lassie.RegisterSubscriber(pp.subscriber)
	}

	outfile := fmt.Sprintf("%s.car", rootCid)
	if c.IsSet("output") {
		outfile = c.String("output")
	}

	var carWriter *storage.DeferredCarWriter
	if outfile == "-" { // stdout
		carWriter = storage.NewDeferredCarWriterForStream(rootCid, &onlyWriter{c.App.Writer})
	} else {
		carWriter = storage.NewDeferredCarWriterForPath(rootCid, outfile)
	}
	store := storage.NewTeeingTempReadWrite(carWriter.BlockWriteOpener(), os.TempDir())
	defer store.Close()

	var blockCount int
	var byteLength uint64
	carWriter.OnPut(func(putBytes int) {
		blockCount++
		byteLength += uint64(putBytes)
		if !progress {
			fmt.Fprint(c.App.ErrWriter, ".")
		} else {
			fmt.Fprintf(c.App.ErrWriter, "\rReceived %d blocks / %s...", blockCount, humanize.IBytes(byteLength))
		}
	}, false)

	request, err := types.NewRequestForPath(store, rootCid, path, !c.Bool("shallow"))
	if err != nil {
		return err
	}

	stats, err := lassie.Fetch(c.Context, request)
	if err != nil {
		fmt.Fprintln(c.App.ErrWriter)
		return err
	}
	fmt.Fprintf(c.App.ErrWriter, "\nFetched [%s] from [%s]:\n"+
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
