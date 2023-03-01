package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	cmdinternal "github.com/filecoin-project/lassie/cmd/lassie/internal"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	carstore "github.com/ipld/go-car/v2/storage"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
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
			Usage:     "the CAR file to write to, may be an existing or a new CAR",
			TakesFile: true,
		},
		&cli.DurationFlag{
			Name:    "timeout",
			Aliases: []string{"t"},
			Usage:   "consider it an error after not receiving a response from a storage provider for this long",
			Value:   20 * time.Second,
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

	timeout := c.Duration("timeout")
	timeoutOpt := lassie.WithProviderTimeout(timeout)

	host, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
	if err != nil {
		return err
	}
	hostOpt := lassie.WithHost(host)
	var opts = []lassie.LassieOption{timeoutOpt, hostOpt}
	if len(fetchProviderAddrInfos) > 0 {
		finderOpt := lassie.WithFinder(retriever.NewDirectCandidateFinder(host, fetchProviderAddrInfos))
		opts = append(opts, finderOpt)
	}

	lassie, err := lassie.NewLassie(c.Context, opts...)
	if err != nil {
		return err
	}

	// create and subscribe an event recorder API if configured
	setupLassieEventRecorder(c, lassie)

	if len(fetchProviderAddrInfos) == 0 {
		fmt.Printf("Fetching %s", rootCid.String()+path)
	} else {
		fmt.Printf("Fetching %s from %v", rootCid.String()+path, fetchProviderAddrInfos)
	}
	if progress {
		fmt.Println()
		pp := &progressPrinter{}
		lassie.RegisterSubscriber(pp.subscriber)
	}

	outfile := fmt.Sprintf("%s.car", rootCid)
	if c.IsSet("output") {
		outfile = c.String("output")
	}
	var openedFile *os.File
	defer func() {
		if openedFile != nil {
			openedFile.Close()
		}
	}()

	// TODO: unfortunately errors from here have to propagate through graphsync
	// then data-transfer and all the way back up to retrieval; we should
	// probably have a way to cancel the retrieval and return an error
	// immediately if this fails.
	var parentOpener = func() (*carstore.StorageCar, error) {
		var err error
		// always Create, truncating and making a new store - can't resume here
		// because our headers (roots) won't match
		// TODO: option to resume existing CAR and ignore mismatching header? it
		// could just append blocks and leave the header as it is
		openedFile, err = os.Create(outfile)
		if err != nil {
			return nil, err
		}
		return carstore.NewReadableWritable(openedFile, []cid.Cid{rootCid}, carv2.WriteAsCarV1(true))
	}

	var blockCount int
	var byteLength uint64
	putCb := func(putCount int, putBytes int) {
		blockCount += putCount
		byteLength += uint64(putBytes)
		if !progress {
			fmt.Print(strings.Repeat(".", putCount))
		} else {
			fmt.Printf("\rReceived %d blocks / %s...", blockCount, humanize.IBytes(byteLength))
		}
	}

	store := cmdinternal.NewPutCbStore(parentOpener, putCb)
	request, err := types.NewRequestForPath(store, rootCid, path, !c.Bool("shallow"))
	if err != nil {
		return err
	}

	stats, err := lassie.Fetch(c.Context, request)
	if err != nil {
		fmt.Println()
		return err
	}
	fmt.Printf("\nFetched [%s] from [%s]:\n"+
		"\tDuration: %s\n"+
		"\t  Blocks: %d\n"+
		"\t   Bytes: %s\n",
		rootCid,
		stats.StorageProviderId,
		stats.Duration,
		blockCount,
		humanize.IBytes(stats.Size),
	)

	return store.Finalize()
}

type progressPrinter struct {
	candidatesFound int
}

func (pp *progressPrinter) subscriber(event types.RetrievalEvent) {
	switch ret := event.(type) {
	case events.RetrievalEventStarted:
		switch ret.Phase() {
		case types.IndexerPhase:
			fmt.Printf("\rQuerying indexer for %s...\n", ret.PayloadCid())
		case types.QueryPhase:
			fmt.Printf("\rQuerying [%s] (%s)...\n", types.Identifier(ret), ret.Code())
		case types.RetrievalPhase:
			fmt.Printf("\rRetrieving from [%s] (%s)...\n", types.Identifier(ret), ret.Code())
		}
	case events.RetrievalEventConnected:
		switch ret.Phase() {
		case types.QueryPhase:
			fmt.Printf("\rQuerying [%s] (%s)...\n", types.Identifier(ret), ret.Code())
		case types.RetrievalPhase:
			fmt.Printf("\rRetrieving from [%s] (%s)...\n", types.Identifier(ret), ret.Code())
		}
	case events.RetrievalEventProposed:
		fmt.Printf("\rRetrieving from [%s] (%s)...\n", types.Identifier(ret), ret.Code())
	case events.RetrievalEventAccepted:
		fmt.Printf("\rRetrieving from [%s] (%s)...\n", types.Identifier(ret), ret.Code())
	case events.RetrievalEventFirstByte:
		fmt.Printf("\rRetrieving from [%s] (%s)...\n", types.Identifier(ret), ret.Code())
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
			fmt.Printf("Found %d storage providers candidates from the indexer, querying %s:\n", pp.candidatesFound, num)
		} else {
			fmt.Printf("Using the explicitly specified storage provider(s), querying %s:\n", num)
		}
		for _, candidate := range ret.Candidates() {
			fmt.Printf("\r\t%s, Protocols: %v\n", candidate.MinerPeer.ID, candidate.Metadata.Protocols())
		}
	case events.RetrievalEventQueryAsked:
		fmt.Printf("\rGot query response from [%s] (checking): size=%s, price-per-byte=%s, unseal-price=%s, message=%s\n", types.Identifier(ret), humanize.IBytes(ret.QueryResponse().Size), ret.QueryResponse().MinPricePerByte, ret.QueryResponse().UnsealPrice, ret.QueryResponse().Message)
	case events.RetrievalEventQueryAskedFiltered:
		fmt.Printf("\rGot query response from [%s] (filtered): size=%s, price-per-byte=%s, unseal-price=%s, message=%s\n", types.Identifier(ret), humanize.IBytes(ret.QueryResponse().Size), ret.QueryResponse().MinPricePerByte, ret.QueryResponse().UnsealPrice, ret.QueryResponse().Message)
	case events.RetrievalEventFailed:
		if ret.Phase() == types.IndexerPhase {
			fmt.Printf("\rRetrieval failure from indexer: %s\n", ret.ErrorMessage())
		} else {
			fmt.Printf("\rRetrieval failure for [%s]: %s\n", types.Identifier(ret), ret.ErrorMessage())
		}
	case events.RetrievalEventSuccess:
		// noop, handled at return from Retrieve()
	}
}
