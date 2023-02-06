package main

import (
	"context"
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
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
)

var fetchProviderAddrInfo *peer.AddrInfo

var fetchCmd = &cli.Command{
	Name:   "fetch",
	Usage:  "Fetches content from Filecoin",
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
		&cli.StringFlag{
			Name:        "provider",
			DefaultText: "The provider will be discovered automatically",
			Usage:       "The provider addr including its peer ID. Example: /ip4/1.2.3.4/tcp/1234/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
			Action: func(cctx *cli.Context, v string) error {
				var err error
				fetchProviderAddrInfo, err = peer.AddrInfoFromString(v)
				return err
			},
		},
		FlagVerbose,
		FlagVeryVerbose,
	},
}

func Fetch(c *cli.Context) error {
	if c.Args().Len() != 1 {
		return fmt.Errorf("usage: lassie fetch [-o <CAR file>] [-t <timeout>] <CID>")
	}
	progress := c.Bool("progress")

	rootCid, err := cid.Parse(c.Args().Get(0))
	if err != nil {
		return err
	}

	timeout := c.Duration("timeout")
	timeoutOpt := lassie.WithTimeout(timeout)

	var opts = []lassie.LassieOption{timeoutOpt}
	if fetchProviderAddrInfo != nil {
		finderOpt := lassie.WithFinder(explicitCandidateFinder{provider: *fetchProviderAddrInfo})
		opts = append(opts, finderOpt)
	}

	lassie, err := lassie.NewLassie(c.Context, opts...)
	if err != nil {
		return err
	}

	if fetchProviderAddrInfo == nil {
		fmt.Printf("Fetching %s", rootCid)
	} else {
		fmt.Printf("Fetching %s from %s", rootCid, fetchProviderAddrInfo.String())
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
	linkSystem := cidlink.DefaultLinkSystem()
	linkSystem.SetReadStorage(store)
	linkSystem.SetWriteStorage(store)

	_, stats, err := lassie.Fetch(c.Context, rootCid, linkSystem)
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
			fmt.Printf("\rQuerying [%s] (%s)...\n", ret.StorageProviderId(), ret.Code())
		case types.RetrievalPhase:
			fmt.Printf("\rRetrieving from [%s] (%s)...\n", ret.StorageProviderId(), ret.Code())
		}
	case events.RetrievalEventConnected:
		switch ret.Phase() {
		case types.QueryPhase:
			fmt.Printf("\rQuerying [%s] (%s)...\n", ret.StorageProviderId(), ret.Code())
		case types.RetrievalPhase:
			fmt.Printf("\rRetrieving from [%s] (%s)...\n", ret.StorageProviderId(), ret.Code())
		}
	case events.RetrievalEventProposed:
		fmt.Printf("\rRetrieving from [%s] (%s)...\n", ret.StorageProviderId(), ret.Code())
	case events.RetrievalEventAccepted:
		fmt.Printf("\rRetrieving from [%s] (%s)...\n", ret.StorageProviderId(), ret.Code())
	case events.RetrievalEventFirstByte:
		fmt.Printf("\rRetrieving from [%s] (%s)...\n", ret.StorageProviderId(), ret.Code())
	case events.RetrievalEventCandidatesFound:
		pp.candidatesFound = len(ret.Candidates())
	case events.RetrievalEventCandidatesFiltered:
		num := "all of them"
		if pp.candidatesFound != len(ret.Candidates()) {
			num = fmt.Sprintf("%d of them", len(ret.Candidates()))
		} else if pp.candidatesFound == 1 {
			num = "it"
		}
		if fetchProviderAddrInfo == nil {
			fmt.Printf("Found %d storage providers candidates from the indexer, querying %s:\n", pp.candidatesFound, num)
		} else {
			fmt.Printf("Using the explicitly specified storage provider, querying %s:\n", num)
		}
		for _, candidate := range ret.Candidates() {
			fmt.Printf("\r\t%s\n", candidate.MinerPeer.ID)
		}
	case events.RetrievalEventQueryAsked:
		fmt.Printf("\rGot query response from [%s] (checking): size=%s, price-per-byte=%s, unseal-price=%s, message=%s\n", ret.StorageProviderId(), humanize.IBytes(ret.QueryResponse().Size), ret.QueryResponse().MinPricePerByte, ret.QueryResponse().UnsealPrice, ret.QueryResponse().Message)
	case events.RetrievalEventQueryAskedFiltered:
		fmt.Printf("\rGot query response from [%s] (filtered): size=%s, price-per-byte=%s, unseal-price=%s, message=%s\n", ret.StorageProviderId(), humanize.IBytes(ret.QueryResponse().Size), ret.QueryResponse().MinPricePerByte, ret.QueryResponse().UnsealPrice, ret.QueryResponse().Message)
	case events.RetrievalEventFailed:
		fmt.Printf("\rRetrieval failure for [%s]: %s\n", ret.StorageProviderId(), ret.ErrorMessage())
	case events.RetrievalEventSuccess:
		// noop, handled at return from Retrieve()
	}
}

var _ retriever.CandidateFinder = (*explicitCandidateFinder)(nil)

type explicitCandidateFinder struct {
	provider peer.AddrInfo
}

func (e explicitCandidateFinder) FindCandidatesAsync(ctx context.Context, c cid.Cid) (<-chan types.FindCandidatesResult, error) {
	rs, err := e.FindCandidates(ctx, c)
	if err != nil {
		return nil, err
	}
	switch len(rs) {
	case 0:
		return nil, nil
	default:
		rch := make(chan types.FindCandidatesResult, len(rs))
		for _, r := range rs {
			rch <- types.FindCandidatesResult{
				Candidate: r,
			}
		}
		return rch, nil
	}
}

func (e explicitCandidateFinder) FindCandidates(_ context.Context, c cid.Cid) ([]types.RetrievalCandidate, error) {
	return []types.RetrievalCandidate{
		{
			MinerPeer: e.provider,
			RootCid:   c,
		},
	}, nil
}
