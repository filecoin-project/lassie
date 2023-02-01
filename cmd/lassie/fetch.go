package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/lassie/internal"
	lcli "github.com/filecoin-project/lassie/pkg/cli"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync/storeutil"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
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
		lcli.FlagVerbose,
		lcli.FlagVeryVerbose,
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

	outfile := fmt.Sprintf("%s.car", rootCid)
	if c.IsSet("output") {
		outfile = c.String("output")
	}

	var parentOpener = func() (*carblockstore.ReadWrite, error) {
		return carblockstore.OpenReadWrite(outfile, []cid.Cid{rootCid}, carblockstore.WriteAsCarV1(true))
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
	timeout := c.Duration("timeout")
	bstore := internal.NewPutCbBlockstore(parentOpener, putCb)

	linkSystem := storeutil.LinkSystemForBlockstore(bstore)

	var ret *retriever.Retriever
	if fetchProviderAddrInfo == nil {
		ret, err = internal.SetupRetriever(c.Context, timeout)
	} else {
		ret, err = internal.SetupRetrieverWithFinder(c.Context, timeout, explicitCandidateFinder{provider: *fetchProviderAddrInfo})
	}
	if err != nil {
		return err
	}

	if fetchProviderAddrInfo == nil {
		fmt.Printf("Fetching %s", rootCid)
	} else {
		fmt.Printf("Fetching %s from %s", rootCid, fetchProviderAddrInfo.String())
	}
	var eventsCb func(types.RetrievalEvent) = nil
	if progress {
		fmt.Println()
		pp := &progressPrinter{}
		eventsCb = pp.subscriber
	}
	retrievalId, err := types.NewRetrievalID()
	if err != nil {
		return err
	}
	stats, err := ret.Retrieve(c.Context, linkSystem, retrievalId, rootCid, eventsCb)
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

	return bstore.Finalize()
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

func (e explicitCandidateFinder) FindCandidates(_ context.Context, c cid.Cid) ([]types.RetrievalCandidate, error) {
	return []types.RetrievalCandidate{
		{
			MinerPeer: e.provider,
			RootCid:   c,
		},
	}, nil
}
