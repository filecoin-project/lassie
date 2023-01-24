package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/lassie/cmd/lassie/internal"
	"github.com/filecoin-project/lassie/pkg/client"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

var fetchProviderAddrInfo *peer.AddrInfo

var fetchCmd = &cli.Command{
	Name:   "fetch",
	Usage:  "fetch content from Filecoin",
	Action: Fetch,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:      "output",
			Aliases:   []string{"o"},
			Usage:     "The CAR file to write to, may be an existing or a new CAR",
			TakesFile: true,
		},
		&cli.DurationFlag{
			Name:    "timeout",
			Aliases: []string{"t"},
			Usage:   "Consider it an error after not receiving a response from a storage provider for this long",
			Value:   20 * time.Second,
		},
		&cli.BoolFlag{
			Name:    "progress",
			Aliases: []string{"p"},
			Usage:   "Print progress output",
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
		return carblockstore.OpenReadWrite(outfile, []cid.Cid{rootCid})
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
	bstore := &putCbBlockstore{parentOpener: parentOpener, cb: putCb}

	var ret *retriever.Retriever
	if fetchProviderAddrInfo == nil {
		ret, err = setupRetriever(c, timeout, bstore)
	} else {
		ret, err = setupRetrieverWithFinder(c, timeout, bstore, explicitCandidateFinder{provider: *fetchProviderAddrInfo})
	}
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
		ret.RegisterSubscriber(pp.subscriber)
	}
	retrievalId, err := types.NewRetrievalID()
	if err != nil {
		return err
	}
	stats, err := ret.Retrieve(c.Context, retrievalId, rootCid)
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

func setupRetriever(c *cli.Context, timeout time.Duration, blockstore blockstore.Blockstore) (*retriever.Retriever, error) {
	return setupRetrieverWithFinder(c, timeout, blockstore, indexerlookup.NewCandidateFinder("https://cid.contact"))
}

func setupRetrieverWithFinder(c *cli.Context, timeout time.Duration, blockstore blockstore.Blockstore, finder retriever.CandidateFinder) (*retriever.Retriever, error) {
	datastore := dss.MutexWrap(datastore.NewMapDatastore())

	host, err := internal.InitHost(c.Context, multiaddr.StringCast("/ip4/0.0.0.0/tcp/6746"))
	if err != nil {
		return nil, err
	}

	retrievalClient, err := client.NewClient(
		blockstore,
		datastore,
		host,
		nil,
	)
	if err != nil {
		return nil, err
	}

	indexer := indexerlookup.NewCandidateFinder("https://cid.contact")

	retrieverCfg := retriever.RetrieverConfig{
		DefaultMinerConfig: retriever.MinerConfig{
			RetrievalTimeout: timeout,
		},
	}

	ret, err := retriever.NewRetriever(c.Context, retrieverCfg, retrievalClient, indexer)
	if err != nil {
		return nil, err
	}
	<-ret.Start()
	return ret, nil
}

// putCbBlockstore simply calls a callback on each put(), with the number of blocks put
var _ blockstore.Blockstore = (*putCbBlockstore)(nil)

type putCbBlockstore struct {
	// parentOpener lazily opens the parent blockstore upon first call to this blockstore.
	// This avoids blockstore instantiation until there is some interaction from the retriever.
	// In the case of CARv2 blockstores, this will avoid creation of empty .car files should
	// the retriever fail to find any candidates.
	parentOpener func() (*carblockstore.ReadWrite, error)
	// parent is lazily instantiated and should not be directly used; use parentBlockstore instead.
	parent *carblockstore.ReadWrite
	cb     func(putCount int, putBytes int)
}

func (pcb *putCbBlockstore) DeleteBlock(ctx context.Context, cid cid.Cid) error {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return err
	}
	return pbs.DeleteBlock(ctx, cid)
}
func (pcb *putCbBlockstore) Has(ctx context.Context, cid cid.Cid) (bool, error) {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return false, err
	}
	return pbs.Has(ctx, cid)
}
func (pcb *putCbBlockstore) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return nil, err
	}
	return pbs.Get(ctx, cid)
}
func (pcb *putCbBlockstore) GetSize(ctx context.Context, cid cid.Cid) (int, error) {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return 0, err
	}
	return pbs.GetSize(ctx, cid)
}
func (pcb *putCbBlockstore) Put(ctx context.Context, block blocks.Block) error {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return err
	}
	pcb.cb(1, len(block.RawData()))
	return pbs.Put(ctx, block)
}
func (pcb *putCbBlockstore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return err
	}
	var byts int
	for _, b := range blocks {
		byts += len(b.RawData())
	}
	pcb.cb(len(blocks), byts)
	return pbs.PutMany(ctx, blocks)
}
func (pcb *putCbBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return nil, err
	}
	return pbs.AllKeysChan(ctx)
}
func (pcb *putCbBlockstore) HashOnRead(enabled bool) {
	if pbs, err := pcb.parentBlockstore(); err != nil {
		log.Printf("Failed to instantiate blockstore while setting HashOnRead: %v\n", err)
	} else {
		pbs.HashOnRead(enabled)
	}
}
func (pcb *putCbBlockstore) Finalize() error {
	if pbs, err := pcb.parentBlockstore(); err != nil {
		return err
	} else {
		return pbs.Finalize()
	}
}
func (pcb *putCbBlockstore) parentBlockstore() (*carblockstore.ReadWrite, error) {
	if pcb.parent == nil {
		var err error
		if pcb.parent, err = pcb.parentOpener(); err != nil {
			return nil, err
		}
	}
	return pcb.parent, nil
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
