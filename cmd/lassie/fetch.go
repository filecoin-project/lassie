package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/lassie/cmd/lassie/internal"
	"github.com/filecoin-project/lassie/pkg/client"
	"github.com/filecoin-project/lassie/pkg/eventpublisher"
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
	putCb := func(putCount int) {
		blockCount += putCount
		if !progress {
			fmt.Print(strings.Repeat(".", putCount))
		} else if blockCount%10 == 0 {
			fmt.Printf("Received %d blocks...\n", blockCount)
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
		ret.RegisterListener(&progressPrinter{})
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

	confirmer := func(cid cid.Cid) (bool, error) {
		return blockstore.Has(c.Context, cid)
	}

	retrieverCfg := retriever.RetrieverConfig{
		DefaultMinerConfig: retriever.MinerConfig{
			RetrievalTimeout: timeout,
		},
	}

	ret, err := retriever.NewRetriever(c.Context, retrieverCfg, retrievalClient, finder, confirmer)
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
	cb     func(putCount int)
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
	pcb.cb(1)
	return pbs.Put(ctx, block)
}
func (pcb *putCbBlockstore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	pbs, err := pcb.parentBlockstore()
	if err != nil {
		return err
	}
	pcb.cb(len(blocks))
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

var _ retriever.RetrievalEventListener = (*progressPrinter)(nil)

type progressPrinter struct {
	candidatesFound int
}

func (progressPrinter) IndexerProgress(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, stage eventpublisher.Code) {
	fmt.Printf("Querying indexer for %s...\n", requestedCid)
}
func (pp *progressPrinter) IndexerCandidates(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, stage eventpublisher.Code, storageProviderIds []peer.ID) {
	switch stage {
	case eventpublisher.CandidatesFoundCode:
		pp.candidatesFound = len(storageProviderIds)
	case eventpublisher.CandidatesFilteredCode:
		num := "all of them"
		if pp.candidatesFound != len(storageProviderIds) {
			num = fmt.Sprintf("%d of them", len(storageProviderIds))
		} else if pp.candidatesFound == 1 {
			num = "it"
		}
		if fetchProviderAddrInfo == nil {
			fmt.Printf("Found %d storage providers candidates from the indexer, querying %s:\n", pp.candidatesFound, num)
		} else {
			fmt.Printf("Using the explicitly specified storage provider, querying %s:\n", num)
		}
		for _, id := range storageProviderIds {
			fmt.Printf("\t%s\n", id)
		}
	}
}
func (progressPrinter) QueryProgress(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage eventpublisher.Code) {
	fmt.Printf("Querying [%s] (%s)...\n", storageProviderId, stage)
}
func (progressPrinter) QueryFailure(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, errString string) {
	fmt.Printf("Query failure for [%s]: %s\n", storageProviderId, errString)
}
func (progressPrinter) QuerySuccess(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse) {
	fmt.Printf("Query response from [%s]: size=%s, price-per-byte=%s, unseal-price=%s, message=%s\n", storageProviderId, humanize.IBytes(queryResponse.Size), queryResponse.MinPricePerByte, queryResponse.UnsealPrice, queryResponse.Message)
}
func (progressPrinter) RetrievalProgress(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage eventpublisher.Code) {
	fmt.Printf("Retrieving from [%s] (%s)...\n", storageProviderId, stage)
}
func (progressPrinter) RetrievalSuccess(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, receivedSize uint64, receivedCids uint64, confirmed bool) {
}
func (progressPrinter) RetrievalFailure(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, errString string) {
	fmt.Printf("Retrieval failure for [%s]: %s\n", storageProviderId, errString)
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
