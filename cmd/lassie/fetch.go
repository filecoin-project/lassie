package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
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
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	blocks "github.com/ipfs/go-libipfs/blocks"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

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

	var parentOpener = func() (blockstore.Blockstore, io.Closer, error) {
		f, err := os.OpenFile(outfile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return nil, nil, err
		}
		bs, err := carblockstore.CreateWriteOnlyV1(f, []cid.Cid{rootCid})
		if err != nil {
			return nil, nil, f.Close()
		}
		return bs, f, err
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
	bstore := &putCbBlockstore{parentOpener: parentOpener, cb: putCb}
	ret, err := setupRetriever(c, c.Duration("timeout"), bstore)
	if err != nil {
		return err
	}

	fmt.Printf("Fetching %s", rootCid)
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

	confirmer := func(cid cid.Cid) (bool, error) {
		return blockstore.Has(c.Context, cid)
	}

	retrieverCfg := retriever.RetrieverConfig{
		DefaultMinerConfig: retriever.MinerConfig{
			RetrievalTimeout: timeout,
		},
	}

	ret, err := retriever.NewRetriever(c.Context, retrieverCfg, retrievalClient, indexer, confirmer)
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
	parentOpener func() (blockstore.Blockstore, io.Closer, error)
	// parent is lazily instantiated and should not be directly used; use parentBlockstore instead.
	parent blockstore.Blockstore
	closer io.Closer
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
	if pcb.closer != nil {
		return pcb.closer.Close()
	}
	return nil
}

func (pcb *putCbBlockstore) parentBlockstore() (blockstore.Blockstore, error) {
	if pcb.parent == nil {
		var err error
		if pcb.parent, pcb.closer, err = pcb.parentOpener(); err != nil {
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
		fmt.Printf("Found %d storage providers candidates from the indexer, querying %s:\n", pp.candidatesFound, num)
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
