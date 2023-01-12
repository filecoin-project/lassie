package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/lassie/cmd/lassie/internal"
	"github.com/filecoin-project/lassie/pkg/client"
	"github.com/filecoin-project/lassie/pkg/eventpublisher"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/google/uuid"
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

var fetchCmd = &cli.Command{
	Name:   "fetch",
	Usage:  "fetch content from Filecoin",
	Action: Fetch,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:      "output",
			Aliases:   []string{"o"},
			Usage:     "The CAR file to write to",
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

	carStore, err := carblockstore.OpenReadWrite(outfile, []cid.Cid{rootCid})
	if err != nil {
		return err
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
	bstore := &putCbBlockstore{parent: carStore, cb: putCb}
	retriever, err := setupRetriever(c, c.Duration("timeout"), bstore)
	if err != nil {
		return err
	}

	fmt.Printf("Fetching %s", rootCid)
	if progress {
		fmt.Println()
		retriever.RegisterListener(progressPrinter{})
	}
	stats, err := retriever.Retrieve(c.Context, rootCid)
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

	carStore.Finalize()

	return nil
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

	return retriever.NewRetriever(c.Context, retrieverCfg, retrievalClient, indexer, confirmer)
}

// putCbBlockstore simply calls a callback on each put(), with the number of blocks put
var _ blockstore.Blockstore = (*putCbBlockstore)(nil)

type putCbBlockstore struct {
	parent blockstore.Blockstore
	cb     func(putCount int)
}

func (pcb *putCbBlockstore) DeleteBlock(ctx context.Context, cid cid.Cid) error {
	return pcb.parent.DeleteBlock(ctx, cid)
}
func (pcb *putCbBlockstore) Has(ctx context.Context, cid cid.Cid) (bool, error) {
	return pcb.parent.Has(ctx, cid)
}
func (pcb *putCbBlockstore) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	return pcb.parent.Get(ctx, cid)
}
func (pcb *putCbBlockstore) GetSize(ctx context.Context, cid cid.Cid) (int, error) {
	return pcb.parent.GetSize(ctx, cid)
}
func (pcb *putCbBlockstore) Put(ctx context.Context, block blocks.Block) error {
	pcb.cb(1)
	return pcb.parent.Put(ctx, block)
}
func (pcb *putCbBlockstore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	pcb.cb(len(blocks))
	return pcb.parent.PutMany(ctx, blocks)
}
func (pcb *putCbBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return pcb.parent.AllKeysChan(ctx)
}
func (pcb *putCbBlockstore) HashOnRead(enabled bool) {
	pcb.parent.HashOnRead(enabled)
}

type progressPrinter struct{}

func (progressPrinter) QueryProgress(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage eventpublisher.Code) {
	fmt.Printf("Querying [%s] (%s)...\n", storageProviderId, stage)
}
func (progressPrinter) QueryFailure(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, errString string) {
	fmt.Printf("Query failure for [%s]: %s\n", storageProviderId, errString)
}
func (progressPrinter) QuerySuccess(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse) {
	fmt.Printf("Query response from [%s]: size=%d bytes, price-per-byte=%s, unseal-price=%s, message=[%s]\n", storageProviderId, queryResponse.Size, queryResponse.MinPricePerByte, queryResponse.UnsealPrice, queryResponse.Message)
}
func (progressPrinter) RetrievalProgress(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage eventpublisher.Code) {
	fmt.Printf("Retrieving from [%s] (%s)...\n", storageProviderId, stage)
}
func (progressPrinter) RetrievalSuccess(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, receivedSize uint64, receivedCids int64, confirmed bool) {
}
func (progressPrinter) RetrievalFailure(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, errString string) {
	fmt.Printf("Retrieval failure for [%s]: %s\n", storageProviderId, errString)
}
