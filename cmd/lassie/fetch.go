package main

import (
	"context"
	"fmt"
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

	carStore, err := carblockstore.OpenReadWrite(outfile, []cid.Cid{rootCid})
	if err != nil {
		return err
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
	bstore := &putCbBlockstore{parent: carStore, cb: putCb}
	ret, err := setupRetriever(c, c.Duration("timeout"), bstore)
	if err != nil {
		return err
	}

	fmt.Printf("Fetching %s", rootCid)
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
	parent blockstore.Blockstore
	cb     func(putCount int, putBytes int)
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
	pcb.cb(1, len(block.RawData()))
	return pcb.parent.Put(ctx, block)
}
func (pcb *putCbBlockstore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	var byts int
	for _, b := range blocks {
		byts += len(b.RawData())
	}
	pcb.cb(len(blocks), byts)
	return pcb.parent.PutMany(ctx, blocks)
}
func (pcb *putCbBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return pcb.parent.AllKeysChan(ctx)
}
func (pcb *putCbBlockstore) HashOnRead(enabled bool) {
	pcb.parent.HashOnRead(enabled)
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
		fmt.Printf("\rFound %d storage providers candidates from the indexer, querying %s:\n", pp.candidatesFound, num)
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
