package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/lassie/cmd/lassie/internal"
	"github.com/filecoin-project/lassie/pkg/client"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/retriever"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

func Fetch(c *cli.Context) error {
	if c.Args().Len() == 0 || !c.IsSet("output") {
		return fmt.Errorf("usage: lassie fetch -o <CAR file> [-t <timeout>] <CID>")
	}

	rootCid, err := cid.Parse(c.Args().Get(0))
	if err != nil {
		return err
	}

	configDir, err := setupConfigDir()
	if err != nil {
		return err
	}

	carStore, err := carblockstore.OpenReadWrite(c.String("output"), []cid.Cid{rootCid})
	if err != nil {
		return err
	}

	var blockCount int
	putCb := func(putCount int) {
		fmt.Print(strings.Repeat(".", putCount))
		blockCount++
	}
	bstore := &putCbBlockstore{parent: carStore, cb: putCb}
	retriever, err := setupRetriever(configDir, c, c.Duration("timeout"), bstore)
	if err != nil {
		return err
	}

	fmt.Printf("Fetching %s", rootCid)
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

func setupConfigDir() (string, error) {
	homedir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(homedir, ".lassie", "datastore"), nil
}

func setupRetriever(configDir string, c *cli.Context, timeout time.Duration, blockstore blockstore.Blockstore) (*retriever.Retriever, error) {
	datastore := dss.MutexWrap(datastore.NewMapDatastore())

	host, err := internal.InitHost(c.Context, configDir, multiaddr.StringCast("/ip4/0.0.0.0/tcp/6746"))
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
