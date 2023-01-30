package lassie

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/filecoin-project/lassie/internal"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync/storeutil"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
)

type Lassie struct {
	timeout time.Duration
}

func NewLassie(timeout time.Duration) *Lassie {
	return &Lassie{
		timeout: timeout,
	}
}

func (l *Lassie) Fetch(ctx context.Context, rootCid cid.Cid, outfile *os.File) (*types.RetrievalStats, types.RetrievalID, error) {
	var parentOpener = func() (*carblockstore.ReadWrite, error) {
		return carblockstore.OpenReadWriteFile(outfile, []cid.Cid{rootCid})
	}

	var blockCount int
	var byteLength uint64
	putCb := func(putCount int, putBytes int) {
		blockCount += putCount
		byteLength += uint64(putBytes)
	}
	bstore := internal.NewPutCbBlockstore(parentOpener, putCb)

	linkSystem := storeutil.LinkSystemForBlockstore(bstore)

	var ret *retriever.Retriever
	ret, err := internal.SetupRetriever(ctx, l.timeout)
	if err != nil {
		return nil, types.RetrievalID{}, err
	}

	retrievalId, err := types.NewRetrievalID()
	if err != nil {
		return nil, types.RetrievalID{}, err
	}

	stats, err := ret.Retrieve(ctx, linkSystem, retrievalId, rootCid)
	if err != nil {
		fmt.Println()
		return nil, retrievalId, err
	}

	return stats, retrievalId, nil
}
