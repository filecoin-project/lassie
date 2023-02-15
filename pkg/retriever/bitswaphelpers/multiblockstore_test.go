package bitswaphelpers_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/lassie/pkg/retriever/bitswaphelpers"
	"github.com/filecoin-project/lassie/pkg/retriever/testutil"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/require"
)

func TestMultiblockstore(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	ds1 := datastore.NewMapDatastore()
	dss1 := sync.MutexWrap(ds1)
	bs1 := blockstore.NewBlockstore(dss1)
	lsys1 := storeutil.LinkSystemForBlockstore(bs1)
	ds2 := datastore.NewMapDatastore()
	dss2 := sync.MutexWrap(ds2)
	bs2 := blockstore.NewBlockstore(dss2)
	lsys2 := storeutil.LinkSystemForBlockstore(bs2)
	mbs := bitswaphelpers.NewMultiblockstore()
	id1, err := types.NewRetrievalID()
	req.NoError(err)
	id2, err := types.NewRetrievalID()
	req.NoError(err)
	storage1Ctx := types.RegisterRetrievalIDToContext(ctx, id1)
	storage2Ctx := types.RegisterRetrievalIDToContext(ctx, id2)
	mbs.AddLinkSystem(id1, &lsys1)
	mbs.AddLinkSystem(id2, &lsys2)
	blks := testutil.GenerateBlocksOfSize(5, 1000)
	cids := make([]cid.Cid, 0, 5)
	for _, blk := range blks {
		cids = append(cids, blk.Cid())
	}
	// should start off with no blocks returning anything
	for _, c := range cids {
		_, err := mbs.Get(ctx, c)
		req.True(format.IsNotFound(err))
		_, err = mbs.Get(storage1Ctx, c)
		req.True(format.IsNotFound(err))
		_, err = mbs.Get(storage2Ctx, c)
		req.True(format.IsNotFound(err))
	}
	// put to root store is not supported
	err = mbs.Put(ctx, blks[0])
	req.Equal(bitswaphelpers.ErrNotSupported, err)
	// put some blocks in each system
	err = mbs.Put(storage1Ctx, blks[0])
	req.NoError(err)
	err = mbs.Put(storage1Ctx, blks[1])
	req.NoError(err)
	err = mbs.Put(storage1Ctx, blks[2])
	req.NoError(err)
	err = mbs.PutMany(storage2Ctx, blks[2:])
	req.NoError(err)
	// verify blocks retrievable on per context basis
	for i, c := range cids {
		// no blocks for root context
		_, err := mbs.Get(ctx, c)
		req.True(format.IsNotFound(err))
		// storage contexts only return blocks put with their key
		blk, err := mbs.Get(storage1Ctx, c)
		if i <= 2 {
			req.NoError(err)
			req.Equal(blks[i].RawData(), blk.RawData())
		} else {
			req.True(format.IsNotFound(err))
		}
		blk, err = mbs.Get(storage2Ctx, c)
		if i >= 2 {
			req.NoError(err)
			req.Equal(blks[i].RawData(), blk.RawData())
		} else {
			req.True(format.IsNotFound(err))
		}
	}
	// verify only registered link systems still return blocks
	mbs.RemoveLinkSystem(id1)
	for i, c := range cids {
		// no blocks for root context
		_, err := mbs.Get(ctx, c)
		req.True(format.IsNotFound(err))
		// cancelled storage contexts return no blocks
		_, err = mbs.Get(storage1Ctx, c)
		req.True(format.IsNotFound(err))
		// storage contexts only return blocks put with their key
		blk, err := mbs.Get(storage2Ctx, c)
		if i >= 2 {
			req.NoError(err)
			req.Equal(blks[i].RawData(), blk.RawData())
		} else {
			req.True(format.IsNotFound(err))
		}
	}
	// unsupported operations
	_, err = mbs.Has(storage2Ctx, cids[2])
	req.Equal(bitswaphelpers.ErrNotSupported, err)
	err = mbs.DeleteBlock(storage2Ctx, cids[2])
	req.Equal(bitswaphelpers.ErrNotSupported, err)
	_, err = mbs.GetSize(storage2Ctx, cids[2])
	req.Equal(bitswaphelpers.ErrNotSupported, err)
	_, err = mbs.AllKeysChan(storage2Ctx)
	req.Equal(bitswaphelpers.ErrNotSupported, err)
}
