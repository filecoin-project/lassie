package storage_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/storage"
	"github.com/filecoin-project/lassie/pkg/types"
	blocks "github.com/ipfs/go-block-format"
	unixfs "github.com/ipfs/go-unixfsnode/testutil"
	carv2 "github.com/ipld/go-car/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"
)

func TestDuplicateAdderCar(t *testing.T) {

	setupStore := &testutil.CorrectedMemStore{ParentStore: &memstore.Store{
		Bag: make(map[string][]byte),
	}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.TrustedStorage = true
	lsys.SetReadStorage(setupStore)
	lsys.SetWriteStorage(setupStore)

	unixfsFileWithDups := unixfs.GenerateFile(t, &lsys, testutil.ZeroReader{}, 4<<20)
	unixfsFileWithDupsBlocks := testutil.ToBlocks(t, lsys, unixfsFileWithDups.Root, selectorparse.CommonSelector_ExploreAllRecursively)
	buf := new(bytes.Buffer)

	store := storage.NewDeferredStorageCar("", unixfsFileWithDups.Root)
	ctx := context.Background()
	carWriter := storage.NewDuplicateAdderCarForStream(ctx, unixfsFileWithDups.Root, "", types.DagScopeAll, store, buf)
	cachingTempStore := storage.NewCachingTempStore(carWriter.BlockWriteOpener(), store)

	// write the root block, containing sharding metadata
	cachingTempStore.Put(ctx, unixfsFileWithDupsBlocks[0].Cid().KeyString(), unixfsFileWithDupsBlocks[0].RawData())
	// write the duped block that the root points to for all but the last block
	cachingTempStore.Put(ctx, unixfsFileWithDupsBlocks[1].Cid().KeyString(), unixfsFileWithDupsBlocks[1].RawData())
	// write the last block, which will be unique because of a different length
	cachingTempStore.Put(ctx, unixfsFileWithDupsBlocks[len(unixfsFileWithDupsBlocks)-1].Cid().KeyString(), unixfsFileWithDupsBlocks[len(unixfsFileWithDupsBlocks)-1].RawData())
	err := carWriter.Close()
	require.NoError(t, err)
	err = cachingTempStore.Close()
	require.NoError(t, err)

	// now, verify the traversal output a whole car with dups.
	reader, err := carv2.NewBlockReader(buf)
	require.NoError(t, err)
	receivedBlocks := make([]blocks.Block, 0, len(unixfsFileWithDupsBlocks))
	for {
		blk, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		receivedBlocks = append(receivedBlocks, blk)
	}
	require.Equal(t, unixfsFileWithDupsBlocks, receivedBlocks)
}
