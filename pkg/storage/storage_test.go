package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

func TestTempCarStorage(t *testing.T) {
	// Testing both DeferredCarStorage and CachingTempStore here with just some
	// additional pieces of logic to make sure the teeing version is actually
	// teeing.
	for _, teeing := range []bool{true, false} {
		teeing := teeing
		t.Run(fmt.Sprintf("teeing=%t", teeing), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			testCid1, testData1 := randBlock()
			testCid2, testData2 := randBlock()
			testCid3, _ := randBlock()

			tempDir := t.TempDir()

			teeCollect := make(map[cid.Cid][]byte, 0)
			var cw types.ReadableWritableStorage
			if teeing {
				bwo := func(ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
					var buf bytes.Buffer
					return &buf, func(lnk ipld.Link) error {
						teeCollect[lnk.(cidlink.Link).Cid] = buf.Bytes()
						return nil
					}, nil
				}
				cw = NewCachingTempStore(bwo, NewDeferredStorageCar(tempDir))
			} else {
				cw = NewDeferredStorageCar(tempDir)
			}

			ents, err := os.ReadDir(tempDir)
			require.NoError(t, err)
			require.Len(t, ents, 0)

			has, err := cw.Has(ctx, testCid3.KeyString())
			require.NoError(t, err)
			require.False(t, has)
			_, err = cw.Get(ctx, testCid1.KeyString())
			require.Error(t, err)
			enf, ok := err.(interface{ NotFound() bool })
			require.True(t, ok)
			require.True(t, enf.NotFound())
			_, err = cw.GetStream(ctx, testCid1.KeyString())
			require.Error(t, err)
			enf, ok = err.(interface{ NotFound() bool })
			require.True(t, ok)
			require.True(t, enf.NotFound())

			require.NoError(t, cw.Put(ctx, testCid1.KeyString(), testData1))
			has, err = cw.Has(ctx, testCid1.KeyString())
			require.NoError(t, err)
			require.True(t, has)
			got, err := cw.Get(ctx, testCid1.KeyString())
			require.NoError(t, err)
			require.Equal(t, testData1, got)
			gotStream, err := cw.GetStream(ctx, testCid1.KeyString())
			require.NoError(t, err)
			got, err = io.ReadAll(gotStream)
			require.NoError(t, err)
			require.Equal(t, testData1, got)
			require.NoError(t, cw.Put(ctx, testCid2.KeyString(), testData2))
			has, err = cw.Has(ctx, testCid1.KeyString())
			require.NoError(t, err)
			require.True(t, has)
			has, err = cw.Has(ctx, testCid2.KeyString())
			require.NoError(t, err)
			require.True(t, has)
			got, err = cw.Get(ctx, testCid2.KeyString())
			require.NoError(t, err)
			require.Equal(t, testData2, got)
			gotStream, err = cw.GetStream(ctx, testCid2.KeyString())
			require.NoError(t, err)
			got, err = io.ReadAll(gotStream)
			require.NoError(t, err)
			require.Equal(t, testData2, got)
			has, err = cw.Has(ctx, testCid3.KeyString())
			require.NoError(t, err)
			require.False(t, has)
			_, err = cw.Get(ctx, testCid3.KeyString())
			require.Error(t, err)
			enf, ok = err.(interface{ NotFound() bool })
			require.True(t, ok)
			require.True(t, enf.NotFound())

			ents, err = os.ReadDir(tempDir)
			require.NoError(t, err)
			require.Len(t, ents, 1)
			require.Contains(t, ents[0].Name(), "carstorage")
			stat, err := os.Stat(tempDir + "/" + ents[0].Name())
			require.NoError(t, err)
			require.True(t, stat.Size() > int64(len(testData1)+len(testData2)))

			closer, ok := cw.(io.Closer)
			require.True(t, ok)
			require.NoError(t, closer.Close())

			// should be deleted
			ents, err = os.ReadDir(tempDir)
			require.NoError(t, err)
			require.Len(t, ents, 0)

			if teeing {
				require.Len(t, teeCollect, 2)
				require.Equal(t, testData1, teeCollect[testCid1])
				require.Equal(t, testData2, teeCollect[testCid2])
			}
		})
	}
}

type blk struct {
	cid  cid.Cid
	data []byte
}

func TestPreloadStore(t *testing.T) {
	ctx := context.Background()

	td := make([]blk, 0, 10)
	for i := 0; i < 10; i++ {
		c, d := randBlock()
		td = append(td, blk{c, d})
	}

	bwoCnt := 0
	bwo := func(ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		var buf bytes.Buffer
		return &buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			if bwoCnt >= len(td) {
				require.Fail(t, "too many calls to bwo")
			} else {
				require.Equal(t, td[bwoCnt].cid, c)
				require.Equal(t, td[bwoCnt].data, buf.Bytes())
			}
			bwoCnt++
			return nil
		}, nil
	}
	mainStore := NewCachingTempStore(bwo, NewDeferredStorageCar(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, mainStore.Close())
	})
	preload := mainStore.PreloadStore()

	checkNotHas := func(blks []blk, stores ...types.ReadableWritableStorage) {
		for _, d := range blks {
			for _, s := range stores {
				has, err := s.Has(ctx, d.cid.KeyString())
				require.NoError(t, err)
				require.False(t, has)
				_, err = s.Get(ctx, d.cid.KeyString())
				require.Error(t, err)
				enf, ok := err.(interface{ NotFound() bool })
				require.True(t, ok)
				require.True(t, enf.NotFound())
				_, err = s.GetStream(ctx, d.cid.KeyString())
				require.Error(t, err)
				enf, ok = err.(interface{ NotFound() bool })
				require.True(t, ok)
				require.True(t, enf.NotFound())
			}
		}
	}

	checkHas := func(blks []blk, stores ...types.ReadableWritableStorage) {
		for _, d := range blks {
			for _, s := range stores {
				has, err := s.Has(ctx, d.cid.KeyString())
				require.NoError(t, err)
				require.True(t, has)
				got, err := s.Get(ctx, d.cid.KeyString())
				require.NoError(t, err)
				require.Equal(t, d.data, got)
				gotStream, err := s.GetStream(ctx, d.cid.KeyString())
				require.NoError(t, err)
				got, err = io.ReadAll(gotStream)
				require.NoError(t, err)
				require.Equal(t, d.data, got)
			}
		}
	}

	checkNotHas(td, mainStore, preload)
	mainStore.Put(ctx, td[0].cid.KeyString(), td[0].data)
	checkNotHas(td, preload)
	checkNotHas(td[1:], mainStore)
	checkHas(td[:1], mainStore)
	for i := 5; i >= 1; i-- { // out of order, partial preload
		preload.Put(ctx, td[i].cid.KeyString(), td[i].data)
	}
	checkNotHas(td[1:], mainStore)
	checkNotHas(td[:1], preload)
	checkHas(td[1:6], preload)
	checkNotHas(td[6:], preload)
	for _, d := range td[1:] { // in order, complete
		mainStore.Put(ctx, d.cid.KeyString(), d.data)
	}
	checkHas(td, mainStore)
	checkNotHas(td, preload)
}
