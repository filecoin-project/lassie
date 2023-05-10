package verifiedcar_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/verifiedcar"
	"github.com/ipfs/go-cid"
	gstestutil "github.com/ipfs/go-graphsync/testutil"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"
)

func TestVerifiedCar(t *testing.T) {
	ctx := context.Background()

	req := require.New(t)

	store := &correctedMemStore{&memstore.Store{
		Bag: make(map[string][]byte),
	}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	tbc1 := gstestutil.SetupBlockChain(ctx, t, lsys, 1000, 100)
	root1 := tbc1.TipLink.(cidlink.Link).Cid
	allBlocks := make([]block, 0, 100)
	for _, b := range tbc1.AllBlocks() {
		allBlocks = append(allBlocks, block{b.Cid(), b.RawData()})
	}
	allSelector, err := selector.CompileSelector(selectorparse.CommonSelector_ExploreAllRecursively)
	req.NoError(err)

	testCases := []struct {
		name   string
		blocks []block
		cfg    verifiedcar.Config
	}{
		{

			name:   "complete carv1",
			blocks: allBlocks,
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			req := require.New(t)

			var writeCounter int
			bwo := func(lc linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
				var buf bytes.Buffer
				return &buf, func(l datamodel.Link) error {
					req.Equal(testCase.blocks[writeCounter].cid, l.(cidlink.Link).Cid, "block %d", writeCounter)
					req.Equal(testCase.blocks[writeCounter].data, buf.Bytes(), "block %d", writeCounter)
					writeCounter++
					return nil
				}, nil
			}

			carStream := makeCarStream(t, ctx, testCase.cfg.Root, testCase.blocks)
			blockCount, byteCount, err := testCase.cfg.Verify(ctx, carStream, bwo)
			req.NoError(err)
			req.Equal(uint64(len(testCase.blocks)), blockCount)
			req.Equal(sizeOf(testCase.blocks), byteCount)
			req.Equal(len(testCase.blocks), writeCounter)
		})
	}
}

func makeCarStream(t *testing.T, ctx context.Context, root cid.Cid, blocks []block) io.Reader {
	r, w := io.Pipe()

	go func() {
		req := require.New(t)
		carWriter, err := storage.NewWritable(w, []cid.Cid{root}, car.WriteAsCarV1(true), car.AllowDuplicatePuts(false))
		req.NoError(err)
		if err != nil {
			return
		}
		for _, block := range blocks {
			req.NoError(carWriter.Put(ctx, block.cid.KeyString(), block.data))
			if ctx.Err() != nil {
				return
			}
		}
		req.NoError(w.Close())
	}()

	go func() {
		<-ctx.Done()
		if ctx.Err() != nil {
			r.CloseWithError(ctx.Err())
		}
	}()

	return r
}

type block struct {
	cid  cid.Cid
	data []byte
}

func sizeOf(blocks []block) uint64 {
	total := uint64(0)
	for _, block := range blocks {
		total += uint64(len(block.data))
	}
	return total
}

// TODO: remove when this is fixed in IPLD prime
type correctedMemStore struct {
	*memstore.Store
}

func (cms *correctedMemStore) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := cms.Store.Get(ctx, key)
	if err != nil && err.Error() == "404" {
		err = format.ErrNotFound{}
	}
	return data, err
}

func (cms *correctedMemStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := cms.Store.GetStream(ctx, key)
	if err != nil && err.Error() == "404" {
		err = format.ErrNotFound{}
	}
	return rc, err
}
