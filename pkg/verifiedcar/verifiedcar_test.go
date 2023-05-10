package verifiedcar_test

import (
	"bytes"
	"context"
	"io"
	"os"
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
	"github.com/ipld/go-ipld-prime/node/basicnode"
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
	lsys.TrustedStorage = true
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	tbc1 := gstestutil.SetupBlockChain(ctx, t, lsys, 1000, 100)
	root1 := tbc1.TipLink.(cidlink.Link).Cid
	allBlocks := make([]block, 0, 100)
	for _, b := range tbc1.AllBlocks() {
		allBlocks = append(allBlocks, block{b.Cid(), b.RawData()})
	}
	extraneousLnk, err := lsys.Store(linking.LinkContext{}, cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1,
		Codec:    0x71,
		MhType:   0x12,
		MhLength: 32,
	}}, basicnode.NewString("borp"))
	req.NoError(err)
	extraneousByts, err := lsys.LoadRaw(linking.LinkContext{}, extraneousLnk)
	req.NoError(err)
	allSelector, err := selector.CompileSelector(selectorparse.CommonSelector_ExploreAllRecursively)
	req.NoError(err)

	testCases := []struct {
		name   string
		blocks []block
		roots  []cid.Cid
		carv2  bool
		err    string
		cfg    verifiedcar.Config
	}{
		{
			name:   "complete carv1",
			blocks: allBlocks,
			roots:  []cid.Cid{root1},
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:   "carv2 without AllowCARv2 errors",
			blocks: allBlocks,
			roots:  []cid.Cid{root1},
			carv2:  true,
			err:    "bad CAR version",
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:   "complete carv2 with AllowCARv2",
			blocks: allBlocks,
			roots:  []cid.Cid{root1},
			carv2:  true,
			cfg: verifiedcar.Config{
				Root:       root1,
				Selector:   allSelector,
				AllowCARv2: true,
			},
		},
		{
			name:   "carv1 with multiple roots errors",
			blocks: allBlocks,
			roots:  []cid.Cid{root1, root1},
			err:    "root CID mismatch",
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:   "carv1 with wrong root errors",
			blocks: allBlocks,
			roots:  []cid.Cid{tbc1.AllBlocks()[1].Cid()},
			err:    "root CID mismatch",
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:   "carv1 with extraneous trailing block errors",
			blocks: append(append([]block{}, allBlocks...), block{extraneousLnk.(cidlink.Link).Cid, extraneousByts}),
			roots:  []cid.Cid{root1},
			err:    "extraneous block",
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:   "carv1 with extraneous leading block errors",
			blocks: append([]block{{extraneousLnk.(cidlink.Link).Cid, extraneousByts}}, allBlocks...),
			roots:  []cid.Cid{root1},
			err:    "unexpected block: " + extraneousLnk.(cidlink.Link).Cid.String() + " != " + allBlocks[0].cid.String(),
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:   "carv1 with out-of-order blocks errors",
			blocks: append(append([]block{}, allBlocks[50:]...), allBlocks[0:50]...),
			roots:  []cid.Cid{root1},
			err:    "unexpected block: " + allBlocks[50].cid.String() + " != " + allBlocks[0].cid.String(),
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:   "carv1 with mismatching CID errors",
			blocks: append(append([]block{}, allBlocks[0:99]...), block{allBlocks[99].cid, extraneousByts}),
			roots:  []cid.Cid{root1},
			err:    "mismatch in content integrity",
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

			carStream := makeCarStream(t, ctx, testCase.roots, testCase.blocks, testCase.carv2, testCase.err != "")
			blockCount, byteCount, err := testCase.cfg.Verify(ctx, carStream, bwo)
			if testCase.err != "" {
				req.ErrorContains(err, testCase.err)
				req.Equal(uint64(0), blockCount)
				req.Equal(uint64(0), byteCount)
			} else {
				req.NoError(err)
				req.Equal(uint64(len(testCase.blocks)), blockCount)
				req.Equal(sizeOf(testCase.blocks), byteCount)
				req.Equal(len(testCase.blocks), writeCounter)
			}
		})
	}
}

func makeCarStream(
	t *testing.T,
	ctx context.Context,
	roots []cid.Cid,
	blocks []block,
	carv2 bool,
	expectErrors bool,
) io.Reader {

	r, w := io.Pipe()

	go func() {
		req := require.New(t)

		var carW io.Writer = w

		var v2f *os.File
		if carv2 {
			// if v2 we have to write to a temp file and stream that out since we
			// can't create a streaming v2
			var err error
			v2f, err = os.CreateTemp(t.TempDir(), "carv2")
			req.NoError(err)
			t.Cleanup(func() {
				v2f.Close()
				os.Remove(v2f.Name())
			})
			carW = v2f
		}

		carWriter, err := storage.NewWritable(carW, roots, car.WriteAsCarV1(!carv2), car.AllowDuplicatePuts(false))
		req.NoError(err)
		if err != nil {
			return
		}
		for _, block := range blocks {
			err := carWriter.Put(ctx, block.cid.KeyString(), block.data)
			if !expectErrors {
				req.NoError(err)
			}
			if ctx.Err() != nil {
				return
			}
		}
		req.NoError(carWriter.Finalize())

		if carv2 {
			v2f.Seek(0, io.SeekStart)
			// ignore error because upstream will strictly stop and close after
			// reading the carv1 payload so we'll get an error here
			io.Copy(w, v2f)
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
