package verifiedcar_test

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/internal/itest/unixfs"
	"github.com/filecoin-project/lassie/pkg/verifiedcar"
	"github.com/ipfs/go-cid"
	gstestutil "github.com/ipfs/go-graphsync/testutil"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"
)

func TestVerifiedCar(t *testing.T) {
	ctx := context.Background()

	req := require.New(t)

	rndSeed := time.Now().UTC().UnixNano()
	t.Logf("random seed: %d", rndSeed)
	var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

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
	extraneousLnk, err := lsys.Store(linking.LinkContext{}, cidlink.LinkPrototype{Prefix: cid.Prefix{Version: 1, Codec: 0x71, MhType: 0x12, MhLength: 32}}, basicnode.NewString("borp"))
	req.NoError(err)
	extraneousByts, err := lsys.LoadRaw(linking.LinkContext{}, extraneousLnk)
	req.NoError(err)

	allSelector := mustCompile(selectorparse.CommonSelector_ExploreAllRecursively)

	unixfsFile := unixfs.GenerateFile(t, &lsys, rndReader, 4<<20)
	unixfsFileBlocks := toBlocks(t, lsys, unixfsFile.Root, allSelector)

	var unixfsDir unixfs.DirEntry
	var unixfsDirBlocks []block
	for {
		unixfsDir = unixfs.GenerateDirectory(t, &lsys, rndReader, 8<<20, false)
		unixfsDirBlocks = toBlocks(t, lsys, unixfsDir.Root, allSelector)
		if len(unixfsDir.Children) > 2 { // we want at least 3 children to test the path subset selector
			break
		}
	}

	unixfsShardedDir := unixfs.GenerateDirectory(t, &lsys, rndReader, 8<<20, true)
	unixfsShardedDirBlocks := toBlocks(t, lsys, unixfsShardedDir.Root, allSelector)

	unixfsPreloadSelector := mustCompile(unixfsnode.MatchUnixFSPreloadSelector.Node())

	unixfsPreloadDirBlocks := toBlocks(t, lsys, unixfsDir.Root, unixfsPreloadSelector)
	unixfsPreloadShardedDirBlocks := toBlocks(t, lsys, unixfsShardedDir.Root, unixfsPreloadSelector)

	unixfsDirSubsetSelector := mustCompile(unixfsnode.UnixFSPathSelectorBuilder(unixfsDir.Children[1].Path, unixfsnode.MatchUnixFSPreloadSelector, false))

	unixfsWrappedPathSelector := mustCompile(unixfsnode.UnixFSPathSelectorBuilder(unixfs.WrapPath, unixfsnode.ExploreAllRecursivelySelector, false))
	unixfsWrappedPreloadPathSelector := mustCompile(unixfsnode.UnixFSPathSelectorBuilder(unixfs.WrapPath, unixfsnode.MatchUnixFSPreloadSelector, false))

	unixfsWrappedFile := unixfs.WrapContent(t, rndReader, &lsys, unixfsFile)
	unixfsWrappedFileBlocks := toBlocks(t, lsys, unixfsWrappedFile.Root, allSelector)
	// "trimmed" is similar to "exclusive" except that "trimmed" is a subset
	// of a larger DAG, whereas "exclusive" is a complete DAG.
	unixfsTrimmedWrappedFileBlocks := toBlocks(t, lsys, unixfsWrappedFile.Root, unixfsWrappedPathSelector)
	unixfsExclusiveWrappedFile := unixfs.WrapContentExclusive(t, rndReader, &lsys, unixfsFile)
	unixfsExclusiveWrappedFileBlocks := toBlocks(t, lsys, unixfsExclusiveWrappedFile.Root, allSelector)

	unixfsWrappedShardedDir := unixfs.WrapContent(t, rndReader, &lsys, unixfsShardedDir)
	unixfsWrappedShardedDirBlocks := toBlocks(t, lsys, unixfsWrappedShardedDir.Root, allSelector)
	// "trimmed" is similar to "exclusive" except that "trimmed" is a subset
	// of a larger DAG, whereas "exclusive" is a complete DAG.
	unixfsTrimmedWrappedShardedDirBlocks := toBlocks(t, lsys, unixfsWrappedShardedDir.Root, unixfsWrappedPathSelector)
	unixfsTrimmedWrappedShardedDirOnlyBlocks := toBlocks(t, lsys, unixfsWrappedShardedDir.Root, unixfsWrappedPreloadPathSelector)
	unixfsExclusiveWrappedShardedDir := unixfs.WrapContentExclusive(t, rndReader, &lsys, unixfsShardedDir)
	unixfsExclusiveWrappedShardedDirBlocks := toBlocks(t, lsys, unixfsExclusiveWrappedShardedDir.Root, allSelector)
	unixfsExclusiveWrappedShardedDirOnlyBlocks := toBlocks(t, lsys, unixfsExclusiveWrappedShardedDir.Root, unixfsWrappedPreloadPathSelector)

	testCases := []struct {
		name   string
		blocks []expectedBlock
		roots  []cid.Cid
		carv2  bool
		err    string
		cfg    verifiedcar.Config
	}{
		{
			name:   "complete carv1",
			blocks: consumedBlocks(allBlocks),
			roots:  []cid.Cid{root1},
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:   "carv2 without AllowCARv2 errors",
			blocks: consumedBlocks(allBlocks),
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
			blocks: consumedBlocks(allBlocks),
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
			blocks: consumedBlocks(allBlocks),
			roots:  []cid.Cid{root1, root1},
			err:    "root CID mismatch",
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:   "carv1 with wrong root errors",
			blocks: consumedBlocks(allBlocks),
			roots:  []cid.Cid{tbc1.AllBlocks()[1].Cid()},
			err:    "root CID mismatch",
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:   "carv1 with extraneous trailing block errors",
			blocks: append(consumedBlocks(append([]block{}, allBlocks...)), expectedBlock{block{extraneousLnk.(cidlink.Link).Cid, extraneousByts}, true}),
			roots:  []cid.Cid{root1},
			err:    "extraneous block in CAR",
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:   "carv1 with extraneous trailing block errors, allow extraneous blocks",
			blocks: append(consumedBlocks(append([]block{}, allBlocks...)), expectedBlock{block{extraneousLnk.(cidlink.Link).Cid, extraneousByts}, true}),
			roots:  []cid.Cid{root1},
			cfg: verifiedcar.Config{
				Root:                  root1,
				Selector:              allSelector,
				AllowExtraneousBlocks: true,
			},
		},
		{
			name:   "carv1 with extraneous leading block errors",
			blocks: append(skippedBlocks([]block{{extraneousLnk.(cidlink.Link).Cid, extraneousByts}}), consumedBlocks(allBlocks)...),
			roots:  []cid.Cid{root1},
			err:    "unexpected block in CAR: " + extraneousLnk.(cidlink.Link).Cid.String() + " != " + allBlocks[0].cid.String(),
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},

		{
			name:   "carv1 with extraneous leading blocks, allow unexpected blocks",
			blocks: append(skippedBlocks([]block{{extraneousLnk.(cidlink.Link).Cid, extraneousByts}}), consumedBlocks(allBlocks)...),
			roots:  []cid.Cid{root1},
			cfg: verifiedcar.Config{
				Root:                  root1,
				AllowUnexpectedBlocks: true,
				Selector:              allSelector,
			},
		},
		{
			name:   "carv1 with out-of-order blocks errors",
			blocks: consumedBlocks(append(append([]block{}, allBlocks[50:]...), allBlocks[0:50]...)),
			roots:  []cid.Cid{root1},
			err:    "unexpected block in CAR: " + allBlocks[50].cid.String() + " != " + allBlocks[0].cid.String(),
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:   "carv1 with out-of-order blocks errors, allow unexpectedBlocks",
			blocks: append(append([]expectedBlock{}, skippedBlocks(allBlocks[50:])...), consumedBlocks(allBlocks)...),
			roots:  []cid.Cid{root1},
			cfg: verifiedcar.Config{
				Root:                  root1,
				AllowUnexpectedBlocks: true,
				Selector:              allSelector,
			},
		},
		{
			name:   "carv1 with mismatching CID errors",
			blocks: consumedBlocks(append(append([]block{}, allBlocks[0:99]...), block{allBlocks[99].cid, extraneousByts})),
			roots:  []cid.Cid{root1},
			err:    "mismatch in content integrity",
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:   "unixfs: large sharded file",
			blocks: consumedBlocks(unixfsFileBlocks),
			roots:  []cid.Cid{unixfsFile.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsFile.Root,
				Selector: allSelector,
			},
		},
		{
			name:   "unixfs: large directory",
			blocks: consumedBlocks(unixfsDirBlocks),
			roots:  []cid.Cid{unixfsDir.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsDir.Root,
				Selector: allSelector,
			},
		},
		{
			name:   "unixfs: large sharded directory",
			blocks: consumedBlocks(unixfsShardedDirBlocks),
			roots:  []cid.Cid{unixfsShardedDir.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsShardedDir.Root,
				Selector: allSelector,
			},
		},
		{
			name:   "unixfs: large sharded file with file scope",
			blocks: consumedBlocks(unixfsFileBlocks),
			roots:  []cid.Cid{unixfsFile.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsFile.Root,
				Selector: unixfsPreloadSelector,
			},
		},
		{
			// TODO: this is flaky, why?
			//     Error "extraneous block in CAR" does not contain "unexpected block in CAR"
			// it's always a directory.UnixFSBasicDir, we use preload match `.` which should
			// only want the first block. unixfsDirBlocks is created from an allSelector
			// traversal, why is unixfs-preload making a difference for just matching a
			// directory.UnixFSBasicDir.
			name:   "unixfs: all of large directory with file scope, errors",
			blocks: consumedBlocks(unixfsDirBlocks),
			roots:  []cid.Cid{unixfsDir.Root},
			err:    "extraneous block in CAR",
			cfg: verifiedcar.Config{
				Root:     unixfsDir.Root,
				Selector: unixfsPreloadSelector,
			},
		},
		{
			name:   "unixfs: all of large sharded directory with file scope, errors",
			blocks: consumedBlocks(unixfsShardedDirBlocks),
			roots:  []cid.Cid{unixfsShardedDir.Root},
			err:    "extraneous block in CAR",
			cfg: verifiedcar.Config{
				Root:     unixfsShardedDir.Root,
				Selector: unixfsPreloadSelector,
			},
		},
		{
			name:   "unixfs: all of large directory with file scope",
			blocks: consumedBlocks(unixfsPreloadDirBlocks),
			roots:  []cid.Cid{unixfsDir.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsDir.Root,
				Selector: unixfsPreloadSelector,
			},
		},
		{
			name:   "unixfs: all of large sharded directory with file scope",
			blocks: consumedBlocks(unixfsPreloadShardedDirBlocks),
			roots:  []cid.Cid{unixfsShardedDir.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsShardedDir.Root,
				Selector: unixfsPreloadSelector,
			},
		},
		{
			name:   "unixfs: pathed subset inside large directory with file scope, errors",
			blocks: consumedBlocks(unixfsDirBlocks),
			roots:  []cid.Cid{unixfsDir.Root},
			err:    "unexpected block in CAR",
			cfg: verifiedcar.Config{
				Root:     unixfsDir.Root,
				Selector: unixfsDirSubsetSelector,
			},
		},
		{
			name:   "unixfs: large sharded file wrapped in directories",
			blocks: consumedBlocks(unixfsWrappedFileBlocks),
			roots:  []cid.Cid{unixfsWrappedFile.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsWrappedFile.Root,
				Selector: allSelector,
			},
		},
		{
			// our wrapped file has additional in the nested directories
			name:   "unixfs: large sharded file wrapped in directories, pathed, errors",
			blocks: consumedBlocks(unixfsWrappedFileBlocks),
			roots:  []cid.Cid{unixfsWrappedFile.Root},
			err:    "unexpected block in CAR",
			cfg: verifiedcar.Config{
				Root:     unixfsWrappedFile.Root,
				Selector: unixfsWrappedPathSelector,
			},
		},
		{
			name:   "unixfs: large sharded file wrapped in directories, trimmed, pathed",
			blocks: consumedBlocks(unixfsTrimmedWrappedFileBlocks),
			roots:  []cid.Cid{unixfsWrappedFile.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsWrappedFile.Root,
				Selector: unixfsWrappedPathSelector,
			},
		},
		{
			name:   "unixfs: large sharded file wrapped in directories, trimmed, all, errors",
			blocks: consumedBlocks(unixfsTrimmedWrappedFileBlocks),
			roots:  []cid.Cid{unixfsWrappedFile.Root},
			err:    "unexpected block in CAR",
			cfg: verifiedcar.Config{
				Root:     unixfsWrappedFile.Root,
				Selector: allSelector,
			},
		},
		{
			name:   "unixfs: large sharded file wrapped in directories, exclusive, pathed",
			blocks: consumedBlocks(unixfsExclusiveWrappedFileBlocks),
			roots:  []cid.Cid{unixfsExclusiveWrappedFile.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsExclusiveWrappedFile.Root,
				Selector: unixfsWrappedPathSelector,
			},
		},
		{
			name:   "unixfs: large sharded dir wrapped in directories",
			blocks: consumedBlocks(unixfsWrappedShardedDirBlocks),
			roots:  []cid.Cid{unixfsWrappedShardedDir.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsWrappedShardedDir.Root,
				Selector: allSelector,
			},
		},
		{
			// our wrapped dir has additional in the nested directories
			name:   "unixfs: large sharded dir wrapped in directories, pathed, errors",
			blocks: consumedBlocks(unixfsWrappedShardedDirBlocks),
			roots:  []cid.Cid{unixfsWrappedShardedDir.Root},
			err:    "unexpected block in CAR",
			cfg: verifiedcar.Config{
				Root:     unixfsWrappedShardedDir.Root,
				Selector: unixfsWrappedPathSelector,
			},
		},
		{
			name:   "unixfs: large sharded dir wrapped in directories, trimmed, pathed",
			blocks: consumedBlocks(unixfsTrimmedWrappedShardedDirBlocks),
			roots:  []cid.Cid{unixfsWrappedShardedDir.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsWrappedShardedDir.Root,
				Selector: unixfsWrappedPathSelector,
			},
		},
		{
			name:   "unixfs: large sharded dir wrapped in directories, trimmed, preload, pathed",
			blocks: consumedBlocks(unixfsTrimmedWrappedShardedDirOnlyBlocks),
			roots:  []cid.Cid{unixfsWrappedShardedDir.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsWrappedShardedDir.Root,
				Selector: unixfsWrappedPreloadPathSelector,
			},
		},
		{
			name:   "unixfs: large sharded dir wrapped in directories, trimmed, all, errors",
			blocks: consumedBlocks(unixfsTrimmedWrappedShardedDirBlocks),
			roots:  []cid.Cid{unixfsWrappedShardedDir.Root},
			err:    "unexpected block in CAR",
			cfg: verifiedcar.Config{
				Root:     unixfsWrappedShardedDir.Root,
				Selector: allSelector,
			},
		},
		{
			name:   "unixfs: large sharded dir wrapped in directories, exclusive, pathed",
			blocks: consumedBlocks(unixfsExclusiveWrappedShardedDirBlocks),
			roots:  []cid.Cid{unixfsExclusiveWrappedShardedDir.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsExclusiveWrappedShardedDir.Root,
				Selector: unixfsWrappedPathSelector,
			},
		},
		{
			name:   "unixfs: large sharded dir wrapped in directories, exclusive, preload, pathed",
			blocks: consumedBlocks(unixfsExclusiveWrappedShardedDirOnlyBlocks),
			roots:  []cid.Cid{unixfsExclusiveWrappedShardedDir.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsExclusiveWrappedShardedDir.Root,
				Selector: unixfsWrappedPreloadPathSelector,
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

			store := &correctedMemStore{&memstore.Store{
				Bag: make(map[string][]byte),
			}}
			lsys := cidlink.DefaultLinkSystem()
			lsys.SetReadStorage(store)
			lsys.SetWriteStorage(store)
			bwo := lsys.StorageWriteOpener
			var writeCounter int
			var skipped int
			lsys.StorageWriteOpener = func(lc linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
				var buf bytes.Buffer
				return &buf, func(l datamodel.Link) error {
					for testCase.blocks[writeCounter+skipped].skipped {
						skipped++
					}
					req.Equal(testCase.blocks[writeCounter+skipped].cid, l.(cidlink.Link).Cid, "block %d", writeCounter)
					req.Equal(testCase.blocks[writeCounter+skipped].data, buf.Bytes(), "block %d", writeCounter)
					writeCounter++
					w, wc, err := bwo(lc)
					if err != nil {
						return err
					}
					buf.WriteTo(w)
					return wc(l)
				}, nil
			}

			carStream := makeCarStream(t, ctx, testCase.roots, testCase.blocks, testCase.carv2, testCase.err != "")
			blockCount, byteCount, err := testCase.cfg.Verify(ctx, carStream, lsys)
			// read the rest of data
			io.ReadAll(carStream)

			if testCase.err != "" {
				req.ErrorContains(err, testCase.err)
				req.Equal(uint64(0), blockCount)
				req.Equal(uint64(0), byteCount)
			} else {
				req.NoError(err)
				req.Equal(count(testCase.blocks), blockCount)
				req.Equal(sizeOf(testCase.blocks), byteCount)
				req.Equal(int(count(testCase.blocks)), writeCounter)
			}
		})
	}
}

func makeCarStream(
	t *testing.T,
	ctx context.Context,
	roots []cid.Cid,
	blocks []expectedBlock,
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

		carWriter, err := storage.NewWritable(carW, roots, car.WriteAsCarV1(!carv2), car.AllowDuplicatePuts(true))
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

type expectedBlock struct {
	block
	skipped bool
}

func consumedBlocks(blocks []block) []expectedBlock {
	expectedBlocks := make([]expectedBlock, 0, len(blocks))
	for _, block := range blocks {
		expectedBlocks = append(expectedBlocks, expectedBlock{block, false})
	}
	return expectedBlocks
}
func skippedBlocks(blocks []block) []expectedBlock {
	expectedBlocks := make([]expectedBlock, 0, len(blocks))
	for _, block := range blocks {
		expectedBlocks = append(expectedBlocks, expectedBlock{block, true})
	}
	return expectedBlocks
}

func count(blocks []expectedBlock) uint64 {
	total := uint64(0)
	for _, block := range blocks {
		if !block.skipped {
			total++
		}
	}
	return total
}

func sizeOf(blocks []expectedBlock) uint64 {
	total := uint64(0)
	for _, block := range blocks {
		if !block.skipped {
			total += uint64(len(block.data))
		}
	}
	return total
}

func toBlocks(t *testing.T, lsys linking.LinkSystem, root cid.Cid, sel selector.Selector) []block {
	blocks := make([]block, 0)
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)
	osro := lsys.StorageReadOpener
	lsys.StorageReadOpener = func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		r, err := osro(lc, l)
		if err != nil {
			return nil, err
		}
		byts, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block{l.(cidlink.Link).Cid, byts})
		return bytes.NewReader(byts), nil
	}
	var proto datamodel.NodePrototype = basicnode.Prototype.Any
	if root.Prefix().Codec == cid.DagProtobuf {
		proto = dagpb.Type.PBNode
	}
	rootNode, err := lsys.Load(linking.LinkContext{}, cidlink.Link{Cid: root}, proto)
	require.NoError(t, err)
	prog := traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem:                     lsys,
			LinkTargetNodePrototypeChooser: dagpb.AddSupportToChooser(basicnode.Chooser),
		},
	}
	vf := func(p traversal.Progress, n datamodel.Node, vr traversal.VisitReason) error { return nil }
	err = prog.WalkAdv(rootNode, sel, vf)
	require.NoError(t, err)

	return blocks
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

func mustCompile(selNode datamodel.Node) selector.Selector {
	sel, err := selector.CompileSelector(selNode)
	if err != nil {
		panic(err)
	}
	return sel
}
