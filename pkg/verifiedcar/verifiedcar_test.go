package verifiedcar_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/verifiedcar"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	gstestutil "github.com/ipfs/go-graphsync/testutil"
	"github.com/ipfs/go-unixfsnode"
	unixfs "github.com/ipfs/go-unixfsnode/testutil"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"
)

func TestVerifiedCar(t *testing.T) {
	ctx := context.Background()

	req := require.New(t)

	rndSeed := time.Now().UTC().UnixNano()
	t.Logf("random seed: %d", rndSeed)
	var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

	store := &testutil.CorrectedMemStore{ParentStore: &memstore.Store{
		Bag: make(map[string][]byte),
	}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.TrustedStorage = true
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)

	tbc1 := gstestutil.SetupBlockChain(ctx, t, lsys, 1000, 100)
	root1 := tbc1.TipLink.(cidlink.Link).Cid
	allBlocks := tbc1.AllBlocks()
	extraneousLnk, err := lsys.Store(linking.LinkContext{}, cidlink.LinkPrototype{Prefix: cid.Prefix{Version: 1, Codec: 0x71, MhType: 0x12, MhLength: 32}}, basicnode.NewString("borp"))
	req.NoError(err)
	extraneousByts, err := lsys.LoadRaw(linking.LinkContext{}, extraneousLnk)
	req.NoError(err)
	extraneousBlk, err := blocks.NewBlockWithCid(extraneousByts, extraneousLnk.(cidlink.Link).Cid)
	req.NoError(err)

	allSelector := selectorparse.CommonSelector_ExploreAllRecursively

	wrapPath := "/some/path/to/content"

	unixfsFile := testutil.GenerateNoDupes(func() unixfs.DirEntry { return unixfs.GenerateFile(t, &lsys, rndReader, 4<<20) })
	unixfsFileBlocks := testutil.ToBlocks(t, lsys, unixfsFile.Root, allSelector)

	unixfsFileWithDups := unixfs.GenerateFile(t, &lsys, testutil.ZeroReader{}, 4<<20)
	unixfsFileWithDupsBlocks := testutil.ToBlocks(t, lsys, unixfsFileWithDups.Root, allSelector)
	var unixfsDir unixfs.DirEntry
	var unixfsDirBlocks []blocks.Block
	for {
		unixfsDir = testutil.GenerateNoDupes(func() unixfs.DirEntry { return unixfs.GenerateDirectory(t, &lsys, rndReader, 8<<20, false) })
		unixfsDirBlocks = testutil.ToBlocks(t, lsys, unixfsDir.Root, allSelector)
		if len(unixfsDir.Children) > 2 { // we want at least 3 children to test the path subset selector
			break
		}
	}

	unixfsShardedDir := testutil.GenerateNoDupes(func() unixfs.DirEntry {
		return testutil.GenerateStrictlyNestedShardedDir(t, &lsys, rndReader, 8<<20)
	})
	unixfsShardedDirBlocks := testutil.ToBlocks(t, lsys, unixfsShardedDir.Root, allSelector)

	unixfsPreloadSelector := unixfsnode.MatchUnixFSPreloadSelector.Node()

	unixfsPreloadDirBlocks := testutil.ToBlocks(t, lsys, unixfsDir.Root, unixfsPreloadSelector)
	unixfsPreloadShardedDirBlocks := testutil.ToBlocks(t, lsys, unixfsShardedDir.Root, unixfsPreloadSelector)

	unixfsDirSubsetSelector := unixfsnode.UnixFSPathSelectorBuilder(unixfsDir.Children[1].Path, unixfsnode.MatchUnixFSPreloadSelector, false)

	unixfsWrappedPathSelector := unixfsnode.UnixFSPathSelectorBuilder(wrapPath, unixfsnode.ExploreAllRecursivelySelector, false)
	unixfsWrappedPreloadPathSelector := unixfsnode.UnixFSPathSelectorBuilder(wrapPath, unixfsnode.MatchUnixFSPreloadSelector, false)

	unixfsWrappedFile := testutil.GenerateNoDupes(func() unixfs.DirEntry { return unixfs.WrapContent(t, rndReader, &lsys, unixfsFile, wrapPath, false) })
	unixfsWrappedFileBlocks := testutil.ToBlocks(t, lsys, unixfsWrappedFile.Root, allSelector)
	// "trimmed" is similar to "exclusive" except that "trimmed" is a subset
	// of a larger DAG, whereas "exclusive" is a complete DAG.
	unixfsTrimmedWrappedFileBlocks := testutil.ToBlocks(t, lsys, unixfsWrappedFile.Root, unixfsWrappedPathSelector)
	unixfsExclusiveWrappedFile := testutil.GenerateNoDupes(func() unixfs.DirEntry { return unixfs.WrapContent(t, rndReader, &lsys, unixfsFile, wrapPath, true) })
	unixfsExclusiveWrappedFileBlocks := testutil.ToBlocks(t, lsys, unixfsExclusiveWrappedFile.Root, allSelector)

	unixfsWrappedShardedDir := testutil.GenerateNoDupes(func() unixfs.DirEntry {
		return unixfs.WrapContent(t, rndReader, &lsys, unixfsShardedDir, wrapPath, false)
	})
	unixfsWrappedShardedDirBlocks := testutil.ToBlocks(t, lsys, unixfsWrappedShardedDir.Root, allSelector)
	// "trimmed" is similar to "exclusive" except that "trimmed" is a subset
	// of a larger DAG, whereas "exclusive" is a complete DAG.
	unixfsTrimmedWrappedShardedDirBlocks := testutil.ToBlocks(t, lsys, unixfsWrappedShardedDir.Root, unixfsWrappedPathSelector)
	unixfsTrimmedWrappedShardedDirOnlyBlocks := testutil.ToBlocks(t, lsys, unixfsWrappedShardedDir.Root, unixfsWrappedPreloadPathSelector)
	unixfsExclusiveWrappedShardedDir := testutil.GenerateNoDupes(func() unixfs.DirEntry {
		return unixfs.WrapContent(t, rndReader, &lsys, unixfsShardedDir, wrapPath, true)
	})
	unixfsExclusiveWrappedShardedDirBlocks := testutil.ToBlocks(t, lsys, unixfsExclusiveWrappedShardedDir.Root, allSelector)
	unixfsExclusiveWrappedShardedDirOnlyBlocks := testutil.ToBlocks(t, lsys, unixfsExclusiveWrappedShardedDir.Root, unixfsWrappedPreloadPathSelector)

	mismatchedCidBlk, _ := blocks.NewBlockWithCid(extraneousByts, allBlocks[99].Cid())
	testCases := []struct {
		name            string
		blocks          []expectedBlock
		roots           []cid.Cid
		carv2           bool
		expectErr       string
		streamErr       error
		blockWriteErr   error
		cfg             verifiedcar.Config
		incomingHasDups bool
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
			name:      "carv2 without AllowCARv2 errors",
			blocks:    consumedBlocks(allBlocks),
			roots:     []cid.Cid{root1},
			carv2:     true,
			expectErr: "bad CAR version",
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
			name:      "carv1 with multiple roots errors",
			blocks:    consumedBlocks(allBlocks),
			roots:     []cid.Cid{root1, root1},
			expectErr: "root CID mismatch",
			cfg: verifiedcar.Config{
				Root:               root1,
				Selector:           allSelector,
				CheckRootsMismatch: true,
			},
		},
		{
			name:   "carv1 with multiple roots errors, no root cid mismatch",
			blocks: consumedBlocks(allBlocks),
			roots:  []cid.Cid{root1, root1},
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:      "carv1 with wrong root errors",
			blocks:    consumedBlocks(allBlocks),
			roots:     []cid.Cid{tbc1.AllBlocks()[1].Cid()},
			expectErr: "root CID mismatch",
			cfg: verifiedcar.Config{
				Root:               root1,
				Selector:           allSelector,
				CheckRootsMismatch: true,
			},
		},
		{
			name:      "carv1 with extraneous trailing block errors",
			blocks:    append(consumedBlocks(append([]blocks.Block{}, allBlocks...)), expectedBlock{extraneousBlk, true}),
			roots:     []cid.Cid{root1},
			expectErr: "extraneous block in CAR",
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:      "carv1 with extraneous leading block errors",
			blocks:    append(consumedBlocks([]blocks.Block{extraneousBlk}), consumedBlocks(allBlocks)...),
			roots:     []cid.Cid{root1},
			expectErr: "unexpected block in CAR: " + extraneousLnk.(cidlink.Link).Cid.String() + " != " + allBlocks[0].Cid().String(),
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:      "carv1 with out-of-order blocks errors",
			blocks:    consumedBlocks(append(append([]blocks.Block{}, allBlocks[50:]...), allBlocks[0:50]...)),
			roots:     []cid.Cid{root1},
			expectErr: "unexpected block in CAR: " + allBlocks[50].Cid().String() + " != " + allBlocks[0].Cid().String(),
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:      "carv1 with mismatching CID errors",
			blocks:    consumedBlocks(append(append([]blocks.Block{}, allBlocks[0:99]...), mismatchedCidBlk)),
			roots:     []cid.Cid{root1},
			expectErr: "mismatch in content integrity",
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:   "carv1 over budget errors",
			blocks: consumedBlocks(allBlocks),
			roots:  []cid.Cid{root1},
			expectErr: (&traversal.ErrBudgetExceeded{
				BudgetKind: "link",
				Path:       datamodel.ParsePath("Parents/0/Parents/0/Parents/0"),
				Link:       tbc1.LinkTipIndex(3),
			}).Error(),
			cfg: verifiedcar.Config{
				Root:      root1,
				Selector:  allSelector,
				MaxBlocks: 3,
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
			name:      "unixfs: all of large directory with file scope, errors",
			blocks:    consumedBlocks(unixfsDirBlocks),
			roots:     []cid.Cid{unixfsDir.Root},
			expectErr: "extraneous block in CAR",
			cfg: verifiedcar.Config{
				Root:     unixfsDir.Root,
				Selector: unixfsPreloadSelector,
			},
		},
		{
			name:      "unixfs: all of large sharded directory with file scope, errors",
			blocks:    consumedBlocks(unixfsShardedDirBlocks),
			roots:     []cid.Cid{unixfsShardedDir.Root},
			expectErr: "unexpected block in CAR:",
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
			name:      "unixfs: pathed subset inside large directory with file scope, errors",
			blocks:    consumedBlocks(unixfsDirBlocks),
			roots:     []cid.Cid{unixfsDir.Root},
			expectErr: "unexpected block in CAR",
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
			name:      "unixfs: large sharded file wrapped in directories, pathed, errors",
			blocks:    consumedBlocks(unixfsWrappedFileBlocks),
			roots:     []cid.Cid{unixfsWrappedFile.Root},
			expectErr: "unexpected block in CAR",
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
			name:      "unixfs: large sharded file wrapped in directories, trimmed, all, errors",
			blocks:    consumedBlocks(unixfsTrimmedWrappedFileBlocks),
			roots:     []cid.Cid{unixfsWrappedFile.Root},
			expectErr: "unexpected block in CAR",
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
			name:      "unixfs: large sharded dir wrapped in directories, pathed, errors",
			blocks:    consumedBlocks(unixfsWrappedShardedDirBlocks),
			roots:     []cid.Cid{unixfsWrappedShardedDir.Root},
			expectErr: "unexpected block in CAR",
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
			name:      "unixfs: large sharded dir wrapped in directories, trimmed, all, errors",
			blocks:    consumedBlocks(unixfsTrimmedWrappedShardedDirBlocks),
			roots:     []cid.Cid{unixfsWrappedShardedDir.Root},
			expectErr: "unexpected block in CAR",
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
		{
			name:   "unixfs: file with dups",
			blocks: append(append(consumedBlocks(unixfsFileWithDupsBlocks[:2]), skippedBlocks(unixfsFileWithDupsBlocks[2:len(unixfsFileWithDupsBlocks)-1])...), consumedBlocks(unixfsFileWithDupsBlocks[len(unixfsFileWithDupsBlocks)-1:])...),
			roots:  []cid.Cid{unixfsFileWithDups.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsFileWithDups.Root,
				Selector: allSelector,
			},
		},
		{
			name:      "unixfs: file with dups, incoming has dups, not allowed",
			blocks:    append(append(consumedBlocks(unixfsFileWithDupsBlocks[:2]), skippedBlocks(unixfsFileWithDupsBlocks[2:len(unixfsFileWithDupsBlocks)-1])...), consumedBlocks(unixfsFileWithDupsBlocks[len(unixfsFileWithDupsBlocks)-1:])...),
			expectErr: "unexpected block in CAR: " + unixfsFileWithDupsBlocks[2].Cid().String() + " != " + unixfsFileWithDupsBlocks[len(unixfsFileWithDupsBlocks)-1].Cid().String(),
			roots:     []cid.Cid{unixfsFileWithDups.Root},
			cfg: verifiedcar.Config{
				Root:     unixfsFileWithDups.Root,
				Selector: allSelector,
			},
			incomingHasDups: true,
		},
		{
			name:   "unixfs: file with dups, incoming has dups, allowed",
			blocks: append(append(consumedBlocks(unixfsFileWithDupsBlocks[:2]), skippedBlocks(unixfsFileWithDupsBlocks[2:len(unixfsFileWithDupsBlocks)-1])...), consumedBlocks(unixfsFileWithDupsBlocks[len(unixfsFileWithDupsBlocks)-1:])...),
			roots:  []cid.Cid{unixfsFileWithDups.Root},
			cfg: verifiedcar.Config{
				Root:               unixfsFileWithDups.Root,
				Selector:           allSelector,
				ExpectDuplicatesIn: true,
			},
			incomingHasDups: true,
		},
		{
			name:   "unixfs: file with dups, duplicate writes on",
			blocks: consumedBlocks(unixfsFileWithDupsBlocks),
			roots:  []cid.Cid{unixfsFileWithDups.Root},
			cfg: verifiedcar.Config{
				Root:               unixfsFileWithDups.Root,
				Selector:           allSelector,
				WriteDuplicatesOut: true,
			},
		},
		{
			name:   "unixfs: file with dups, duplicate writes on, incoming dups",
			blocks: consumedBlocks(unixfsFileWithDupsBlocks),
			roots:  []cid.Cid{unixfsFileWithDups.Root},
			cfg: verifiedcar.Config{
				Root:               unixfsFileWithDups.Root,
				Selector:           allSelector,
				WriteDuplicatesOut: true,
				ExpectDuplicatesIn: true,
			},
			incomingHasDups: true,
		},
		{
			name:      "premature stream end errors",
			blocks:    consumedBlocks(allBlocks),
			roots:     []cid.Cid{root1},
			expectErr: "something wicked this way comes",
			streamErr: errors.New("something wicked this way comes"),
			cfg: verifiedcar.Config{
				Root:     root1,
				Selector: allSelector,
			},
		},
		{
			name:          "block write error errors",
			blocks:        consumedBlocks(allBlocks),
			roots:         []cid.Cid{root1},
			expectErr:     "something wicked this way comes",
			blockWriteErr: errors.New("something wicked this way comes"),
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

			store := &testutil.CorrectedMemStore{ParentStore: &memstore.Store{
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
					if testCase.blockWriteErr != nil && writeCounter+skipped == len(testCase.blocks)/2 {
						return testCase.blockWriteErr
					}
					for testCase.blocks[writeCounter+skipped].skipped {
						skipped++
					}
					req.Equal(testCase.blocks[writeCounter+skipped].Cid().String(), l.(cidlink.Link).Cid.String(), "block %d", writeCounter)
					req.Equal(testCase.blocks[writeCounter+skipped].RawData(), buf.Bytes(), "block %d", writeCounter)
					writeCounter++
					w, wc, err := bwo(lc)
					if err != nil {
						return err
					}
					buf.WriteTo(w)
					return wc(l)
				}, nil
			}

			carStream := makeCarStream(t, ctx, testCase.roots, testCase.blocks, testCase.carv2, testCase.expectErr != "", testCase.incomingHasDups, testCase.streamErr)
			blockCount, byteCount, err := testCase.cfg.VerifyCar(ctx, carStream, lsys)

			// read the rest of data
			io.ReadAll(carStream)

			if testCase.expectErr != "" {
				req.ErrorContains(err, testCase.expectErr)
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
	allowDuplicatePuts bool,
	streamError error,
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

		carWriter, err := storage.NewWritable(carW, roots, car.WriteAsCarV1(!carv2), car.AllowDuplicatePuts(allowDuplicatePuts))
		req.NoError(err)
		if err != nil {
			return
		}
		for ii, block := range blocks {
			if streamError != nil && ii == len(blocks)/2 {
				w.CloseWithError(streamError)
				return
			}
			err := carWriter.Put(ctx, block.Cid().KeyString(), block.RawData())
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

type expectedBlock struct {
	blocks.Block
	skipped bool
}

func consumedBlocks(blocks []blocks.Block) []expectedBlock {
	expectedBlocks := make([]expectedBlock, 0, len(blocks))
	for _, block := range blocks {
		expectedBlocks = append(expectedBlocks, expectedBlock{block, false})
	}
	return expectedBlocks
}

func skippedBlocks(blocks []blocks.Block) []expectedBlock {
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
			total += uint64(len(block.RawData()))
		}
	}
	return total
}
