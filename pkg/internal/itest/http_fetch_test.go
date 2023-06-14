//go:build !race

package itest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/filecoin-project/lassie/pkg/internal/itest/mocknet"
	"github.com/filecoin-project/lassie/pkg/internal/itest/testpeer"
	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/retriever"
	httpserver "github.com/filecoin-project/lassie/pkg/server/http"
	"github.com/filecoin-project/lassie/pkg/verifiedcar"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	unixfs "github.com/ipfs/go-unixfsnode/testutil"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"

	_ "net/http/pprof"
)

// DEBUG_DATA, when true, will write source and received data to CARs
// for inspection if tests fail; otherwise they are cleaned up as tests
// proceed.
const DEBUG_DATA = false

func TestHttpFetch(t *testing.T) {
	entityQuery := func(q url.Values, _ []testpeer.TestPeer) {
		q.Set("dag-scope", "entity")
	}
	blockQuery := func(q url.Values, _ []testpeer.TestPeer) {
		q.Set("dag-scope", "block")
	}
	noDups := func(header http.Header) {
		header.Set("Accept", "application/vnd.ipld.car;order=dfs;version=1;dups=n;")
	}
	type headerSetter func(http.Header)
	type queryModifier func(url.Values, []testpeer.TestPeer)
	type bodyValidator func(*testing.T, unixfs.DirEntry, []byte)
	type lassieOptsGen func(*testing.T, *mocknet.MockRetrievalNet) []lassie.LassieOption
	wrapPath := "/want2/want1/want0"

	testCases := []struct {
		name             string
		graphsyncRemotes int
		bitswapRemotes   int
		httpRemotes      int
		disableGraphsync bool
		expectFail       bool
		expectUncleanEnd bool
		modifyHttpConfig func(httpserver.HttpServerConfig) httpserver.HttpServerConfig
		generate         func(*testing.T, io.Reader, []testpeer.TestPeer) []unixfs.DirEntry
		paths            []string
		setHeader        headerSetter
		modifyQueries    []queryModifier
		validateBodies   []bodyValidator
		lassieOpts       lassieOptsGen
	}{
		{
			name:             "graphsync large sharded file",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)}
			},
		},
		{
			name:           "bitswap large sharded file",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)}
			},
		},
		{
			name:        "http large sharded file",
			httpRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)}
			},
		},
		{
			name:             "graphsync large directory",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false)}
			},
		},
		{
			name:           "bitswap large directory",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false)}
			},
		},
		{
			name:        "http large directory",
			httpRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false)}
			},
		},
		{
			name:             "graphsync large sharded directory",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true)}
			},
		},
		{
			name:           "bitswap large sharded directory",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true)}
			},
		},
		{
			name:        "http large sharded directory",
			httpRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true)}
			},
		},
		{
			name:             "graphsync max block limit",
			graphsyncRemotes: 1,
			expectUncleanEnd: true,
			modifyHttpConfig: func(cfg httpserver.HttpServerConfig) httpserver.HttpServerConfig {
				cfg.MaxBlocksPerRequest = 3
				return cfg
			},
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)}
			},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// 3 blocks max, start at the root and then two blocks into the sharded data
				wantCids := []cid.Cid{
					srcData.Root,
					srcData.SelfCids[0],
					srcData.SelfCids[1],
				}
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:             "graphsync max block limit in request",
			graphsyncRemotes: 1,
			expectUncleanEnd: true,
			modifyQueries: []queryModifier{
				func(values url.Values, _ []testpeer.TestPeer) {
					values.Add("blockLimit", "3")
				},
			},
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)}
			},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// 3 blocks max, start at the root and then two blocks into the sharded data
				wantCids := []cid.Cid{
					srcData.Root,
					srcData.SelfCids[0],
					srcData.SelfCids[1],
				}
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:             "bitswap max block limit",
			bitswapRemotes:   1,
			expectUncleanEnd: true,
			modifyHttpConfig: func(cfg httpserver.HttpServerConfig) httpserver.HttpServerConfig {
				cfg.MaxBlocksPerRequest = 3
				return cfg
			},
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)}
			},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// 3 blocks max, start at the root and then two blocks into the sharded data
				wantCids := []cid.Cid{
					srcData.Root,
					srcData.SelfCids[0],
					srcData.SelfCids[1],
				}
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:             "http max block limit",
			httpRemotes:      1,
			expectUncleanEnd: true,
			modifyHttpConfig: func(cfg httpserver.HttpServerConfig) httpserver.HttpServerConfig {
				cfg.MaxBlocksPerRequest = 3
				return cfg
			},
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)}
			},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// 3 blocks max, start at the root and then two blocks into the sharded data
				wantCids := []cid.Cid{
					srcData.Root,
					srcData.SelfCids[0],
					srcData.SelfCids[1],
				}
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:             "bitswap block timeout from missing block",
			bitswapRemotes:   1,
			expectUncleanEnd: true,
			lassieOpts: func(t *testing.T, mrn *mocknet.MockRetrievalNet) []lassie.LassieOption {
				return []lassie.LassieOption{lassie.WithProviderTimeout(500 * time.Millisecond)}
			},
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				file := unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)
				remotes[0].Blockstore().DeleteBlock(context.Background(), file.SelfCids[2])
				return []unixfs.DirEntry{file}
			},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// 3 blocks max, start at the root and then two blocks into the sharded data
				wantCids := []cid.Cid{
					srcData.Root,
					srcData.SelfCids[0],
					srcData.SelfCids[1],
				}
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:           "same content, http missing block, bitswap completes",
			bitswapRemotes: 1,
			httpRemotes:    1,
			lassieOpts: func(t *testing.T, mrn *mocknet.MockRetrievalNet) []lassie.LassieOption {
				return []lassie.LassieOption{lassie.WithProviderTimeout(500 * time.Millisecond)}
			},
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				file := unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)
				for _, c := range file.SelfCids {
					blk, err := remotes[0].Blockstore().Get(context.Background(), c)
					require.NoError(t, err)
					writer, commit, err := remotes[1].LinkSystem.StorageWriteOpener(linking.LinkContext{Ctx: context.Background()})
					require.NoError(t, err)
					_, err = writer.Write(blk.RawData())
					require.NoError(t, err)
					err = commit(cidlink.Link{Cid: c})
					require.NoError(t, err)
				}
				remotes[1].Blockstore().DeleteBlock(context.Background(), file.SelfCids[3])
				return []unixfs.DirEntry{file}
			},
		},
		{
			// dag-scope entity fetch should get the same DAG as full for a plain file
			name:             "graphsync large sharded file, dag-scope entity",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)}
			},
			modifyQueries: []queryModifier{entityQuery},
		},
		{
			// dag-scope entity fetch should get the same DAG as full for a plain file
			name:           "bitswap large sharded file, dag-scope entity",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)}
			},
			modifyQueries: []queryModifier{entityQuery},
		},
		{
			name:             "graphsync nested large sharded file, with path, dag-scope entity",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateFile(t, lsys, rndReader, 4<<20), wrapPath, false)}
			},
			paths:         []string{wrapPath},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full file)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:           "bitswap nested large sharded file, with path, dag-scope entity",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateFile(t, lsys, rndReader, 4<<20), wrapPath, false)}
			},
			paths:         []string{wrapPath},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full file)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:        "http nested large sharded file, with path, dag-scope entity",
			httpRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateFile(t, lsys, rndReader, 4<<20), wrapPath, false)}
			},
			paths:         []string{wrapPath},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full file)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:             "graphsync large directory, dag-scope entity",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false)}
			},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// expect a CAR of one block, to represent the root directory we asked for
				validateCarBody(t, body, srcData.Root, []cid.Cid{srcData.Root}, true)
			}},
		},
		{
			name:           "bitswap large directory, dag-scope entity",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false)}
			},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// expect a CAR of one block, to represent the root directory we asked for
				validateCarBody(t, body, srcData.Root, []cid.Cid{srcData.Root}, true)
			}},
		},
		{
			name:        "http large directory, dag-scope entity",
			httpRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false)}
			},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// expect a CAR of one block, to represent the root directory we asked for
				validateCarBody(t, body, srcData.Root, []cid.Cid{srcData.Root}, true)
			}},
		},
		{
			name:             "graphsync nested large directory, with path, dag-scope entity",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false), wrapPath, false)}
			},
			paths:         []string{wrapPath},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full dir)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:           "bitswap nested large directory, with path, dag-scope entity",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false), wrapPath, false)}
			},
			paths:         []string{wrapPath},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full dir)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:        "http nested large directory, with path, dag-scope entity",
			httpRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false), wrapPath, false)}
			},
			paths:         []string{wrapPath},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full dir)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:             "graphsync nested large directory, with path, full",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false), wrapPath, false)}
			},
			paths: []string{wrapPath},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full dir)
				)
				// validate we got the dag-scope entity form
				validateCarBody(t, body, srcData.Root, wantCids, false)
				// validate that we got the full depth form under the path
				gotDir := CarToDirEntry(t, bytes.NewReader(body), srcData.Children[1].Children[1].Children[1].Root, true)
				gotDir.Path = "want0"
				unixfs.CompareDirEntries(t, srcData.Children[1].Children[1].Children[1], gotDir)
			}},
		},
		{
			name:           "bitswap nested large directory, with path, full",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false), wrapPath, false)}
			},
			paths: []string{wrapPath},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full dir)
				)
				// validate we got the dag-scope entity form
				validateCarBody(t, body, srcData.Root, wantCids, false)
				// validate that we got the full depth form under the path
				gotDir := CarToDirEntry(t, bytes.NewReader(body), srcData.Children[1].Children[1].Children[1].Root, true)
				gotDir.Path = "want0"
				unixfs.CompareDirEntries(t, srcData.Children[1].Children[1].Children[1], gotDir)
			}},
		},
		{
			name:        "bitswap nested large directory, with path, full",
			httpRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false), wrapPath, false)}
			},
			paths: []string{wrapPath},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full dir)
				)
				// validate we got the dag-scope entity form
				validateCarBody(t, body, srcData.Root, wantCids, false)
				// validate that we got the full depth form under the path
				gotDir := CarToDirEntry(t, bytes.NewReader(body), srcData.Children[1].Children[1].Children[1].Root, true)
				gotDir.Path = "want0"
				unixfs.CompareDirEntries(t, srcData.Children[1].Children[1].Children[1], gotDir)
			}},
		},
		{
			name:             "graphsync nested large sharded directory, dag-scope entity",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true)}
			},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// sharded directory contains multiple blocks, so we expect a CAR with
				// exactly those blocks
				validateCarBody(t, body, srcData.Root, srcData.SelfCids, true)
			}},
		},
		{
			name:           "bitswap nested large sharded directory, dag-scope entity",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true)}
			},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// sharded directory contains multiple blocks, so we expect a CAR with
				// exactly those blocks
				validateCarBody(t, body, srcData.Root, srcData.SelfCids, true)
			}},
		},
		{
			name:        "http nested large sharded directory, dag-scope entity",
			httpRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true)}
			},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// sharded directory contains multiple blocks, so we expect a CAR with
				// exactly those blocks
				validateCarBody(t, body, srcData.Root, srcData.SelfCids, true)
			}},
		},
		{
			name:             "graphsync nested large sharded directory, with path, dag-scope entity",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true), wrapPath, false)}
			},
			paths:         []string{wrapPath},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full dir)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:           "bitswap nested large sharded directory, with path, dag-scope entity",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true), wrapPath, false)}
			},
			paths:         []string{wrapPath},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full dir)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:        "http nested large sharded directory, with path, dag-scope entity",
			httpRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true), wrapPath, false)}
			},
			paths:         []string{wrapPath},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full dir)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:             "graphsync nested large sharded directory, with path, full",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true), wrapPath, false)}
			},
			paths: []string{wrapPath},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full dir)
				)
				// validate we got the dag-scope entity form
				validateCarBody(t, body, srcData.Root, wantCids, false)
				// validate that we got the full depth form under the path
				gotDir := CarToDirEntry(t, bytes.NewReader(body), srcData.Children[1].Children[1].Children[1].Root, true)
				gotDir.Path = "want0"
				unixfs.CompareDirEntries(t, srcData.Children[1].Children[1].Children[1], gotDir)
			}},
		},
		{
			name:           "bitswap nested large sharded directory, with path, full",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true), wrapPath, false)}
			},
			paths: []string{wrapPath},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full dir)
				)
				// validate we got the dag-scope entity form
				validateCarBody(t, body, srcData.Root, wantCids, false)
				// validate that we got the full depth form under the path
				gotDir := CarToDirEntry(t, bytes.NewReader(body), srcData.Children[1].Children[1].Children[1].Root, true)
				gotDir.Path = "want0"
				unixfs.CompareDirEntries(t, srcData.Children[1].Children[1].Children[1], gotDir)
			}},
		},
		{
			name:        "http nested large sharded directory, with path, full",
			httpRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true), wrapPath, false)}
			},
			paths: []string{wrapPath},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full dir)
				)
				// validate we got the dag-scope entity form
				validateCarBody(t, body, srcData.Root, wantCids, false)
				// validate that we got the full depth form under the path
				gotDir := CarToDirEntry(t, bytes.NewReader(body), srcData.Children[1].Children[1].Children[1].Root, true)
				gotDir.Path = "want0"
				unixfs.CompareDirEntries(t, srcData.Children[1].Children[1].Children[1], gotDir)
			}},
		},
		{
			// A very contrived example - we spread the content generated for this test across 4 peers,
			// then we also make sure the root is in all of them, so the CandidateFinder will return them
			// all. The retriever should then form a swarm of 4 peers and fetch the content from across
			// the set.
			name:           "bitswap, nested large sharded directory, spread across multiple peers, with path, dag-scope entity",
			bitswapRemotes: 4,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				// rotating linksystem - each block will be written to a different remote
				lsys := cidlink.DefaultLinkSystem()
				var blkIdx int
				lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
					defer func() { blkIdx++ }()
					return remotes[blkIdx%len(remotes)].LinkSystem.StorageWriteOpener(lctx)
				}
				lsys.TrustedStorage = true
				// generate data
				data := unixfs.WrapContent(t, rndReader, &lsys, unixfs.GenerateDirectory(t, &lsys, rndReader, 16<<20, true), wrapPath, false)

				// copy the root block to all remotes
				lctx := ipld.LinkContext{}
				rootLnk := cidlink.Link{Cid: data.Root}
				// the root should be the last written block, so we should be able to
				// find it on remote: (blkIdx-1)%len(remotes)
				blkRdr, err := remotes[(blkIdx-1)%len(remotes)].LinkSystem.StorageReadOpener(lctx, rootLnk)
				require.NoError(t, err)
				blk, err := io.ReadAll(blkRdr)
				require.NoError(t, err)
				for _, remote := range remotes {
					w, wc, err := remote.LinkSystem.StorageWriteOpener(lctx)
					require.NoError(t, err)
					_, err = w.Write(blk)
					require.NoError(t, err)
					require.NoError(t, wc(rootLnk))
				}

				return []unixfs.DirEntry{data}
			},
			paths:         []string{wrapPath},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full dir)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:           "two separate, parallel bitswap retrievals",
			bitswapRemotes: 2,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{
					unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20),
					unixfs.GenerateDirectory(t, remotes[1].LinkSystem, rndReader, 16<<20, false),
				}
			},
		},
		{
			name:             "two separate, parallel graphsync retrievals",
			graphsyncRemotes: 2,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{
					unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20),
					unixfs.GenerateDirectory(t, remotes[1].LinkSystem, rndReader, 16<<20, false),
				}
			},
		},
		{
			name:             "two separate, parallel graphsync retrievals, with graphsync disabled",
			graphsyncRemotes: 2,
			disableGraphsync: true,
			expectFail:       true,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{
					unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20),
					unixfs.GenerateDirectory(t, remotes[1].LinkSystem, rndReader, 16<<20, false),
				}
			},
		},
		{
			name:             "parallel, separate graphsync and bitswap retrievals",
			graphsyncRemotes: 1,
			bitswapRemotes:   1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{
					unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20),
					unixfs.GenerateDirectory(t, remotes[1].LinkSystem, rndReader, 16<<20, false),
				}
			},
		},
		{
			// dag-scope block fetch should only get the the root node for a plain file
			name:             "graphsync large sharded file, dag-scope block",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)}
			},
			modifyQueries: []queryModifier{blockQuery},
			validateBodies: []bodyValidator{
				func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
					wantCids := []cid.Cid{
						srcData.Root, // "/""
					}
					validateCarBody(t, body, srcData.Root, wantCids, true)
				},
			},
		},
		{
			name:             "graphsync nested large sharded file, with path, dag-scope block",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateFile(t, lsys, rndReader, 4<<20), wrapPath, false)}
			},
			paths:         []string{wrapPath},
			modifyQueries: []queryModifier{blockQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := []cid.Cid{
					srcData.Root,                                     // "/""
					srcData.Children[1].Root,                         // "/want2"
					srcData.Children[1].Children[1].Root,             // "/want2/want1"
					srcData.Children[1].Children[1].Children[1].Root, // wrapPath
				}
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:             "graphsync large sharded file, fixedPeer",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				fileEntry := unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)
				// wipe content routing information for remote
				remotes[0].Cids = make(map[cid.Cid]struct{})
				return []unixfs.DirEntry{fileEntry}
			},
			modifyQueries: []queryModifier{func(v url.Values, tp []testpeer.TestPeer) {
				multiaddrs, _ := peer.AddrInfoToP2pAddrs(tp[0].AddrInfo())
				maStrings := make([]string, 0, len(multiaddrs))
				for _, ma := range multiaddrs {
					maStrings = append(maStrings, ma.String())
				}
				v.Set("providers", strings.Join(maStrings, ","))
			}},
		},
		{
			name:             "graphsync large sharded file, fixedPeer through startup opts",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				fileEntry := unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)
				// wipe content routing information for remote
				remotes[0].Cids = make(map[cid.Cid]struct{})
				return []unixfs.DirEntry{fileEntry}
			},
			lassieOpts: func(t *testing.T, mrn *mocknet.MockRetrievalNet) []lassie.LassieOption {
				return []lassie.LassieOption{lassie.WithFinder(retriever.NewDirectCandidateFinder(mrn.Self, []peer.AddrInfo{*mrn.Remotes[0].AddrInfo()}))}
			},
		},
		{
			name:           "bitswap large sharded file, fixedPeer",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				fileEntry := unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)
				// wipe content routing information for remote
				remotes[0].Cids = make(map[cid.Cid]struct{})
				return []unixfs.DirEntry{fileEntry}
			},
			modifyQueries: []queryModifier{func(v url.Values, tp []testpeer.TestPeer) {
				multiaddrs, _ := peer.AddrInfoToP2pAddrs(tp[0].AddrInfo())
				maStrings := make([]string, 0, len(multiaddrs))
				for _, ma := range multiaddrs {
					maStrings = append(maStrings, ma.String())
				}
				v.Set("providers", strings.Join(maStrings, ","))
			}},
		},
		{
			name:           "bitswap large sharded file, fixedPeer through startup opts",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				fileEntry := unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)
				// wipe content routing information for remote
				remotes[0].Cids = make(map[cid.Cid]struct{})
				return []unixfs.DirEntry{fileEntry}
			},
			lassieOpts: func(t *testing.T, mrn *mocknet.MockRetrievalNet) []lassie.LassieOption {
				return []lassie.LassieOption{lassie.WithFinder(retriever.NewDirectCandidateFinder(mrn.Self, []peer.AddrInfo{*mrn.Remotes[0].AddrInfo()}))}
			},
		},
		{
			name:        "http large sharded file with dups",
			httpRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, testutil.ZeroReader{}, 4<<20)}
			},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				store := &testutil.CorrectedMemStore{Store: &memstore.Store{
					Bag: make(map[string][]byte),
				}}
				lsys := cidlink.DefaultLinkSystem()
				lsys.SetReadStorage(store)
				lsys.SetWriteStorage(store)
				_, _, err := verifiedcar.Config{
					Root:               srcData.Root,
					Selector:           selectorparse.CommonSelector_ExploreAllRecursively,
					ExpectDuplicatesIn: true,
				}.VerifyCar(context.Background(), bytes.NewReader(body), lsys)
				require.NoError(t, err)
			}},
		},
		{
			name:        "http large sharded file with dups, no dups response requested",
			httpRemotes: 1,
			setHeader:   noDups,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, testutil.ZeroReader{}, 4<<20)}
			},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := []cid.Cid{
					srcData.Root, // "/""
					srcData.SelfCids[1],
					srcData.SelfCids[len(srcData.SelfCids)-1],
				}
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			rndSeed := time.Now().UTC().UnixNano()
			t.Logf("random seed: %d", rndSeed)
			var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

			mrn := mocknet.NewMockRetrievalNet(ctx, t)
			mrn.AddGraphsyncPeers(testCase.graphsyncRemotes)
			finishedChans := make([]chan []datatransfer.Event, 0)
			for _, r := range mrn.Remotes {
				finishedChans = append(finishedChans, mocknet.SetupRetrieval(t, r))
			}
			mrn.AddBitswapPeers(testCase.bitswapRemotes)
			mrn.AddHttpPeers(testCase.httpRemotes)

			require.NoError(t, mrn.MN.LinkAll())

			carFiles := debugRemotes(t, ctx, testCase.name, mrn.Remotes)
			srcData := testCase.generate(t, rndReader, mrn.Remotes)

			// Setup a new lassie
			req := require.New(t)
			var customOpts []lassie.LassieOption
			if testCase.lassieOpts != nil {
				customOpts = testCase.lassieOpts(t, mrn)
			}
			opts := append([]lassie.LassieOption{lassie.WithProviderTimeout(20 * time.Second),
				lassie.WithHost(mrn.Self),
				lassie.WithFinder(mrn.Finder),
			}, customOpts...)
			if testCase.disableGraphsync {
				opts = append(opts, lassie.WithProtocols([]multicodec.Code{multicodec.TransportBitswap}))
			}
			lassie, err := lassie.NewLassie(
				ctx,
				opts...,
			)
			req.NoError(err)

			// Start an HTTP server
			cfg := httpserver.HttpServerConfig{Address: "127.0.0.1", Port: 0, TempDir: t.TempDir()}
			if testCase.modifyHttpConfig != nil {
				cfg = testCase.modifyHttpConfig(cfg)
			}
			httpServer, err := httpserver.NewHttpServer(ctx, lassie, cfg)
			req.NoError(err)
			serverError := make(chan error, 1)
			go func() {
				serverError <- httpServer.Start()
			}()

			responseChans := make([]chan *http.Response, 0)
			for i := 0; i < len(srcData); i++ {
				responseChan := make(chan *http.Response, 1)
				responseChans = append(responseChans, responseChan)
				go func(i int) {
					// Make a request for our CID and read the complete CAR bytes
					var path string
					if testCase.paths != nil && testCase.paths[i] != "" {
						path = testCase.paths[i]
					}
					addr := fmt.Sprintf("http://%s/ipfs/%s%s", httpServer.Addr(), srcData[i].Root.String(), path)
					getReq, err := http.NewRequest("GET", addr, nil)
					req.NoError(err)
					if testCase.setHeader == nil {
						getReq.Header.Add("Accept", "application/vnd.ipld.car")
					} else {
						testCase.setHeader(getReq.Header)
					}
					if testCase.modifyQueries != nil && testCase.modifyQueries[i] != nil {
						q := getReq.URL.Query()
						testCase.modifyQueries[i](q, mrn.Remotes)
						getReq.URL.RawQuery = q.Encode()
					}
					t.Log("Fetching", getReq.URL.String())
					resp, err := http.DefaultClient.Do(getReq)
					req.NoError(err)
					responseChan <- resp
				}(i)
			}

			responses := make([]*http.Response, 0)
			for _, responseChan := range responseChans {
				select {
				case resp := <-responseChan:
					responses = append(responses, resp)
				case <-ctx.Done():
					req.FailNow("Did not receive responses")
				}
			}

			if !testCase.disableGraphsync {
				// wait for graphsync retrievals to finish on the remotes
				var wg sync.WaitGroup
				wg.Add(len(finishedChans))
				for _, finishedChan := range finishedChans {
					go func(finishedChan chan []datatransfer.Event) {
						mocknet.WaitForFinish(ctx, t, finishedChan, 1*time.Second)
						wg.Done()
					}(finishedChan)
				}
				wg.Wait()
			}

			for i, resp := range responses {
				if testCase.expectFail {
					req.Equal(http.StatusGatewayTimeout, resp.StatusCode)
				} else {
					if resp.StatusCode != http.StatusOK {
						body, err := io.ReadAll(resp.Body)
						req.NoError(err)
						req.Failf("200 response code not received", "got code: %d, body: %s", resp.StatusCode, string(body))
					}
					req.Equal(fmt.Sprintf(`attachment; filename="%s.car"`, srcData[i].Root.String()), resp.Header.Get("Content-Disposition"))
					req.Equal("none", resp.Header.Get("Accept-Ranges"))
					req.Equal("public, max-age=29030400, immutable", resp.Header.Get("Cache-Control"))
					req.Equal("application/vnd.ipld.car; version=1", resp.Header.Get("Content-Type"))
					req.Equal("nosniff", resp.Header.Get("X-Content-Type-Options"))
					etagStart := fmt.Sprintf(`"%s.car.`, srcData[i].Root.String())
					etagGot := resp.Header.Get("ETag")
					req.True(strings.HasPrefix(etagGot, etagStart), "ETag should start with [%s], got [%s]", etagStart, etagGot)
					req.Equal(`"`, etagGot[len(etagGot)-1:], "ETag should end with a quote")
					var path string
					if testCase.paths != nil && testCase.paths[i] != "" {
						path = testCase.paths[i]
					}
					req.Equal(fmt.Sprintf("/ipfs/%s%s", srcData[i].Root.String(), path), resp.Header.Get("X-Ipfs-Path"))
					requestId := resp.Header.Get("X-Trace-Id")
					require.NotEmpty(t, requestId)
					_, err := uuid.Parse(requestId)
					req.NoError(err)
					body, err := io.ReadAll(resp.Body)
					if !testCase.expectUncleanEnd {
						req.NoError(err)
					} else {
						req.Error(err)
					}
					err = resp.Body.Close()
					req.NoError(err)

					if DEBUG_DATA {
						t.Logf("Creating CAR %s in temp dir", fmt.Sprintf("%s_received%d.car", testCase.name, i))
						dstf, err := os.CreateTemp("", fmt.Sprintf("%s_received%d.car", testCase.name, i))
						req.NoError(err)
						t.Logf("Writing received data to CAR @ %s", dstf.Name())
						_, err = dstf.Write(body)
						req.NoError(err)
						carFiles = append(carFiles, dstf)
					}

					if testCase.validateBodies != nil && testCase.validateBodies[i] != nil {
						testCase.validateBodies[i](t, srcData[i], body)
					} else {
						// gotDir := CarToDirEntry(t, bytes.NewReader(body), srcData[i].Root, true)
						gotLsys := CarBytesLinkSystem(t, bytes.NewReader(body))
						gotDir := unixfs.ToDirEntry(t, gotLsys, srcData[i].Root, true)
						unixfs.CompareDirEntries(t, srcData[i], gotDir)
					}
				}
			}

			if DEBUG_DATA {
				for _, cf := range carFiles {
					req.NoError(cf.Close())
					req.NoError(os.Remove(cf.Name()))
				}
				t.Logf("Cleaned up CARs")
			}

			err = httpServer.Close()
			req.NoError(err)
			select {
			case <-ctx.Done():
				req.FailNow("server failed to shut down")
			case err = <-serverError:
				req.NoError(err)
			}
		})
	}
}

// validateCarBody reads the given bytes as a CAR, validates the root is correct
// and that it contains all of the wantCids (not strictly in order). If
// onlyWantCids is true, it also validates that wantCids are the only CIDs in
// the CAR (with no duplicates).
func validateCarBody(t *testing.T, body []byte, root cid.Cid, wantCids []cid.Cid, onlyWantCids bool) {
	br, err := carv2.NewBlockReader(bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, []cid.Cid{root}, br.Roots)
	gotCids := make([]cid.Cid, 0)
	for {
		blk, err := br.Next()
		if err != nil {
			require.EqualError(t, err, io.EOF.Error())
			break
		}
		gotCids = append(gotCids, blk.Cid())
	}
	for _, cw := range wantCids {
		var found bool
		for _, cg := range gotCids {
			if cw.Equals(cg) {
				found = true
				break
			}
		}
		require.True(t, found)
	}
	if onlyWantCids {
		require.Len(t, gotCids, len(wantCids))
	}
}

func debugRemotes(t *testing.T, ctx context.Context, name string, remotes []testpeer.TestPeer) []*os.File {
	if !DEBUG_DATA {
		return nil
	}
	carFiles := make([]*os.File, 0)
	for ii, r := range remotes {
		func(ii int, r testpeer.TestPeer) {
			carFile, err := os.CreateTemp("", fmt.Sprintf("%s_remote%d.car", name, ii))
			require.NoError(t, err)
			t.Logf("Writing source data to CAR @ %s", carFile.Name())
			carFiles = append(carFiles, carFile)
			carW, err := storage.NewWritable(carFile, []cid.Cid{}, carv2.WriteAsCarV1(true), carv2.AllowDuplicatePuts(true))
			require.NoError(t, err)
			swo := r.LinkSystem.StorageWriteOpener
			r.LinkSystem.StorageWriteOpener = func(lc linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
				w, c, err := swo(lc)
				if err != nil {
					return nil, nil, err
				}
				var buf bytes.Buffer
				return &buf, func(l datamodel.Link) error {
					require.NoError(t, carW.Put(ctx, l.(cidlink.Link).Cid.KeyString(), buf.Bytes()))
					_, err := w.Write(buf.Bytes())
					if err != nil {
						return err
					}
					return c(l)
				}, nil
			}
		}(ii, r)
	}
	return carFiles
}
