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
	"github.com/filecoin-project/lassie/pkg/internal/itest/unixfs"
	"github.com/filecoin-project/lassie/pkg/lassie"
	httpserver "github.com/filecoin-project/lassie/pkg/server/http"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"

	_ "net/http/pprof"
)

// DEBUG_DATA, when true, will write source and received data to CARs
// for inspection if tests fail; otherwise they are cleaned up as tests
// proceed.
const DEBUG_DATA = true

func TestHttpFetch(t *testing.T) {
	fileQuery := func(q url.Values, _ []testpeer.TestPeer) {
		q.Set("car-scope", "file")
	}
	blockQuery := func(q url.Values, _ []testpeer.TestPeer) {
		q.Set("car-scope", "block")
	}

	type queryModifier func(url.Values, []testpeer.TestPeer)
	type bodyValidator func(*testing.T, unixfs.DirEntry, []byte)

	testCases := []struct {
		name             string
		graphsyncRemotes int
		bitswapRemotes   int
		disableGraphsync bool
		expectFail       bool
		modifyHttpConfig func(httpserver.HttpServerConfig) httpserver.HttpServerConfig
		generate         func(*testing.T, io.Reader, []testpeer.TestPeer) []unixfs.DirEntry
		paths            []string
		modifyQueries    []queryModifier
		validateBodies   []bodyValidator
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
			name:             "graphsync max block limit",
			graphsyncRemotes: 1,
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
			name:           "bitswap max block limit",
			bitswapRemotes: 1,
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
			// car-scope file fetch should get the same DAG as full for a plain file
			name:             "graphsync large sharded file, car-scope file",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)}
			},
			modifyQueries: []queryModifier{fileQuery},
		},
		{
			// car-scope file fetch should get the same DAG as full for a plain file
			name:           "bitswap large sharded file, car-scope file",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, rndReader, 4<<20)}
			},
			modifyQueries: []queryModifier{fileQuery},
		},
		{
			name:             "graphsync nested large sharded file, with path, car-scope file",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{wrapUnixfsContent(t, rndReader, lsys, unixfs.GenerateFile(t, lsys, rndReader, 4<<20))}
			},
			paths:         []string{"/want2/want1/want0"},
			modifyQueries: []queryModifier{fileQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // "/want2/want1/want0" (full file)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:           "bitswap nested large sharded file, with path, car-scope file",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{wrapUnixfsContent(t, rndReader, lsys, unixfs.GenerateFile(t, lsys, rndReader, 4<<20))}
			},
			paths:         []string{"/want2/want1/want0"},
			modifyQueries: []queryModifier{fileQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // "/want2/want1/want0" (full file)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:             "graphsync large directory, car-scope file",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false)}
			},
			modifyQueries: []queryModifier{fileQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// expect a CAR of one block, to represent the root directory we asked for
				validateCarBody(t, body, srcData.Root, []cid.Cid{srcData.Root}, true)
			}},
		},
		{
			name:           "bitswap large directory, car-scope file",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false)}
			},
			modifyQueries: []queryModifier{fileQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// expect a CAR of one block, to represent the root directory we asked for
				validateCarBody(t, body, srcData.Root, []cid.Cid{srcData.Root}, true)
			}},
		},
		{
			name:             "graphsync nested large directory, with path, car-scope file",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{wrapUnixfsContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false))}
			},
			paths:         []string{"/want2/want1/want0"},
			modifyQueries: []queryModifier{fileQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // "/want2/want1/want0" (full dir)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:           "bitswap nested large directory, with path, car-scope file",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{wrapUnixfsContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false))}
			},
			paths:         []string{"/want2/want1/want0"},
			modifyQueries: []queryModifier{fileQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // "/want2/want1/want0" (full dir)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:             "graphsync nested large directory, with path, full",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{wrapUnixfsContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false))}
			},
			paths: []string{"/want2/want1/want0"},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // "/want2/want1/want0" (full dir)
				)
				// validate we got the car-scope file form
				validateCarBody(t, body, srcData.Root, wantCids, false)
				// validate that we got the full depth form under the path
				gotDir := unixfs.CarToDirEntry(t, bytes.NewReader(body), srcData.Children[1].Children[1].Children[1].Root, true)
				gotDir.Path = "want0"
				unixfs.CompareDirEntries(t, srcData.Children[1].Children[1].Children[1], gotDir)
			}},
		},
		{
			name:           "bitswap nested large directory, with path, full",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{wrapUnixfsContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, false))}
			},
			paths: []string{"/want2/want1/want0"},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // "/want2/want1/want0" (full dir)
				)
				// validate we got the car-scope file form
				validateCarBody(t, body, srcData.Root, wantCids, false)
				// validate that we got the full depth form under the path
				gotDir := unixfs.CarToDirEntry(t, bytes.NewReader(body), srcData.Children[1].Children[1].Children[1].Root, true)
				gotDir.Path = "want0"
				unixfs.CompareDirEntries(t, srcData.Children[1].Children[1].Children[1], gotDir)
			}},
		},
		{
			name:             "graphsync nested large sharded directory, car-scope file",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true)}
			},
			modifyQueries: []queryModifier{fileQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// sharded directory contains multiple blocks, so we expect a CAR with
				// exactly those blocks
				validateCarBody(t, body, srcData.Root, srcData.SelfCids, true)
			}},
		},
		{
			name:           "bitswap nested large sharded directory, car-scope file",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true)}
			},
			modifyQueries: []queryModifier{fileQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// sharded directory contains multiple blocks, so we expect a CAR with
				// exactly those blocks
				validateCarBody(t, body, srcData.Root, srcData.SelfCids, true)
			}},
		},
		{
			name:             "graphsync nested large sharded directory, with path, car-scope file",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{wrapUnixfsContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true))}
			},
			paths:         []string{"/want2/want1/want0"},
			modifyQueries: []queryModifier{fileQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // "/want2/want1/want0" (full dir)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:           "bitswap nested large sharded directory, with path, car-scope file",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{wrapUnixfsContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true))}
			},
			paths:         []string{"/want2/want1/want0"},
			modifyQueries: []queryModifier{fileQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // "/want2/want1/want0" (full dir)
				)
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:             "graphsync nested large sharded directory, with path, full",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{wrapUnixfsContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true))}
			},
			paths: []string{"/want2/want1/want0"},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // "/want2/want1/want0" (full dir)
				)
				// validate we got the car-scope file form
				validateCarBody(t, body, srcData.Root, wantCids, false)
				// validate that we got the full depth form under the path
				gotDir := unixfs.CarToDirEntry(t, bytes.NewReader(body), srcData.Children[1].Children[1].Children[1].Root, true)
				gotDir.Path = "want0"
				unixfs.CompareDirEntries(t, srcData.Children[1].Children[1].Children[1], gotDir)
			}},
		},
		{
			name:           "bitswap nested large sharded directory, with path, full",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{wrapUnixfsContent(t, rndReader, lsys, unixfs.GenerateDirectory(t, remotes[0].LinkSystem, rndReader, 16<<20, true))}
			},
			paths: []string{"/want2/want1/want0"},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // "/want2/want1/want0" (full dir)
				)
				// validate we got the car-scope file form
				validateCarBody(t, body, srcData.Root, wantCids, false)
				// validate that we got the full depth form under the path
				gotDir := unixfs.CarToDirEntry(t, bytes.NewReader(body), srcData.Children[1].Children[1].Children[1].Root, true)
				gotDir.Path = "want0"
				unixfs.CompareDirEntries(t, srcData.Children[1].Children[1].Children[1], gotDir)
			}},
		},
		{
			// A very contrived example - we spread the content generated for this test across 4 peers,
			// then we also make sure the root is in all of them, so the CandidateFinder will return them
			// all. The retriever should then form a swarm of 4 peers and fetch the content from across
			// the set.
			name:           "bitswap, nested large sharded directory, spread across multiple peers, with path, car-scope file",
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
				data := wrapUnixfsContent(t, rndReader, &lsys, unixfs.GenerateDirectory(t, &lsys, rndReader, 16<<20, true))

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
			paths:         []string{"/want2/want1/want0"},
			modifyQueries: []queryModifier{fileQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want2"
					srcData.Children[1].Children[1].Root, // "/want2/want1"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // "/want2/want1/want0" (full dir)
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
			// car-scope block fetch should only get the the root node for a plain file
			name:             "graphsync large sharded file, car-scope block",
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
			name:             "graphsync nested large sharded file, with path, car-scope block",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{wrapUnixfsContent(t, rndReader, lsys, unixfs.GenerateFile(t, lsys, rndReader, 4<<20))}
			},
			paths:         []string{"/want2/want1/want0"},
			modifyQueries: []queryModifier{blockQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := []cid.Cid{
					srcData.Root,                                     // "/""
					srcData.Children[1].Root,                         // "/want2"
					srcData.Children[1].Children[1].Root,             // "/want2/want1"
					srcData.Children[1].Children[1].Children[1].Root, // "/want2/want1/want0"
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
			require.NoError(t, mrn.MN.LinkAll())

			carFiles := debugRemotes(t, ctx, testCase.name, mrn.Remotes)
			srcData := testCase.generate(t, rndReader, mrn.Remotes)

			// Setup a new lassie
			req := require.New(t)
			opts := []lassie.LassieOption{lassie.WithProviderTimeout(20 * time.Second),
				lassie.WithHost(mrn.Self),
				lassie.WithFinder(mrn.Finder),
			}
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
					path := ""
					if testCase.paths != nil && testCase.paths[i] != "" {
						path = testCase.paths[i]
					}
					addr := fmt.Sprintf("http://%s/ipfs/%s%s", httpServer.Addr(), srcData[i].Root.String(), path)
					getReq, err := http.NewRequest("GET", addr, nil)
					req.NoError(err)
					getReq.Header.Add("Accept", "application/vnd.ipld.car")
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
					req.Equal(http.StatusOK, resp.StatusCode)
					body, err := io.ReadAll(resp.Body)
					req.NoError(err)
					err = resp.Body.Close()
					req.NoError(err)

					if DEBUG_DATA {
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
						// gotDir := unixfs.CarToDirEntry(t, bytes.NewReader(body), srcData[i].Root, true)
						gotLsys := unixfs.CarBytesLinkSystem(t, bytes.NewReader(body))
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

// embeds the content we want in some random nested content such that it's
// fetchable under the path "/want2/want1/want0"
func wrapUnixfsContent(t *testing.T, rndReader io.Reader, lsys *ipld.LinkSystem, content unixfs.DirEntry) unixfs.DirEntry {
	before := unixfs.GenerateDirectory(t, lsys, rndReader, 4<<10, false)
	before.Path = "!before"
	// target content goes here
	want := content
	want.Path = "want0"
	after := unixfs.GenerateFile(t, lsys, rndReader, 4<<11)
	after.Path = "~after"
	want = unixfs.BuildDirectory(t, lsys, []unixfs.DirEntry{before, want, after}, false)

	before = unixfs.GenerateFile(t, lsys, rndReader, 4<<10)
	before.Path = "!before"
	want.Path = "want1"
	after = unixfs.GenerateDirectory(t, lsys, rndReader, 4<<11, true)
	after.Path = "~after"
	want = unixfs.BuildDirectory(t, lsys, []unixfs.DirEntry{before, want, after}, false)

	before = unixfs.GenerateFile(t, lsys, rndReader, 4<<10)
	before.Path = "!before"
	want.Path = "want2"
	after = unixfs.GenerateFile(t, lsys, rndReader, 4<<11)
	after.Path = "~after"
	want = unixfs.BuildDirectory(t, lsys, []unixfs.DirEntry{before, want, after}, false)

	return want
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
