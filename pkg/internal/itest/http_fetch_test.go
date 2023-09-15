//go:build !race

package itest

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/filecoin-project/lassie/pkg/aggregateeventrecorder"
	"github.com/filecoin-project/lassie/pkg/internal/itest/mocknet"
	"github.com/filecoin-project/lassie/pkg/internal/itest/testpeer"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/retriever"
	httpserver "github.com/filecoin-project/lassie/pkg/server/http"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	unixfs "github.com/ipfs/go-unixfsnode/testutil"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	trustlesshttp "github.com/ipld/go-trustless-utils/http"
	trustlesstestutil "github.com/ipld/go-trustless-utils/testutil"
	"github.com/ipld/go-trustless-utils/traversal"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"net/http/httptest"
	_ "net/http/pprof"
)

// DEBUG_DATA, when true, will write source and received data to CARs
// for inspection if tests fail; otherwise they are cleaned up as tests
// proceed.
const DEBUG_DATA = false
const wrapPath = "/want2/want1/want0"

type generateFn func(*testing.T, io.Reader, []testpeer.TestPeer) []unixfs.DirEntry
type bodyValidator func(*testing.T, unixfs.DirEntry, []byte)
type response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

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
	type lassieOptsGen func(*testing.T, *mocknet.MockRetrievalNet) []lassie.LassieOption

	testCases := []struct {
		name                  string
		graphsyncRemotes      int
		bitswapRemotes        int
		httpRemotes           int
		disableGraphsync      bool
		expectFail            bool
		expectUncleanEnd      bool
		expectUnauthorized    bool
		expectAggregateEvents []aggregateeventrecorder.AggregateEvent
		modifyHttpConfig      func(httpserver.HttpServerConfig) httpserver.HttpServerConfig
		generate              generateFn
		paths                 []string
		setHeader             headerSetter
		modifyQueries         []queryModifier
		validateBodies        []bodyValidator
		lassieOpts            lassieOptsGen
		expectNoDups          bool
	}{
		{
			name:             "graphsync large sharded file",
			graphsyncRemotes: 1,
			generate:         generateLargeShardedFile(),
			expectAggregateEvents: []aggregateeventrecorder.AggregateEvent{{
				Success:            true,
				URLPath:            "?dag-scope=all&dups=y",
				ProtocolsAllowed:   []string{multicodec.TransportGraphsyncFilecoinv1.String(), multicodec.TransportBitswap.String(), multicodec.TransportIpfsGatewayHttp.String()},
				ProtocolsAttempted: []string{multicodec.TransportGraphsyncFilecoinv1.String()},
			}},
		},
		{
			name:           "bitswap large sharded file",
			bitswapRemotes: 1,
			generate:       generateLargeShardedFile(),
			expectAggregateEvents: []aggregateeventrecorder.AggregateEvent{{
				Success:            true,
				URLPath:            "?dag-scope=all&dups=y",
				ProtocolsAllowed:   []string{multicodec.TransportGraphsyncFilecoinv1.String(), multicodec.TransportBitswap.String(), multicodec.TransportIpfsGatewayHttp.String()},
				ProtocolsAttempted: []string{multicodec.TransportBitswap.String()},
			}},
		},
		{
			name:        "http large sharded file",
			httpRemotes: 1,
			generate:    generateLargeShardedFile(),
			expectAggregateEvents: []aggregateeventrecorder.AggregateEvent{{
				Success:            true,
				URLPath:            "?dag-scope=all&dups=y",
				ProtocolsAllowed:   []string{multicodec.TransportGraphsyncFilecoinv1.String(), multicodec.TransportBitswap.String(), multicodec.TransportIpfsGatewayHttp.String()},
				ProtocolsAttempted: []string{multicodec.TransportIpfsGatewayHttp.String()},
			}},
		},
		{
			name:             "graphsync large directory",
			graphsyncRemotes: 1,
			generate:         generateLargeDirectory(),
		},
		{
			name:           "bitswap large directory",
			bitswapRemotes: 1,
			generate:       generateLargeDirectory(),
		},
		{
			name:        "http large directory",
			httpRemotes: 1,
			generate:    generateLargeDirectory(),
		},
		{
			name:             "graphsync large sharded directory",
			graphsyncRemotes: 1,
			generate:         generateLargeShardedDirectory(),
		},
		{
			name:           "bitswap large sharded directory",
			bitswapRemotes: 1,
			generate:       generateLargeShardedDirectory(),
		},
		{
			name:        "http large sharded directory",
			httpRemotes: 1,
			generate:    generateLargeShardedDirectory(),
		},
		{
			name:             "graphsync max block limit",
			graphsyncRemotes: 1,
			expectUncleanEnd: true,
			modifyHttpConfig: func(cfg httpserver.HttpServerConfig) httpserver.HttpServerConfig {
				cfg.MaxBlocksPerRequest = 3
				return cfg
			},
			generate:       generateLargeShardedFile(),
			validateBodies: validateFirstThreeBlocksOnly,
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
			generate:       generateLargeShardedFile(),
			validateBodies: validateFirstThreeBlocksOnly,
		},
		{
			name:             "bitswap max block limit",
			bitswapRemotes:   1,
			expectUncleanEnd: true,
			modifyHttpConfig: func(cfg httpserver.HttpServerConfig) httpserver.HttpServerConfig {
				cfg.MaxBlocksPerRequest = 3
				return cfg
			},
			generate:       generateLargeShardedFile(),
			validateBodies: validateFirstThreeBlocksOnly,
		},
		{
			name:             "http max block limit",
			httpRemotes:      1,
			expectUncleanEnd: true,
			modifyHttpConfig: func(cfg httpserver.HttpServerConfig) httpserver.HttpServerConfig {
				cfg.MaxBlocksPerRequest = 3
				return cfg
			},
			generate:       generateLargeShardedFile(),
			validateBodies: validateFirstThreeBlocksOnly,
		},
		{
			name:             "bitswap block timeout from missing block",
			bitswapRemotes:   1,
			expectUncleanEnd: true,
			lassieOpts: func(t *testing.T, mrn *mocknet.MockRetrievalNet) []lassie.LassieOption {
				// this delay is going to depend on CI, if it's too short then a slower machine
				// won't get bitswap setup in time to get the block
				return []lassie.LassieOption{lassie.WithProviderTimeout(1 * time.Second)}
			},
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				file := largeShardedFile(t, rndReader, remotes[0])
				remotes[0].Blockstore().DeleteBlock(context.Background(), file.SelfCids[2])
				return []unixfs.DirEntry{file}
			},
			validateBodies: validateFirstThreeBlocksOnly,
		},
		{
			name:           "same content, http missing block, bitswap completes",
			bitswapRemotes: 1,
			httpRemotes:    1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				file := largeShardedFile(t, rndReader, remotes[0])
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
			generate:         generateLargeShardedFile(),
			modifyQueries:    []queryModifier{entityQuery},
		},
		{
			// dag-scope entity fetch should get the same DAG as full for a plain file
			name:           "bitswap large sharded file, dag-scope entity",
			bitswapRemotes: 1,
			generate:       generateLargeShardedFile(),
			modifyQueries:  []queryModifier{entityQuery},
		},
		{
			name:             "graphsync nested large sharded file, with path, dag-scope entity",
			graphsyncRemotes: 1,
			generate:         generateLargeShardedFileWrapped(),
			paths:            []string{wrapPath},
			modifyQueries:    []queryModifier{entityQuery},
			validateBodies:   validatePathedEntityContent,
		},
		{
			name:           "bitswap nested large sharded file, with path, dag-scope entity",
			bitswapRemotes: 1,
			generate:       generateLargeShardedFileWrapped(),
			paths:          []string{wrapPath},
			modifyQueries:  []queryModifier{entityQuery},
			validateBodies: validatePathedEntityContent,
		},
		{
			name:           "http nested large sharded file, with path, dag-scope entity",
			httpRemotes:    1,
			generate:       generateLargeShardedFileWrapped(),
			paths:          []string{wrapPath},
			modifyQueries:  []queryModifier{entityQuery},
			validateBodies: validatePathedEntityContent,
		},
		{
			name:             "graphsync large directory, dag-scope entity",
			graphsyncRemotes: 1,
			generate:         generateLargeDirectory(),
			modifyQueries:    []queryModifier{entityQuery},
			validateBodies:   validateOnlyRoot,
		},
		{
			name:           "bitswap large directory, dag-scope entity",
			bitswapRemotes: 1,
			generate:       generateLargeDirectory(),
			modifyQueries:  []queryModifier{entityQuery},
			validateBodies: validateOnlyRoot,
		},
		{
			name:           "http large directory, dag-scope entity",
			httpRemotes:    1,
			generate:       generateLargeDirectory(),
			modifyQueries:  []queryModifier{entityQuery},
			validateBodies: validateOnlyRoot,
		},
		{
			name:             "graphsync nested large directory, with path, dag-scope entity",
			graphsyncRemotes: 1,
			generate:         generateLargeDirectoryWrapped(),
			paths:            []string{wrapPath},
			modifyQueries:    []queryModifier{entityQuery},
			validateBodies:   validatePathedEntityContent,
		},
		{
			name:           "bitswap nested large directory, with path, dag-scope entity",
			bitswapRemotes: 1,
			generate:       generateLargeDirectoryWrapped(),
			paths:          []string{wrapPath},
			modifyQueries:  []queryModifier{entityQuery},
			validateBodies: validatePathedEntityContent,
		},
		{
			name:           "http nested large directory, with path, dag-scope entity",
			httpRemotes:    1,
			generate:       generateLargeDirectoryWrapped(),
			paths:          []string{wrapPath},
			modifyQueries:  []queryModifier{entityQuery},
			validateBodies: validatePathedEntityContent,
		},
		{
			name:             "graphsync nested large directory, with path, full",
			graphsyncRemotes: 1,
			generate:         generateLargeDirectoryWrapped(),
			paths:            []string{wrapPath},
			validateBodies:   validatePathedFullContent,
		},
		{
			name:           "bitswap nested large directory, with path, full",
			bitswapRemotes: 1,
			generate:       generateLargeDirectoryWrapped(),
			paths:          []string{wrapPath},
			validateBodies: validatePathedFullContent,
		},
		{
			name:           "bitswap nested large directory, with path, full",
			httpRemotes:    1,
			generate:       generateLargeDirectoryWrapped(),
			paths:          []string{wrapPath},
			validateBodies: validatePathedFullContent,
		},
		{
			name:             "graphsync nested large sharded directory, dag-scope entity",
			graphsyncRemotes: 1,
			generate:         generateLargeShardedDirectory(),
			modifyQueries:    []queryModifier{entityQuery},
			validateBodies:   validateOnlyEntity,
		},
		{
			name:           "bitswap nested large sharded directory, dag-scope entity",
			bitswapRemotes: 1,
			generate:       generateLargeShardedDirectory(),
			modifyQueries:  []queryModifier{entityQuery},
			validateBodies: validateOnlyEntity,
		},
		{
			name:           "http nested large sharded directory, dag-scope entity",
			httpRemotes:    1,
			generate:       generateLargeShardedDirectory(),
			modifyQueries:  []queryModifier{entityQuery},
			validateBodies: validateOnlyEntity,
		},
		{
			name:             "graphsync nested large sharded directory, with path, dag-scope entity",
			graphsyncRemotes: 1,
			generate:         generateLargeShardedDirectoryWrapped(),
			paths:            []string{wrapPath},
			modifyQueries:    []queryModifier{entityQuery},
			validateBodies:   validatePathedEntityContent,
		},
		{
			name:           "bitswap nested large sharded directory, with path, dag-scope entity",
			bitswapRemotes: 1,
			generate:       generateLargeShardedDirectoryWrapped(),
			paths:          []string{wrapPath},
			modifyQueries:  []queryModifier{entityQuery},
			validateBodies: validatePathedEntityContent,
		},
		{
			name:           "http nested large sharded directory, with path, dag-scope entity",
			httpRemotes:    1,
			generate:       generateLargeShardedDirectoryWrapped(),
			paths:          []string{wrapPath},
			modifyQueries:  []queryModifier{entityQuery},
			validateBodies: validatePathedEntityContent,
		},
		{
			name:             "graphsync nested large sharded directory, with path, full",
			graphsyncRemotes: 1,
			generate:         generateLargeShardedDirectoryWrapped(),
			paths:            []string{wrapPath},
			validateBodies:   validatePathedFullContent,
		},
		{
			name:           "bitswap nested large sharded directory, with path, full",
			bitswapRemotes: 1,
			generate:       generateLargeShardedDirectoryWrapped(),
			paths:          []string{wrapPath},
			validateBodies: validatePathedFullContent,
		},
		{
			name:           "http nested large sharded directory, with path, full",
			httpRemotes:    1,
			generate:       generateLargeShardedDirectoryWrapped(),
			paths:          []string{wrapPath},
			validateBodies: validatePathedFullContent,
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
			paths:          []string{wrapPath},
			modifyQueries:  []queryModifier{entityQuery},
			validateBodies: validatePathedEntityContent,
		},
		{
			name:           "two separate, parallel bitswap retrievals",
			bitswapRemotes: 2,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{
					largeShardedFile(t, rndReader, remotes[0]),
					largeDirectory(t, rndReader, remotes[1]),
				}
			},
		},
		{
			name:             "two separate, parallel graphsync retrievals",
			graphsyncRemotes: 2,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{
					largeShardedFile(t, rndReader, remotes[0]),
					largeDirectory(t, rndReader, remotes[1]),
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
					largeShardedFile(t, rndReader, remotes[0]),
					largeDirectory(t, rndReader, remotes[1]),
				}
			},
			expectAggregateEvents: []aggregateeventrecorder.AggregateEvent{
				{
					Success:            false,
					URLPath:            "?dag-scope=all&dups=y",
					ProtocolsAllowed:   []string{multicodec.TransportIpfsGatewayHttp.String(), multicodec.TransportBitswap.String()},
					ProtocolsAttempted: []string{},
				},
				{
					Success:            false,
					URLPath:            "?dag-scope=all&dups=y",
					ProtocolsAllowed:   []string{multicodec.TransportIpfsGatewayHttp.String(), multicodec.TransportBitswap.String()},
					ProtocolsAttempted: []string{},
				},
			},
		},
		{
			name:             "parallel, separate graphsync and bitswap retrievals",
			graphsyncRemotes: 1,
			bitswapRemotes:   1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{
					largeShardedFile(t, rndReader, remotes[0]),
					largeDirectory(t, rndReader, remotes[1]),
				}
			},
		},
		{
			// dag-scope block fetch should only get the the root node for a plain file
			name:             "graphsync large sharded file, dag-scope block",
			graphsyncRemotes: 1,
			generate:         generateLargeShardedFile(),
			modifyQueries:    []queryModifier{blockQuery},
			validateBodies:   validateOnlyRoot,
		},
		{
			name:             "graphsync nested large sharded file, with path, dag-scope block",
			graphsyncRemotes: 1,
			generate:         generateLargeShardedFileWrapped(),
			paths:            []string{wrapPath},
			modifyQueries:    []queryModifier{blockQuery},
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
				fileEntry := largeShardedFile(t, rndReader, remotes[0])
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
				fileEntry := largeShardedFile(t, rndReader, remotes[0])
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
				fileEntry := largeShardedFile(t, rndReader, remotes[0])
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
				fileEntry := largeShardedFile(t, rndReader, remotes[0])
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
				return []unixfs.DirEntry{unixfs.GenerateFile(t, remotes[0].LinkSystem, trustlesstestutil.ZeroReader{}, 4<<20)}
			},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				store := &trustlesstestutil.CorrectedMemStore{ParentStore: &memstore.Store{
					Bag: make(map[string][]byte),
				}}
				lsys := cidlink.DefaultLinkSystem()
				lsys.SetReadStorage(store)
				lsys.SetWriteStorage(store)
				_, err := traversal.Config{
					Root:               srcData.Root,
					Selector:           selectorparse.CommonSelector_ExploreAllRecursively,
					ExpectDuplicatesIn: true,
				}.VerifyCar(context.Background(), bytes.NewReader(body), lsys)
				require.NoError(t, err)
			}},
		},
		{
			name:         "http large sharded file with dups, no dups response requested",
			httpRemotes:  1,
			setHeader:    noDups,
			expectNoDups: true,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{largeShardedFile(t, trustlesstestutil.ZeroReader{}, remotes[0])}
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
		{
			name:        "http large sharded file with dups, */* gives dups",
			httpRemotes: 1,
			setHeader:   func(h http.Header) { h.Set("Accept", "*/*") },
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{largeShardedFile(t, trustlesstestutil.ZeroReader{}, remotes[0])}
			},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				store := &trustlesstestutil.CorrectedMemStore{ParentStore: &memstore.Store{
					Bag: make(map[string][]byte),
				}}
				lsys := cidlink.DefaultLinkSystem()
				lsys.SetReadStorage(store)
				lsys.SetWriteStorage(store)
				_, err := traversal.Config{
					Root:               srcData.Root,
					Selector:           selectorparse.CommonSelector_ExploreAllRecursively,
					ExpectDuplicatesIn: true,
				}.VerifyCar(context.Background(), bytes.NewReader(body), lsys)
				require.NoError(t, err)
			}},
		}, {
			name:         "http large sharded file with dups, multiple accept, priority to no dups",
			httpRemotes:  1,
			expectNoDups: true,
			setHeader: func(h http.Header) {
				h.Set("Accept",
					strings.Join([]string{
						"text/html",
						trustlesshttp.DefaultContentType().WithDuplicates(true).WithQuality(0.7).String(),
						trustlesshttp.DefaultContentType().WithDuplicates(false).WithQuality(0.8).String(),
						"*/*;q=0.1",
					}, ", "),
				)
			},
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{largeShardedFile(t, trustlesstestutil.ZeroReader{}, remotes[0])}
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
		{
			name:        "http large sharded file with dups, multiple accept, priority to dups",
			httpRemotes: 1,
			setHeader: func(h http.Header) {
				h.Set("Accept",
					strings.Join([]string{
						"text/html",
						trustlesshttp.DefaultContentType().WithDuplicates(true).WithQuality(0.8).String(),
						trustlesshttp.DefaultContentType().WithDuplicates(false).WithQuality(0.7).String(),
						"*/*;q=0.1",
					}, ", "),
				)
			},
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{largeShardedFile(t, trustlesstestutil.ZeroReader{}, remotes[0])}
			},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				store := &trustlesstestutil.CorrectedMemStore{ParentStore: &memstore.Store{
					Bag: make(map[string][]byte),
				}}
				lsys := cidlink.DefaultLinkSystem()
				lsys.SetReadStorage(store)
				lsys.SetWriteStorage(store)
				_, err := traversal.Config{
					Root:               srcData.Root,
					Selector:           selectorparse.CommonSelector_ExploreAllRecursively,
					ExpectDuplicatesIn: true,
				}.VerifyCar(context.Background(), bytes.NewReader(body), lsys)
				require.NoError(t, err)
			}},
		},
		{
			name:           "bitswap nested file, path with special characters",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateFile(t, lsys, rndReader, 1024), "/?/#/%/ ", false)}
			},
			paths:         []string{"/?/#/%/ "},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := []cid.Cid{
					srcData.Root,                                                 // "/"
					srcData.Children[1].Root,                                     // "/?"
					srcData.Children[1].Children[1].Root,                         // "/?/#"
					srcData.Children[1].Children[1].Children[1].Root,             // "/?/#/%"
					srcData.Children[1].Children[1].Children[1].Children[0].Root, // "/?/#/%/ " (' ' is before '!', so it's the first link after the one named '!before')
				}
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:        "http nested file, path with special characters",
			httpRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateFile(t, lsys, rndReader, 1024), "/?/#/%/ ", false)}
			},
			paths:         []string{"/?/#/%/ "},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := []cid.Cid{
					srcData.Root,                                                 // "/"
					srcData.Children[1].Root,                                     // "/?"
					srcData.Children[1].Children[1].Root,                         // "/?/#"
					srcData.Children[1].Children[1].Children[1].Root,             // "/?/#/%"
					srcData.Children[1].Children[1].Children[1].Children[0].Root, // "/?/#/%/ " (' ' is before '!', so it's the first link after the one named '!before')
				}
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:             "graphsync nested file, path with special characters",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				lsys := remotes[0].LinkSystem
				return []unixfs.DirEntry{unixfs.WrapContent(t, rndReader, lsys, unixfs.GenerateFile(t, lsys, rndReader, 1024), "/?/#/%/ ", false)}
			},
			paths:         []string{"/?/#/%/ "},
			modifyQueries: []queryModifier{entityQuery},
			validateBodies: []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := []cid.Cid{
					srcData.Root,                                                 // "/"
					srcData.Children[1].Root,                                     // "/?"
					srcData.Children[1].Children[1].Root,                         // "/?/#"
					srcData.Children[1].Children[1].Children[1].Root,             // "/?/#/%"
					srcData.Children[1].Children[1].Children[1].Children[0].Root, // "/?/#/%/ " (' ' is before '!', so it's the first link after the one named '!before')
				}
				validateCarBody(t, body, srcData.Root, wantCids, true)
			}},
		},
		{
			name:        "with access token - rejects anonymous requests",
			httpRemotes: 1,
			generate:    generateSmallFile(),
			modifyHttpConfig: func(cfg httpserver.HttpServerConfig) httpserver.HttpServerConfig {
				cfg.AccessToken = "super-secret"
				return cfg
			},
			expectUnauthorized: true,
		},
		{
			name:        "with access token - allows requests with authorization header",
			httpRemotes: 1,
			generate:    generateSmallFile(),
			modifyHttpConfig: func(cfg httpserver.HttpServerConfig) httpserver.HttpServerConfig {
				cfg.AccessToken = "super-secret"
				return cfg
			},
			setHeader: func(header http.Header) {
				header.Set("Authorization", "Bearer super-secret")
				header.Add("Accept", "application/vnd.ipld.car")
			},
			expectUnauthorized: false,
		},
		{
			name:             "non-unixfs graphsync",
			graphsyncRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{trustlesstestutil.MakeDagWithIdentity(t, *remotes[0].LinkSystem)}
			},
		},
		{
			name:           "non-unixfs bitswap",
			bitswapRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{trustlesstestutil.MakeDagWithIdentity(t, *remotes[0].LinkSystem)}
			},
		},
		{
			name:        "non-unixfs http",
			httpRemotes: 1,
			generate: func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
				return []unixfs.DirEntry{trustlesstestutil.MakeDagWithIdentity(t, *remotes[0].LinkSystem)}
			},
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
			opts := append([]lassie.LassieOption{
				lassie.WithProviderTimeout(20 * time.Second),
				lassie.WithHost(mrn.Self),
				lassie.WithFinder(mrn.Finder),
			}, customOpts...)
			if testCase.disableGraphsync {
				opts = append(opts, lassie.WithProtocols([]multicodec.Code{multicodec.TransportBitswap, multicodec.TransportIpfsGatewayHttp}))
			}
			lassie, err := lassie.NewLassie(ctx, opts...)
			req.NoError(err)

			var aggregateEventsCh = make(chan []aggregateeventrecorder.AggregateEvent)
			if len(testCase.expectAggregateEvents) > 0 {
				closer := setupAggregateEventRecorder(t, ctx, len(srcData), lassie, aggregateEventsCh)
				defer closer.Close()
			}

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

			paths := make([]string, len(srcData))
			for i := 0; i < len(srcData); i++ {
				if testCase.paths != nil && testCase.paths[i] != "" {
					p := datamodel.ParsePath(testCase.paths[i])
					for p.Len() > 0 {
						var ps datamodel.PathSegment
						ps, p = p.Shift()
						paths[i] += "/" + url.PathEscape(ps.String())
					}
				}
			}

			responseChans := make([]chan response, 0)
			for i := 0; i < len(srcData); i++ {
				responseChan := make(chan response, 1)
				responseChans = append(responseChans, responseChan)
				go func(i int) {
					// Make a request for our CID and read the complete CAR bytes
					addr := fmt.Sprintf("http://%s/ipfs/%s%s", httpServer.Addr(), srcData[i].Root.String(), paths[i])
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
					expectBodyReadError := ""
					if testCase.expectUncleanEnd {
						expectBodyReadError = "http: unexpected EOF reading trailer"
					}
					body := readAllBody(t, resp.Body, expectBodyReadError)
					req.NoError(resp.Body.Close())
					responseChan <- response{StatusCode: resp.StatusCode, Header: resp.Header, Body: body}
				}(i)
			}

			responses := make([]response, 0)
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
				} else if testCase.expectUnauthorized {
					req.Equal(http.StatusUnauthorized, resp.StatusCode)
				} else {
					if resp.StatusCode != http.StatusOK {
						req.Failf("200 response code not received", "got code: %d, body: %s", resp.StatusCode, string(resp.Body))
					}

					verifyHeaders(t, resp, srcData[i].Root, paths[i], testCase.expectNoDups)

					if DEBUG_DATA {
						t.Logf("Creating CAR %s in temp dir", fmt.Sprintf("%s_received%d.car", testCase.name, i))
						dstf, err := os.CreateTemp("", fmt.Sprintf("%s_received%d.car", testCase.name, i))
						req.NoError(err)
						t.Logf("Writing received data to CAR @ %s", dstf.Name())
						_, err = dstf.Write(resp.Body)
						req.NoError(err)
						carFiles = append(carFiles, dstf)
					}

					if testCase.validateBodies != nil && testCase.validateBodies[i] != nil {
						testCase.validateBodies[i](t, srcData[i], resp.Body)
					} else {
						gotLsys := CarBytesLinkSystem(t, bytes.NewReader(resp.Body))
						gotDir := unixfs.ToDirEntry(t, gotLsys, srcData[i].Root, true)
						unixfs.CompareDirEntries(t, srcData[i], gotDir)
					}
				}
			}

			if len(testCase.expectAggregateEvents) > 0 {
				var events []aggregateeventrecorder.AggregateEvent
				// check that the event recorder got and event for this by looking for the root cid
				select {
				case events = <-aggregateEventsCh:
				case <-ctx.Done():
					req.FailNow("Did not receive aggregate events")
				}
				verifyAggregateEvents(t, mrn.Remotes, srcData, testCase.expectAggregateEvents, events)
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
	br, err := car.NewBlockReader(bytes.NewReader(body))
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

func verifyHeaders(t *testing.T, resp response, root cid.Cid, path string, expectNoDups bool) {
	req := require.New(t)

	req.Regexp(`^lassie/v\d+\.\d+\.\d+-\w+$`, resp.Header.Get("Server"))
	req.Equal(fmt.Sprintf(`attachment; filename="%s.car"`, root.String()), resp.Header.Get("Content-Disposition"))
	req.Equal("none", resp.Header.Get("Accept-Ranges"))
	req.Equal("public, max-age=29030400, immutable", resp.Header.Get("Cache-Control"))
	req.Equal(trustlesshttp.DefaultContentType().WithDuplicates(!expectNoDups).String(), resp.Header.Get("Content-Type"))
	req.Equal("nosniff", resp.Header.Get("X-Content-Type-Options"))
	st := resp.Header.Get("Server-Timing")
	req.Contains(st, "started-finding-candidates")
	req.Contains(st, "candidates-found=")
	req.Contains(st, "retrieval-")
	req.Contains(st, "dur=") // at lest one of these
	etagStart := fmt.Sprintf(`"%s.car.`, root.String())
	etagGot := resp.Header.Get("ETag")
	req.True(strings.HasPrefix(etagGot, etagStart), "ETag should start with [%s], got [%s]", etagStart, etagGot)
	req.Equal(`"`, etagGot[len(etagGot)-1:], "ETag should end with a quote")
	req.Equal(fmt.Sprintf("/ipfs/%s%s", root.String(), path), resp.Header.Get("X-Ipfs-Path"))
	requestId := resp.Header.Get("X-Trace-Id")
	require.NotEmpty(t, requestId)
	_, err := uuid.Parse(requestId)
	req.NoError(err)
}

func setupAggregateEventRecorder(t *testing.T, ctx context.Context, expectCount int, lassie *lassie.Lassie, aggregateEventsCh chan []aggregateeventrecorder.AggregateEvent) interface{ Close() } {
	var aggregateEventsLk sync.Mutex
	events := make([]aggregateeventrecorder.AggregateEvent, 0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Basic listenup", r.Header.Get("Authorization"))
		type batch struct {
			Events []aggregateeventrecorder.AggregateEvent
		}
		var b batch
		err := json.NewDecoder(r.Body).Decode(&b)
		require.NoError(t, err)
		aggregateEventsLk.Lock()
		events = append(events, b.Events...)
		if len(events) == expectCount {
			select {
			case <-ctx.Done():
			case aggregateEventsCh <- events:
			}
		}
		aggregateEventsLk.Unlock()
	}))

	eventRecorder := aggregateeventrecorder.NewAggregateEventRecorder(ctx, aggregateeventrecorder.EventRecorderConfig{
		InstanceID:            "fooblesmush",
		EndpointURL:           ts.URL,
		EndpointAuthorization: "listenup",
	})
	lassie.RegisterSubscriber(eventRecorder.RetrievalEventSubscriber())

	return ts
}

func verifyAggregateEvents(t *testing.T, remotes []testpeer.TestPeer, srcData []unixfs.DirEntry, expectedEvents, actualEvents []aggregateeventrecorder.AggregateEvent) {
	req := require.New(t)

	for ii, src := range srcData {
		var evt aggregateeventrecorder.AggregateEvent
		for _, e := range actualEvents {
			if e.RootCid == src.Root.String() {
				evt = e
				break
			}
		}
		req.NotNil(evt)
		t.Log("got event", evt)

		expect := expectedEvents[ii]
		req.Equal("fooblesmush", evt.InstanceID)
		req.Equal(expect.Success, evt.Success)
		req.Equal(expect.URLPath, evt.URLPath)
		req.ElementsMatch(expect.ProtocolsAttempted, evt.ProtocolsAttempted)
		req.ElementsMatch(expect.ProtocolsAllowed, evt.ProtocolsAllowed)

		// This makes an assumption that there's a clear mapping of remote
		// index to srcData index, which doesn't necessarily hold. So if novel
		// cases need to be tested, this may need to be en-smartened.
		if expect.Success {
			totalBytes := totalBlockBytes(t, *remotes[ii].LinkSystem, src)
			req.Equal(totalBytes, evt.BytesTransferred)

			// This makes an assumption there's only one attempt
			isBitswap := slices.Equal(expect.ProtocolsAttempted, []string{multicodec.TransportBitswap.String()})
			if isBitswap {
				req.Len(evt.RetrievalAttempts, 2)
				req.Contains(evt.RetrievalAttempts, "Bitswap")
			} else {
				req.Len(evt.RetrievalAttempts, 1)
			}
			for _, attempt := range evt.RetrievalAttempts {
				req.Equal("", attempt.Error)
				req.Equal(totalBytes, attempt.BytesTransferred) // both attempts for a bitswap req will have the same number
			}
		}
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
			carW, err := storage.NewWritable(carFile, []cid.Cid{}, car.WriteAsCarV1(true), car.AllowDuplicatePuts(true))
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

func readAllBody(t *testing.T, r io.Reader, expectError string) []byte {
	if expectError == "" {
		body, err := io.ReadAll(r)
		require.NoError(t, err)
		return body
	}
	// expect an error, so let's creep up on it and collect as much of the body
	// as we can before the error blocks us
	// see readLocked() in src/net/http/transfer.go:
	// → b.src.Read(p)
	// → followed by b.readTrailer() which should error; we want to capture both
	var buf bytes.Buffer
	var byt [1]byte
	var err error
	var n int
	for {
		n, err = r.Read(byt[:])
		// record the bytes we read, the error should come after the normal body
		// read and then it attempts to read trailers where it should fail
		buf.Write(byt[:n])
		if err != nil {
			require.EqualError(t, err, expectError)
			break
		}
	}
	return buf.Bytes()
}

func smallFile(t *testing.T, rndReader io.Reader, remote testpeer.TestPeer) unixfs.DirEntry {
	return unixfs.GenerateFile(t, remote.LinkSystem, rndReader, 1024)
}

func generateSmallFile() generateFn {
	return func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
		return []unixfs.DirEntry{smallFile(t, rndReader, remotes[0])}
	}
}

func largeShardedFile(t *testing.T, rndReader io.Reader, remote testpeer.TestPeer) unixfs.DirEntry {
	return unixfs.GenerateFile(t, remote.LinkSystem, rndReader, 4<<20)
}

func generateLargeShardedFile() generateFn {
	return func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
		return []unixfs.DirEntry{largeShardedFile(t, rndReader, remotes[0])}
	}
}

func largeShardedFileWrapped(t *testing.T, rndReader io.Reader, remote testpeer.TestPeer) unixfs.DirEntry {
	return unixfs.WrapContent(t, rndReader, remote.LinkSystem, unixfs.GenerateFile(t, remote.LinkSystem, rndReader, 4<<20), wrapPath, false)
}

func generateLargeShardedFileWrapped() generateFn {
	return func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
		return []unixfs.DirEntry{largeShardedFileWrapped(t, rndReader, remotes[0])}
	}
}

func largeDirectory(t *testing.T, rndReader io.Reader, remote testpeer.TestPeer) unixfs.DirEntry {
	return unixfs.GenerateDirectory(t, remote.LinkSystem, rndReader, 16<<20, false)
}

func generateLargeDirectory() generateFn {
	return func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
		return []unixfs.DirEntry{largeDirectory(t, rndReader, remotes[0])}
	}
}

func largeDirectoryWrapped(t *testing.T, rndReader io.Reader, remote testpeer.TestPeer) unixfs.DirEntry {
	return unixfs.WrapContent(t, rndReader, remote.LinkSystem, unixfs.GenerateDirectory(t, remote.LinkSystem, rndReader, 16<<20, false), wrapPath, false)
}

func generateLargeDirectoryWrapped() generateFn {
	return func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
		return []unixfs.DirEntry{largeDirectoryWrapped(t, rndReader, remotes[0])}
	}
}

func largeShardedDirectory(t *testing.T, rndReader io.Reader, remote testpeer.TestPeer) unixfs.DirEntry {
	return unixfs.GenerateDirectory(t, remote.LinkSystem, rndReader, 16<<20, true)
}

func generateLargeShardedDirectory() generateFn {
	return func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
		return []unixfs.DirEntry{largeShardedDirectory(t, rndReader, remotes[0])}
	}
}

func largeShardedDirectoryWrapped(t *testing.T, rndReader io.Reader, remote testpeer.TestPeer) unixfs.DirEntry {
	return unixfs.WrapContent(t, rndReader, remote.LinkSystem, unixfs.GenerateDirectory(t, remote.LinkSystem, rndReader, 16<<20, true), wrapPath, false)
}

func generateLargeShardedDirectoryWrapped() generateFn {
	return func(t *testing.T, rndReader io.Reader, remotes []testpeer.TestPeer) []unixfs.DirEntry {
		return []unixfs.DirEntry{largeShardedDirectoryWrapped(t, rndReader, remotes[0])}
	}
}

var validateFirstThreeBlocksOnly = []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
	// 3 blocks max, start at the root and then two blocks into the sharded data
	wantCids := []cid.Cid{
		srcData.Root,
		srcData.SelfCids[0],
		srcData.SelfCids[1],
	}
	validateCarBody(t, body, srcData.Root, wantCids, true)
}}

var validateOnlyEntity = []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
	// sharded directory contains multiple blocks, so we expect a CAR with
	// exactly those blocks
	validateCarBody(t, body, srcData.Root, srcData.SelfCids, true)
}}

var validatePathedFullContent = []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
	wantCids := append([]cid.Cid{
		srcData.Root,                         // "/""
		srcData.Children[1].Root,             // "/want2"
		srcData.Children[1].Children[1].Root, // "/want2/want1"
	},
		srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full)
	)
	// validate we got the dag-scope entity form
	validateCarBody(t, body, srcData.Root, wantCids, false)
	// validate that we got the full depth form under the path
	gotDir := CarToDirEntry(t, bytes.NewReader(body), srcData.Children[1].Children[1].Children[1].Root, true)
	gotDir.Path = "want0"
	unixfs.CompareDirEntries(t, srcData.Children[1].Children[1].Children[1], gotDir)
}}

var validatePathedEntityContent = []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
	wantCids := append([]cid.Cid{
		srcData.Root,                         // "/""
		srcData.Children[1].Root,             // "/want2"
		srcData.Children[1].Children[1].Root, // "/want2/want1"
	},
		srcData.Children[1].Children[1].Children[1].SelfCids..., // wrapPath (full)
	)
	validateCarBody(t, body, srcData.Root, wantCids, true)
}}

var validateOnlyRoot = []bodyValidator{func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
	// expect a CAR of one block, to represent the root directory we asked for
	validateCarBody(t, body, srcData.Root, []cid.Cid{srcData.Root}, true)
}}

func totalBlockBytes(t *testing.T, lsys linking.LinkSystem, srcData unixfs.DirEntry) uint64 {
	var total uint64
	for _, c := range srcData.SelfCids {
		b, err := lsys.LoadRaw(ipld.LinkContext{}, cidlink.Link{Cid: c})
		require.NoError(t, err)
		total += uint64(len(b))
		for _, child := range srcData.Children {
			total += totalBlockBytes(t, lsys, child)
		}
	}
	return total
}
