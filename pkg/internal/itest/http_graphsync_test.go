package itest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lassie/pkg/internal/itest/mocknet"
	"github.com/filecoin-project/lassie/pkg/internal/itest/testpeer"
	"github.com/filecoin-project/lassie/pkg/internal/itest/unixfs"
	"github.com/filecoin-project/lassie/pkg/lassie"
	httpserver "github.com/filecoin-project/lassie/pkg/server/http"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

func TestHttpGraphsyncRetrieval(t *testing.T) {
	rndSeed := time.Now().UTC().UnixNano()
	t.Logf("random seed: %d", rndSeed)
	var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

	shallowQuery := func(q url.Values) {
		q.Set("depthType", "shallow")
	}

	testCases := []struct {
		name             string
		generate         func(*testing.T, []testpeer.TestPeer) unixfs.DirEntry
		modifyHttpConfig func(httpserver.HttpServerConfig) httpserver.HttpServerConfig
		path             string
		modifyQuery      func(url.Values)
		validateBody     func(*testing.T, unixfs.DirEntry, []byte)
	}{
		{
			name: "UnixFSFileDAG",
			generate: func(t *testing.T, remotes []testpeer.TestPeer) unixfs.DirEntry {
				return unixfs.GenerateFile(t, &remotes[0].LinkSystem, rndReader, 4<<20)
			},
		},
		{
			name: "UnixFSDirectoryDAG",
			generate: func(t *testing.T, remotes []testpeer.TestPeer) unixfs.DirEntry {
				return unixfs.GenerateDirectory(t, &remotes[0].LinkSystem, rndReader, 16<<20, false)
			},
		},
		{
			name: "UnixFSShardedDirectoryDAG",
			generate: func(t *testing.T, remotes []testpeer.TestPeer) unixfs.DirEntry {
				return unixfs.GenerateDirectory(t, &remotes[0].LinkSystem, rndReader, 16<<20, true)
			},
		},
		{
			name: "max block limit",
			modifyHttpConfig: func(cfg httpserver.HttpServerConfig) httpserver.HttpServerConfig {
				cfg.MaxBlocksPerRequest = 3
				return cfg
			},
			generate: func(t *testing.T, remotes []testpeer.TestPeer) unixfs.DirEntry {
				return unixfs.GenerateFile(t, &remotes[0].LinkSystem, rndReader, 4<<20)
			},
			validateBody: func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// 3 blocks max, start at the root and then two blocks into the sharded data
				wantCids := []cid.Cid{
					srcData.Root,
					srcData.SelfCids[0],
					srcData.SelfCids[1],
				}
				validateCarBody(t, body, srcData.Root, wantCids)
			},
		},
		{
			// shallow fetch should get the same DAG as full for a plain file
			name: "UnixFSFileDAG, shallow",
			generate: func(t *testing.T, remotes []testpeer.TestPeer) unixfs.DirEntry {
				return unixfs.GenerateFile(t, &remotes[0].LinkSystem, rndReader, 4<<20)
			},
			modifyQuery: shallowQuery,
		},
		{
			name: "nested UnixFSFileDAG, with path, shallow",
			generate: func(t *testing.T, remotes []testpeer.TestPeer) unixfs.DirEntry {
				lsys := &remotes[0].LinkSystem

				before := unixfs.GenerateDirectory(t, lsys, rndReader, 4<<10, false)
				before.Path = "!before"
				// target file
				want := unixfs.GenerateFile(t, lsys, rndReader, 4<<20)
				want.Path = "want"
				after := unixfs.GenerateFile(t, lsys, rndReader, 4<<11)
				after.Path = "~after"
				want = unixfs.BuildDirectory(t, lsys, []unixfs.DirEntry{before, want, after}, false)

				before = unixfs.GenerateFile(t, lsys, rndReader, 4<<10)
				before.Path = "!before"
				want.Path = "want"
				after = unixfs.GenerateDirectory(t, lsys, rndReader, 4<<11, true)
				after.Path = "~after"
				want = unixfs.BuildDirectory(t, lsys, []unixfs.DirEntry{before, want, after}, false)

				before = unixfs.GenerateFile(t, lsys, rndReader, 4<<10)
				before.Path = "!before"
				want.Path = "want"
				after = unixfs.GenerateFile(t, lsys, rndReader, 4<<11)
				after.Path = "~after"
				want = unixfs.BuildDirectory(t, lsys, []unixfs.DirEntry{before, want, after}, false)

				return want
			},
			path:        "/want/want/want",
			modifyQuery: shallowQuery,
			validateBody: func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want"
					srcData.Children[1].Children[1].Root, // "/want/want"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // "/want/want/want" (full file)
				)
				validateCarBody(t, body, srcData.Root, wantCids)
			},
		},
		{
			name: "UnixFSDirectoryDAG, shallow",
			generate: func(t *testing.T, remotes []testpeer.TestPeer) unixfs.DirEntry {
				return unixfs.GenerateDirectory(t, &remotes[0].LinkSystem, rndReader, 16<<20, false)
			},
			modifyQuery: shallowQuery,
			validateBody: func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// expect a CAR of one block, to represent the root directory we asked for
				validateCarBody(t, body, srcData.Root, []cid.Cid{srcData.Root})
			},
		},
		{
			name: "nested UnixFSDirectoryDAG, with path, shallow",
			generate: func(t *testing.T, remotes []testpeer.TestPeer) unixfs.DirEntry {
				lsys := &remotes[0].LinkSystem

				before := unixfs.GenerateDirectory(t, lsys, rndReader, 4<<10, false)
				before.Path = "!before"
				// target dir, not sharded
				want := unixfs.GenerateDirectory(t, &remotes[0].LinkSystem, rndReader, 16<<20, false)
				want.Path = "want"
				after := unixfs.GenerateFile(t, lsys, rndReader, 4<<11)
				after.Path = "~after"
				want = unixfs.BuildDirectory(t, lsys, []unixfs.DirEntry{before, want, after}, false)

				before = unixfs.GenerateFile(t, lsys, rndReader, 4<<10)
				before.Path = "!before"
				want.Path = "want"
				after = unixfs.GenerateDirectory(t, lsys, rndReader, 4<<11, true)
				after.Path = "~after"
				want = unixfs.BuildDirectory(t, lsys, []unixfs.DirEntry{before, want, after}, false)

				before = unixfs.GenerateFile(t, lsys, rndReader, 4<<10)
				before.Path = "!before"
				want.Path = "want"
				after = unixfs.GenerateFile(t, lsys, rndReader, 4<<11)
				after.Path = "~after"
				want = unixfs.BuildDirectory(t, lsys, []unixfs.DirEntry{before, want, after}, false)

				return want
			},
			path:        "/want/want/want",
			modifyQuery: shallowQuery,
			validateBody: func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want"
					srcData.Children[1].Children[1].Root, // "/want/want"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // "/want/want/want" (full dir)
				)
				validateCarBody(t, body, srcData.Root, wantCids)
			},
		},
		{
			name: "UnixFSShardedDirectoryDAG, shallow",
			generate: func(t *testing.T, remotes []testpeer.TestPeer) unixfs.DirEntry {
				return unixfs.GenerateDirectory(t, &remotes[0].LinkSystem, rndReader, 16<<20, true)
			},
			modifyQuery: shallowQuery,
			validateBody: func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				// sharded directory contains multiple blocks, so we expect a CAR with
				// exactly those blocks
				validateCarBody(t, body, srcData.Root, srcData.SelfCids)
			},
		},
		{
			name: "nested UnixFSShardedDirectoryDAG, with path, shallow",
			generate: func(t *testing.T, remotes []testpeer.TestPeer) unixfs.DirEntry {
				lsys := &remotes[0].LinkSystem

				before := unixfs.GenerateDirectory(t, lsys, rndReader, 4<<10, false)
				before.Path = "!before"
				// target dir, sharded
				want := unixfs.GenerateDirectory(t, &remotes[0].LinkSystem, rndReader, 16<<20, true)
				want.Path = "want"
				after := unixfs.GenerateFile(t, lsys, rndReader, 4<<11)
				after.Path = "~after"
				want = unixfs.BuildDirectory(t, lsys, []unixfs.DirEntry{before, want, after}, false)

				before = unixfs.GenerateFile(t, lsys, rndReader, 4<<10)
				before.Path = "!before"
				want.Path = "want"
				after = unixfs.GenerateDirectory(t, lsys, rndReader, 4<<11, true)
				after.Path = "~after"
				want = unixfs.BuildDirectory(t, lsys, []unixfs.DirEntry{before, want, after}, false)

				before = unixfs.GenerateFile(t, lsys, rndReader, 4<<10)
				before.Path = "!before"
				want.Path = "want"
				after = unixfs.GenerateFile(t, lsys, rndReader, 4<<11)
				after.Path = "~after"
				want = unixfs.BuildDirectory(t, lsys, []unixfs.DirEntry{before, want, after}, false)

				return want
			},
			path:        "/want/want/want",
			modifyQuery: shallowQuery,
			validateBody: func(t *testing.T, srcData unixfs.DirEntry, body []byte) {
				wantCids := append([]cid.Cid{
					srcData.Root,                         // "/""
					srcData.Children[1].Root,             // "/want"
					srcData.Children[1].Children[1].Root, // "/want/want"
				},
					srcData.Children[1].Children[1].Children[1].SelfCids..., // "/want/want/want" (full dir)
				)
				validateCarBody(t, body, srcData.Root, wantCids)
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			mrn := mocknet.NewMockRetrievalNet(ctx, t)
			mrn.AddGraphsyncPeers(1)
			finishedChan := mocknet.SetupRetrieval(t, mrn.Remotes[0])
			mrn.MN.LinkAll()

			srcData := testCase.generate(t, mrn.Remotes)
			qr := testQueryResponse
			qr.MinPricePerByte = abi.NewTokenAmount(0) // make it free so it's not filtered
			mocknet.SetupQuery(t, mrn.Remotes[0], srcData.Root, qr)

			// Setup a new lassie
			req := require.New(t)
			lassie, err := lassie.NewLassie(
				ctx,
				lassie.WithProviderTimeout(20*time.Second),
				lassie.WithHost(mrn.Self),
				lassie.WithFinder(mrn.Finder),
			)
			req.NoError(err)

			// Start an HTTP server
			cfg := httpserver.HttpServerConfig{Address: "127.0.0.1", Port: 0, TempDir: t.TempDir()}
			if testCase.modifyHttpConfig != nil {
				cfg = testCase.modifyHttpConfig(cfg)
			}
			httpServer, err := httpserver.NewHttpServer(ctx, lassie, cfg)
			req.NoError(err)
			go func() {
				err := httpServer.Start()
				req.NoError(err)
			}()
			t.Cleanup(func() {
				req.NoError(httpServer.Close())
			})

			// Make a request for our CID and read the complete CAR bytes
			path := ""
			if testCase.path != "" {
				path = "/" + testCase.path
			}
			addr := fmt.Sprintf("http://%s/ipfs/%s%s", httpServer.Addr(), srcData.Root.String(), path)
			getReq, err := http.NewRequest("GET", addr, nil)
			req.NoError(err)
			getReq.Header.Add("Accept", "application/vnd.ipld.car")
			if testCase.modifyQuery != nil {
				q := getReq.URL.Query()
				testCase.modifyQuery(q)
				getReq.URL.RawQuery = q.Encode()
			}
			t.Log("Fetching", getReq.URL.String())
			client := &http.Client{}
			resp, err := client.Do(getReq)
			req.NoError(err)
			req.Equal(http.StatusOK, resp.StatusCode)
			body, err := io.ReadAll(resp.Body)
			req.NoError(err)
			resp.Body.Close()
			req.NoError(err)

			mocknet.WaitForFinish(ctx, t, finishedChan, 1*time.Second)

			if testCase.validateBody != nil {
				testCase.validateBody(t, srcData, body)
			} else {
				// Open the CAR bytes as read-only storage
				reader, err := storage.OpenReadable(bytes.NewReader(body))
				req.NoError(err)

				// Load our UnixFS data and compare it to the original
				linkSys := cidlink.DefaultLinkSystem()
				linkSys.SetReadStorage(reader)
				linkSys.NodeReifier = unixfsnode.Reify
				linkSys.TrustedStorage = true
				gotDir := unixfs.ToDirEntry(t, linkSys, srcData.Root, true)
				unixfs.CompareDirEntries(t, srcData, gotDir)
			}
		})
	}
}

func validateCarBody(t *testing.T, body []byte, root cid.Cid, wantCids []cid.Cid) {
	br, err := carv2.NewBlockReader(bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, []cid.Cid{root}, br.Roots)
	var count int
	for {
		blk, err := br.Next()
		if err != nil {
			require.EqualError(t, err, io.EOF.Error())
			break
		}
		count++
		var found bool
		for _, c := range wantCids {
			if c.Equals(blk.Cid()) {
				found = true
				break
			}
		}
		require.True(t, found)
	}
	require.Equal(t, len(wantCids), count)
}
