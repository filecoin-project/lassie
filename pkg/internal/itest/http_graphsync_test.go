package itest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lassie/pkg/internal/itest/mocknet"
	"github.com/filecoin-project/lassie/pkg/internal/itest/unixfs"
	"github.com/filecoin-project/lassie/pkg/lassie"
	httpserver "github.com/filecoin-project/lassie/pkg/server/http"
	"github.com/ipfs/go-unixfsnode"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

func TestHttpRetrieval(t *testing.T) {
	testCases := []struct {
		name  string
		limit uint64
	}{
		{
			name: "regular",
		},
		{
			name:  "max block limit",
			limit: 3,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			rndSeed := time.Now().UTC().UnixNano()
			t.Logf("random seed: %d", rndSeed)
			var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

			mrn := mocknet.NewMockRetrievalNet(ctx, t)
			mrn.AddGraphsyncPeers(1)
			finishedChan := mocknet.SetupRetrieval(t, mrn.Remotes[0])
			mrn.MN.LinkAll()

			rootCid, srcBytes := unixfs.GenerateFile(t, &mrn.Remotes[0].LinkSystem, rndReader, 4<<20)
			mrn.Remotes[0].RootCid = rootCid // for the CandidateFinder
			srcData := []unixfs.DirEntry{{Path: "", Cid: rootCid, Content: srcBytes}}
			qr := testQueryResponse
			qr.MinPricePerByte = abi.NewTokenAmount(0) // make it free so it's not filtered
			mocknet.SetupQuery(t, mrn.Remotes[0], rootCid, qr)

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
			httpServer, err := httpserver.NewHttpServer(ctx, lassie, httpserver.HttpServerConfig{
				Address:             "127.0.0.1",
				Port:                0,
				TempDir:             t.TempDir(),
				MaxBlocksPerRequest: testCase.limit,
			})
			req.NoError(err)
			go func() {
				err := httpServer.Start()
				req.NoError(err)
			}()
			t.Cleanup(func() {
				req.NoError(httpServer.Close())
			})

			// Make a request for our CID and read the complete CAR bytes
			addr := fmt.Sprintf("http://%s/ipfs/%s", httpServer.Addr(), rootCid.String())
			getReq, err := http.NewRequest("GET", addr, nil)
			req.NoError(err)
			getReq.Header.Add("Accept", "application/vnd.ipld.car")
			client := &http.Client{}
			resp, err := client.Do(getReq)
			req.NoError(err)
			req.Equal(http.StatusOK, resp.StatusCode)
			body, err := io.ReadAll(resp.Body)
			req.NoError(err)
			resp.Body.Close()
			req.NoError(err)

			mocknet.WaitForFinish(ctx, t, finishedChan, 1*time.Second)

			if testCase.limit == 0 {
				// Open the CAR bytes as read-only storage
				reader, err := storage.OpenReadable(bytes.NewReader(body))
				req.NoError(err)

				// Load our UnixFS data and compare it to the original
				linkSys := cidlink.DefaultLinkSystem()
				linkSys.SetReadStorage(reader)
				linkSys.NodeReifier = unixfsnode.Reify
				linkSys.TrustedStorage = true
				gotDir := unixfs.ToDirEntry(t, linkSys, rootCid)
				unixfs.CompareDirEntries(t, srcData, gotDir)
			} else {
				br, err := carv2.NewBlockReader(bytes.NewReader(body))
				req.NoError(err)
				count := uint64(0)
				for {
					_, err := br.Next()
					if err != nil {
						req.EqualError(err, io.EOF.Error())
						break
					}
					count++
				}
				req.Equal(testCase.limit, count)
			}
		})
	}
}
