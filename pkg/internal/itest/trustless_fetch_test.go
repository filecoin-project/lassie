package itest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/filecoin-project/lassie/pkg/internal/itest/mocknet"
	"github.com/filecoin-project/lassie/pkg/internal/itest/testpeer"
	"github.com/filecoin-project/lassie/pkg/lassie"
	httpserver "github.com/filecoin-project/lassie/pkg/server/http"
	"github.com/google/uuid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	trustlesspathing "github.com/ipld/ipld/specs/pkg-go/trustless-pathing"
	"github.com/stretchr/testify/require"
)

func TestTrustlessUnixfsFetch(t *testing.T) {
	req := require.New(t)

	testCases, err := trustlesspathing.Unixfs20mVarietyCases()
	req.NoError(err)
	storage, closer, err := trustlesspathing.Unixfs20mVarietyReadableStorage()
	req.NoError(err)
	defer closer.Close()

	lsys := cidlink.DefaultLinkSystem()
	lsys.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)
	lsys.SetReadStorage(storage)

	for _, tc := range testCases {
		for _, proto := range []string{"http", "graphsync", "bitswap"} {
			t.Run(tc.Name+"/"+proto, func(t *testing.T) {
				req := require.New(t)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				t.Logf("query=%s, blocks=%d", tc.AsQuery(), len(tc.ExpectedCids))

				var finishedChan chan []datatransfer.Event
				mrn := mocknet.NewMockRetrievalNet(ctx, t)
				switch proto {
				case "http":
					mrn.AddHttpPeers(1, testpeer.WithLinkSystem(lsys))
				case "graphsync":
					mrn.AddGraphsyncPeers(1, testpeer.WithLinkSystem(lsys))
					finishedChan = mocknet.SetupRetrieval(t, mrn.Remotes[0])
				case "bitswap":
					mrn.AddBitswapPeers(1, testpeer.WithLinkSystem(lsys))
				}

				req.NoError(mrn.MN.LinkAll())
				mrn.Remotes[0].Cids[tc.Root] = struct{}{}
				// pre-connect while https://github.com/ipfs/boxo/issues/432 remains unresolved
				_, err := mrn.MN.ConnectPeers(mrn.Self.ID(), mrn.Remotes[0].ID)
				req.NoError(err)

				lassie, err := lassie.NewLassie(
					ctx,
					lassie.WithProviderTimeout(20*time.Second),
					lassie.WithHost(mrn.Self),
					lassie.WithFinder(mrn.Finder),
				)
				req.NoError(err)
				cfg := httpserver.HttpServerConfig{Address: "127.0.0.1", Port: 0, TempDir: t.TempDir()}
				httpServer, err := httpserver.NewHttpServer(ctx, lassie, cfg)
				req.NoError(err)
				serverError := make(chan error, 1)
				go func() {
					serverError <- httpServer.Start()
				}()
				responseChan := make(chan *http.Response, 1)
				go func() {
					// Make a request for our CID and read the complete CAR bytes
					addr := fmt.Sprintf("http://%s%s", httpServer.Addr(), tc.AsQuery())
					getReq, err := http.NewRequest("GET", addr, nil)
					req.NoError(err)
					getReq.Header.Add("Accept", "application/vnd.ipld.car")
					t.Log("Fetching", getReq.URL.String())
					resp, err := http.DefaultClient.Do(getReq)
					req.NoError(err)
					responseChan <- resp
				}()
				var resp *http.Response
				select {
				case resp = <-responseChan:
				case <-ctx.Done():
					req.FailNow("Did not receive responses")
				}
				if finishedChan != nil {
					// for graphsync
					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						mocknet.WaitForFinish(ctx, t, finishedChan, 1*time.Second)
						wg.Done()
					}()
					wg.Wait()
				}
				if resp.StatusCode != http.StatusOK {
					body, err := io.ReadAll(resp.Body)
					req.NoError(err)
					req.Failf("200 response code not received", "got code: %d, body: %s", resp.StatusCode, string(body))
				}
				req.Equal(fmt.Sprintf(`attachment; filename="%s.car"`, tc.Root.String()), resp.Header.Get("Content-Disposition"))
				req.Equal("none", resp.Header.Get("Accept-Ranges"))
				req.Equal("public, max-age=29030400, immutable", resp.Header.Get("Cache-Control"))
				req.Equal("application/vnd.ipld.car; version=1", resp.Header.Get("Content-Type"))
				req.Equal("nosniff", resp.Header.Get("X-Content-Type-Options"))
				etagStart := fmt.Sprintf(`"%s.car.`, tc.Root.String())
				etagGot := resp.Header.Get("ETag")
				req.True(strings.HasPrefix(etagGot, etagStart), "ETag should start with [%s], got [%s]", etagStart, etagGot)
				req.Equal(`"`, etagGot[len(etagGot)-1:], "ETag should end with a quote")
				req.Equal(fmt.Sprintf("/ipfs/%s%s", tc.Root.String(), tc.Path), resp.Header.Get("X-Ipfs-Path"))
				requestId := resp.Header.Get("X-Trace-Id")
				req.NotEmpty(requestId)
				_, err = uuid.Parse(requestId)
				req.NoError(err)

				rdr, err := car.NewBlockReader(resp.Body)
				req.NoError(err)
				req.Len(rdr.Roots, 1)
				req.Equal(tc.Root.String(), rdr.Roots[0].String())
				for ii := 0; ; ii++ {
					blk, err := rdr.Next()
					if err == io.EOF {
						if ii != len(tc.ExpectedCids) {
							req.FailNowf("unexpected EOF", "expected %d blocks, got %d", len(tc.ExpectedCids), ii)
						}
						break
					}
					req.NoError(err)
					if ii >= len(tc.ExpectedCids) {
						req.FailNowf("unexpected block", "got block %d, expected %d", ii, len(tc.ExpectedCids))
					}
					req.Equal(tc.ExpectedCids[ii].String(), blk.Cid().String(), "unexpected block #%d", ii)
				}
			})
		}
	}
}
