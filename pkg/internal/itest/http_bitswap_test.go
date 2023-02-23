package itest

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/internal/itest/mocknet"
	"github.com/filecoin-project/lassie/pkg/internal/itest/unixfs"
	"github.com/filecoin-project/lassie/pkg/lassie"
	httpserver "github.com/filecoin-project/lassie/pkg/server/http"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2/storage"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

func TestBitswapFetchTwoPeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rndSeed := time.Now().UTC().UnixNano()
	t.Logf("random seed: %d", rndSeed)
	var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

	req := require.New(t)
	mrn := mocknet.NewMockRetrievalNet(ctx, t)
	mrn.AddBitswapPeers(2)

	// build two files of 4MiB random bytes, packaged into unixfs DAGs
	// (rootCid1 & rootCid2) and the original source data retained
	// (srcData1, srcData2)
	rootCid1, srcData1 := unixfs.GenerateFile(t, &mrn.Remotes[0].LinkSystem, rndReader, 4<<20)
	mrn.Remotes[0].RootCid = rootCid1 // for the CandidateFinder
	rootCid2, srcData2 := unixfs.GenerateFile(t, &mrn.Remotes[1].LinkSystem, rndReader, 4<<20)
	mrn.Remotes[1].RootCid = rootCid2 // for the CandidateFinder

	require.NoError(t, mrn.MN.LinkAll())

	lassie, err := lassie.NewLassie(ctx, lassie.WithFinder(mrn.Finder), lassie.WithHost(mrn.Self), lassie.WithGlobalTimeout(5*time.Second))
	req.NoError(err)

	httpServer, err := httpserver.NewHttpServer(ctx, lassie, httpserver.HttpServerConfig{
		Address: "127.0.0.1",
		Port:    8888,
	})
	req.NoError(err)
	baseURL := httpServer.Addr()
	serverError := make(chan error, 1)
	go func() {
		err := httpServer.Start()
		serverError <- err
	}()
	// make two requests at the same time
	resp1Chan := make(chan *http.Response, 1)
	resp2Chan := make(chan *http.Response, 1)
	go func() {
		resp, err := http.DefaultClient.Get(fmt.Sprintf("http://%s/ipfs/%s?format=car", baseURL, rootCid1))
		req.NoError(err)
		resp1Chan <- resp
	}()
	go func() {
		resp, err := http.DefaultClient.Get(fmt.Sprintf("http://%s/ipfs/%s?format=car", baseURL, rootCid2))
		req.NoError(err)
		resp2Chan <- resp
	}()
	var resp1, resp2 *http.Response
	received := 0
	for received < 2 {
		select {
		case resp1 = <-resp1Chan:
			received++
		case resp2 = <-resp2Chan:
			received++
		case <-ctx.Done():
			req.FailNow("Did not receive responses")
		}
	}
	// verify first response
	req.Equal(200, resp1.StatusCode)
	carData, err := io.ReadAll(resp1.Body)
	req.NoError(err)
	rCar, err := storage.OpenReadable(&byteReadAt{carData})
	req.NoError(err)
	outLsys := cidlink.DefaultLinkSystem()
	outLsys.SetReadStorage(rCar)
	outLsys.NodeReifier = unixfsnode.Reify
	nd, err := outLsys.Load(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: rootCid1}, dagpb.Type.PBNode)
	req.NoError(err)
	destData, err := nd.AsBytes()
	req.NoError(err)
	req.Equal(srcData1, destData)

	// verify second response
	req.Equal(200, resp2.StatusCode)
	carData, err = io.ReadAll(resp2.Body)
	req.NoError(err)
	rCar, err = storage.OpenReadable(&byteReadAt{carData})
	req.NoError(err)
	outLsys = cidlink.DefaultLinkSystem()
	outLsys.SetReadStorage(rCar)
	outLsys.NodeReifier = unixfsnode.Reify
	nd, err = outLsys.Load(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: rootCid2}, dagpb.Type.PBNode)
	req.NoError(err)
	destData, err = nd.AsBytes()
	req.NoError(err)
	req.Equal(srcData2, destData)

	err = httpServer.Close()
	req.NoError(err)
	select {
	case <-ctx.Done():
		req.FailNow("server failed to shut down")
	case err = <-serverError:
		req.NoError(err)
	}
}

type byteReadAt struct {
	data []byte
}

func (bra *byteReadAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(bra.data)) {
		return 0, io.EOF
	}
	n = copy(p, bra.data[int64(off):])
	return
}
