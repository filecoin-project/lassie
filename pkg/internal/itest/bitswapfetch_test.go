package itest

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/lassie/pkg/internal/itest/testpeer"
	"github.com/filecoin-project/lassie/pkg/internal/itest/unixfs"
	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/lassie"
	httpserver "github.com/filecoin-project/lassie/pkg/server/http"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync/storeutil"
	"github.com/ipfs/go-libipfs/bitswap/network"
	"github.com/ipfs/go-libipfs/bitswap/server"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2/storage"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestBitswapFetchTwoPeers(t *testing.T) {
	req := require.New(t)
	mn := mocknet.New()
	ctx := context.Background()
	testPeerGenerator := testpeer.NewTestPeerGenerator(ctx, t, mn, []network.NetOpt{}, []server.Option{})
	peers := testPeerGenerator.Peers(2)

	// build two files of 4MiB random bytes, packaged into unixfs DAGs
	// (rootCid1 & rootCid2) and the original source data retained
	// (srcData1, srcData2)
	ls := storeutil.LinkSystemForBlockstore(peers[0].Blockstore())
	rootCid1, srcData1 := unixfs.GenerateFile(t, &ls, rand.Reader, 4<<20)
	ls = storeutil.LinkSystemForBlockstore(peers[1].Blockstore())
	rootCid2, srcData2 := unixfs.GenerateFile(t, &ls, rand.Reader, 4<<20)

	finder := &testutil.MockCandidateFinder{
		Candidates: map[cid.Cid][]types.RetrievalCandidate{
			rootCid1: {
				{
					RootCid: rootCid1,
					MinerPeer: peer.AddrInfo{
						ID:    peers[0].ID,
						Addrs: peers[0].Host.Addrs(),
					},
					Metadata: metadata.Default.New(metadata.Bitswap{}),
				},
			},
			rootCid2: {
				{
					RootCid: rootCid2,
					MinerPeer: peer.AddrInfo{
						ID:    peers[1].ID,
						Addrs: peers[1].Host.Addrs(),
					},
					Metadata: metadata.Default.New(metadata.Bitswap{}),
				},
			},
		},
	}
	self, err := mn.GenPeer()
	req.NoError(err)
	mn.LinkAll()

	lassie, err := lassie.NewLassie(ctx, lassie.WithFinder(finder), lassie.WithHost(self), lassie.WithGlobalTimeout(5*time.Second))
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
