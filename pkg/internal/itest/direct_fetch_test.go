package itest

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lassie/pkg/internal/itest/mocknet"
	"github.com/filecoin-project/lassie/pkg/internal/itest/testpeer"
	"github.com/filecoin-project/lassie/pkg/internal/itest/unixfs"
	"github.com/filecoin-project/lassie/pkg/internal/lp2ptransports"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync/storeutil"
	bsnet "github.com/ipfs/go-libipfs/bitswap/network"
	"github.com/ipfs/go-libipfs/bitswap/server"
	"github.com/ipfs/go-unixfsnode"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	host "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	lpmock "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

const (
	bitswapDirect    = 0
	graphsyncDirect  = 1
	transportsDirect = 2
)

func TestDirectFetch(t *testing.T) {

	testCases := []struct {
		name       string
		directPeer int
	}{
		{
			name:       "direct bitswap peer",
			directPeer: bitswapDirect,
		},
		{
			name:       "direct graphsync peer",
			directPeer: graphsyncDirect,
		},
		{
			name:       "peer responding on transports protocol",
			directPeer: transportsDirect,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			req := require.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			rndSeed := time.Now().UTC().UnixNano()
			t.Logf("random seed: %d", rndSeed)
			var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

			// build two files of 4MiB random bytes, packaged into unixfs DAGs
			// (rootCid1 & rootCid2) and the original source data retained
			// (srcData1, srcData2)
			mrn := mocknet.NewMockRetrievalNet()

			mrn.SetupNet(ctx, t)
			mrn.SetupRetrieval(ctx, t)
			graphsyncAddr := peer.AddrInfo{
				ID:    mrn.HostRemote.ID(),
				Addrs: mrn.HostRemote.Addrs(),
			}
			graphsyncMAs, err := peer.AddrInfoToP2pAddrs(&graphsyncAddr)
			req.NoError(err)
			rootCid, srcBytes := unixfs.GenerateFile(t, &mrn.LinkSystemRemote, rndReader, 4<<20)
			srcData := []unixfs.DirEntry{{Path: "", Cid: rootCid, Content: srcBytes}}
			qr := testQueryResponse
			qr.MinPricePerByte = abi.NewTokenAmount(0) // make it free so it's not filtered
			mrn.SetupQuery(ctx, t, rootCid, qr)
			testPeerGenerator := testpeer.NewTestPeerGenerator(ctx, t, mrn.MN, []bsnet.NetOpt{}, []server.Option{})
			bitswapPeer := testPeerGenerator.Peers(1)[0]
			ls := storeutil.LinkSystemForBlockstore(bitswapPeer.Blockstore())
			rootCidBs, _ := unixfs.GenerateFile(t, &ls, bytes.NewReader(srcBytes), 4<<20)
			req.Equal(rootCid, rootCidBs)
			bitswapAddr := peer.AddrInfo{
				ID:    bitswapPeer.Host.ID(),
				Addrs: bitswapPeer.Host.Addrs(),
			}
			bitswapMAs, err := peer.AddrInfoToP2pAddrs(&bitswapAddr)
			req.NoError(err)
			transportsAddr, clear := handleTransports(t, mrn.MN, []lp2ptransports.Protocol{
				{
					Name:      "bitswap",
					Addresses: bitswapMAs,
				},
				{
					Name:      "libp2p",
					Addresses: graphsyncMAs,
				},
			})
			mrn.MN.LinkAll()
			defer clear()
			var addr peer.AddrInfo
			switch testCase.directPeer {
			case bitswapDirect:
				addr = bitswapAddr
			case graphsyncDirect:
				addr = graphsyncAddr
			case transportsDirect:
				addr = transportsAddr
			default:
				req.FailNow("unrecognized direct peer test")
			}
			directFinder := retriever.NewDirectCandidateFinder(mrn.HostLocal, []peer.AddrInfo{addr})
			lassie, err := lassie.NewLassie(ctx, lassie.WithFinder(directFinder), lassie.WithHost(mrn.HostLocal), lassie.WithGlobalTimeout(5*time.Second))
			req.NoError(err)
			outFile, err := os.CreateTemp(t.TempDir(), "lassie-test-")
			req.NoError(err)
			outCar, err := storage.NewReadableWritable(outFile, []cid.Cid{rootCid}, carv2.WriteAsCarV1(true))
			req.NoError(err)
			outLsys := cidlink.DefaultLinkSystem()
			outLsys.SetReadStorage(outCar)
			outLsys.SetWriteStorage(outCar)
			outLsys.TrustedStorage = true
			_, _, err = lassie.Fetch(ctx, rootCid, outLsys)
			req.NoError(err)
			err = outCar.Finalize()
			req.NoError(err)
			outFile.Seek(0, os.SEEK_SET)
			// Open the CAR bytes as read-only storage
			reader, err := storage.OpenReadable(outFile)
			req.NoError(err)

			// Load our UnixFS data and compare it to the original
			linkSys := cidlink.DefaultLinkSystem()
			linkSys.SetReadStorage(reader)
			linkSys.NodeReifier = unixfsnode.Reify
			linkSys.TrustedStorage = true
			gotDir := unixfs.ToDirEntry(t, linkSys, rootCid)
			unixfs.CompareDirEntries(t, srcData, gotDir)
		})
	}

}

type transportsListener struct {
	t         *testing.T
	host      host.Host
	protocols []lp2ptransports.Protocol
}

func handleTransports(t *testing.T, mn lpmock.Mocknet, protocols []lp2ptransports.Protocol) (peer.AddrInfo, func()) {
	h, err := mn.GenPeer()
	require.NoError(t, err)

	p := &transportsListener{t, h, protocols}
	h.SetStreamHandler(lp2ptransports.TransportsProtocolID, p.handleNewQueryStream)
	return peer.AddrInfo{
			ID:    h.ID(),
			Addrs: h.Addrs(),
		}, func() {
			h.RemoveStreamHandler(lp2ptransports.TransportsProtocolID)
		}
}

// Called when the client opens a libp2p stream
func (l *transportsListener) handleNewQueryStream(s network.Stream) {
	defer s.Close()
	response := lp2ptransports.QueryResponse{Protocols: l.protocols}
	// Write the response to the client
	err := lp2ptransports.BindnodeRegistry.TypeToWriter(&response, s, dagcbor.Encode)
	require.NoError(l.t, err)
}
