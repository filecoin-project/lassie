package itest

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/internal/itest/mocknet"
	"github.com/filecoin-project/lassie/pkg/internal/itest/unixfs"
	"github.com/filecoin-project/lassie/pkg/internal/lp2ptransports"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
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

			mrn := mocknet.NewMockRetrievalNet(ctx, t)
			mrn.AddGraphsyncPeers(1)
			mrn.AddBitswapPeers(1)

			// generate separate 4MiB random unixfs file DAGs on both peers

			// graphsync peer (0)
			graphsyncMAs, err := peer.AddrInfoToP2pAddrs(mrn.Remotes[0].AddrInfo())
			req.NoError(err)
			srcData1 := unixfs.GenerateFile(t, mrn.Remotes[0].LinkSystem, rndReader, 4<<20)
			mocknet.SetupRetrieval(t, mrn.Remotes[0])

			// bitswap peer (1)
			srcData2 := unixfs.GenerateFile(t, mrn.Remotes[1].LinkSystem, bytes.NewReader(srcData1.Content), 4<<20)
			req.Equal(srcData2.Root, srcData2.Root)
			bitswapMAs, err := peer.AddrInfoToP2pAddrs(mrn.Remotes[1].AddrInfo())
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
			req.NoError(mrn.MN.LinkAll())
			defer clear()

			var addr peer.AddrInfo
			switch testCase.directPeer {
			case graphsyncDirect:
				addr = *mrn.Remotes[0].AddrInfo()
			case bitswapDirect:
				addr = *mrn.Remotes[1].AddrInfo()
			case transportsDirect:
				addr = transportsAddr
			default:
				req.FailNow("unrecognized direct peer test")
			}

			directFinder := retriever.NewDirectCandidateFinder(mrn.Self, []peer.AddrInfo{addr})
			lassie, err := lassie.NewLassie(ctx, lassie.WithFinder(directFinder), lassie.WithHost(mrn.Self), lassie.WithGlobalTimeout(5*time.Second))
			req.NoError(err)
			outFile, err := os.CreateTemp(t.TempDir(), "lassie-test-")
			req.NoError(err)
			defer func() {
				req.NoError(outFile.Close())
			}()
			outCar, err := storage.NewReadableWritable(outFile, []cid.Cid{srcData1.Root}, carv2.WriteAsCarV1(true))
			req.NoError(err)
			request, err := types.NewRequestForPath(outCar, srcData1.Root, "", types.DagScopeAll)
			req.NoError(err)
			_, err = lassie.Fetch(ctx, request, func(types.RetrievalEvent) {})
			req.NoError(err)
			err = outCar.Finalize()
			req.NoError(err)
			outFile.Seek(0, io.SeekStart)
			// Open the CAR bytes as read-only storage
			reader, err := storage.OpenReadable(outFile)
			req.NoError(err)

			// Load our UnixFS data and compare it to the original
			linkSys := cidlink.DefaultLinkSystem()
			linkSys.SetReadStorage(reader)
			linkSys.NodeReifier = unixfsnode.Reify
			linkSys.TrustedStorage = true
			gotDir := unixfs.ToDirEntry(t, linkSys, srcData1.Root, true)
			unixfs.CompareDirEntries(t, srcData1, gotDir)
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
