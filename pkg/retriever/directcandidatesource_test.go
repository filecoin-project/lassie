package retriever_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestDirectCandidateSourceNoLibp2p(t *testing.T) {
	rootCid := random.Cids(1)[0]
	p := random.Peers(1)[0]
	rawMultiaddr := random.Multiaddrs(1)[0]
	httpMultiaddr := random.HttpMultiaddrs(1)[0]
	ctx := context.Background()
	testCases := []struct {
		name              string
		provider          types.Provider
		expectedCandidate types.RetrievalCandidate
	}{
		{
			name: "peer with protocols",
			provider: types.Provider{
				Peer: peer.AddrInfo{
					ID:    p,
					Addrs: []multiaddr.Multiaddr{rawMultiaddr},
				},
				Protocols: []metadata.Protocol{metadata.IpfsGatewayHttp{}, metadata.Bitswap{}},
			},
			expectedCandidate: types.RetrievalCandidate{
				MinerPeer: peer.AddrInfo{
					ID:    p,
					Addrs: []multiaddr.Multiaddr{rawMultiaddr},
				},
				RootCid:  rootCid,
				Metadata: metadata.Default.New(metadata.IpfsGatewayHttp{}, metadata.Bitswap{}),
			},
		},
		{
			name: "peer with no protocols and standard multiaddr",
			provider: types.Provider{
				Peer: peer.AddrInfo{
					ID:    p,
					Addrs: []multiaddr.Multiaddr{rawMultiaddr},
				},
			},
			expectedCandidate: types.RetrievalCandidate{
				MinerPeer: peer.AddrInfo{
					ID:    p,
					Addrs: []multiaddr.Multiaddr{rawMultiaddr},
				},
				RootCid:  rootCid,
				Metadata: metadata.Default.New(metadata.IpfsGatewayHttp{}, metadata.Bitswap{}, &metadata.GraphsyncFilecoinV1{}),
			},
		},
		{
			name: "peer with no protocols and http multiaddr",
			provider: types.Provider{
				Peer: peer.AddrInfo{
					ID:    p,
					Addrs: []multiaddr.Multiaddr{httpMultiaddr},
				},
			},
			expectedCandidate: types.RetrievalCandidate{
				MinerPeer: peer.AddrInfo{
					ID:    p,
					Addrs: []multiaddr.Multiaddr{httpMultiaddr},
				},
				RootCid:  rootCid,
				Metadata: metadata.Default.New(metadata.IpfsGatewayHttp{}),
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			d := retriever.NewDirectCandidateSource([]types.Provider{testCase.provider})
			d.FindCandidates(ctx, rootCid, func(candidate types.RetrievalCandidate) {
				require.Equal(t, testCase.expectedCandidate, candidate)
			})
		})
	}
}
