package testutil

import (
	"math/rand"
	"testing"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	"github.com/ipfs/go-libipfs/blocks"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/index-provider/metadata"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

var blockGenerator = blocksutil.NewBlockGenerator()

// var prioritySeq int
var seedSeq int64

// RandomBytes returns a byte array of the given size with random values.
func RandomBytes(n int64) []byte {
	data := make([]byte, n)
	src := rand.NewSource(seedSeq)
	seedSeq++
	r := rand.New(src)
	_, _ = r.Read(data)
	return data
}

// GenerateBlocksOfSize generates a series of blocks of the given byte size
func GenerateBlocksOfSize(n int, size int64) []blocks.Block {
	generatedBlocks := make([]blocks.Block, 0, n)
	for i := 0; i < n; i++ {
		b := blocks.NewBlock(RandomBytes(size))
		generatedBlocks = append(generatedBlocks, b)

	}
	return generatedBlocks
}

// GenerateCid produces a content identifier.
func GenerateCid() cid.Cid {
	return GenerateCids(1)[0]
}

// GenerateCids produces n content identifiers.
func GenerateCids(n int) []cid.Cid {
	cids := make([]cid.Cid, 0, n)
	for i := 0; i < n; i++ {
		c := blockGenerator.Next().Cid()
		cids = append(cids, c)
	}
	return cids
}

var peerSeq int

// GeneratePeers creates n peer ids.
func GeneratePeers(t *testing.T, n int) []peer.ID {
	src := rand.NewSource(seedSeq)
	seedSeq++
	r := rand.New(src)
	peerIds := make([]peer.ID, 0, n)
	for i := 0; i < n; i++ {
		_, publicKey, err := crypto.GenerateEd25519Key(r)
		require.NoError(t, err)
		peerID, err := peer.IDFromPublicKey(publicKey)
		require.NoError(t, err)
		peerIds = append(peerIds, peerID)
	}
	return peerIds
}

// GenerateRetrievalRequests produces retrieval requests
func GenerateRetrievalRequests(t *testing.T, n int) []types.RetrievalRequest {
	cids := GenerateCids(n)
	rids := GenerateRetrievalIDs(t, n)
	requests := make([]types.RetrievalRequest, 0, n)
	for i := 0; i < n; i++ {
		requests = append(requests, types.RetrievalRequest{
			RetrievalID: rids[i],
			Cid:         cids[i],
			LinkSystem:  cidlink.DefaultLinkSystem(),
		})
	}
	return requests
}

// GenerateRetrievalCandidates produces n retrieval candidates
func GenerateRetrievalCandidates(t *testing.T, n int) []types.RetrievalCandidate {
	candidates := make([]types.RetrievalCandidate, 0, n)
	c := GenerateCid()
	peers := GeneratePeers(t, n)
	for i := 0; i < n; i++ {
		candidates = append(candidates, types.NewRetrievalCandidate(peers[i], c, &metadata.Bitswap{}))
	}
	return candidates
}

func GenerateRetrievalIDs(t *testing.T, n int) []types.RetrievalID {
	retrievalIDs := make([]types.RetrievalID, 0, n)
	for i := 0; i < n; i++ {
		id, err := types.NewRetrievalID()
		require.NoError(t, err)
		retrievalIDs = append(retrievalIDs, id)
	}
	return retrievalIDs
}
