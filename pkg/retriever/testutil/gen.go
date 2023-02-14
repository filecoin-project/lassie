package testutil

import (
	"fmt"
	"math/rand"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/libp2p/go-libp2p/core/peer"
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
func GeneratePeers(n int) []peer.ID {
	peerIds := make([]peer.ID, 0, n)
	for i := 0; i < n; i++ {
		peerSeq++
		p := peer.ID(fmt.Sprint(peerSeq))
		peerIds = append(peerIds, p)
	}
	return peerIds
}

// GenerateRetrievalCandidates produces n retrieval candidates
func GenerateRetrievalCandidates(n int) []types.RetrievalCandidate {
	candidates := make([]types.RetrievalCandidate, 0, n)
	c := GenerateCid()
	for i := 0; i < n; i++ {
		peerSeq++
		candidates = append(candidates, types.NewRetrievalCandidate(peer.ID(fmt.Sprint(peerSeq)), c))
	}
	return candidates
}
