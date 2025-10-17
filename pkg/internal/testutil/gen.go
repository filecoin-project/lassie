package testutil

import (
	"fmt"
	"io"
	"testing"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/ipfs/go-unixfsnode/data"
	unixfs "github.com/ipfs/go-unixfsnode/testutil"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/ipni/go-libipni/metadata"
	"github.com/stretchr/testify/require"
)

// GenerateRetrievalRequests produces retrieval requests
func GenerateRetrievalRequests(t *testing.T, n int) []types.RetrievalRequest {
	cids := random.Cids(n)
	rids := GenerateRetrievalIDs(t, n)
	requests := make([]types.RetrievalRequest, 0, n)
	for i := 0; i < n; i++ {
		requests = append(requests, types.RetrievalRequest{
			RetrievalID: rids[i],
			Request:     trustlessutils.Request{Root: cids[i]},
			LinkSystem:  cidlink.DefaultLinkSystem(),
		})
	}
	return requests
}

// GenerateRetrievalCandidates produces n retrieval candidates
func GenerateRetrievalCandidates(t *testing.T, n int, protocols ...metadata.Protocol) []types.RetrievalCandidate {
	c := random.Cids(1)[0]
	return GenerateRetrievalCandidatesForCID(t, n, c, protocols...)
}

// GenerateRetrievalCandidates produces n retrieval candidates
func GenerateRetrievalCandidatesForCID(t *testing.T, n int, c cid.Cid, protocols ...metadata.Protocol) []types.RetrievalCandidate {
	candidates := make([]types.RetrievalCandidate, 0, n)
	peers := random.Peers(n)
	if len(protocols) == 0 {
		protocols = []metadata.Protocol{&metadata.Bitswap{}}
	}
	for i := 0; i < n; i++ {
		addrs := random.HttpMultiaddrs(1)
		candidates = append(candidates, types.NewRetrievalCandidate(peers[i], addrs, c, protocols...))
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

// TODO: these should probably be in unixfsnode/testutil, or as options to
// the respective functions there.

// GenerateNoDupes runs the unixfsnode/testutil generator function repeatedly
// until it produces a DAG with strictly no duplicate CIDs.
func GenerateNoDupes(gen func() unixfs.DirEntry) unixfs.DirEntry {
	var check func(unixfs.DirEntry) bool
	var seen map[cid.Cid]struct{}
	check = func(e unixfs.DirEntry) bool {
		for _, c := range e.SelfCids {
			if _, ok := seen[c]; ok {
				return false
			}
			seen[c] = struct{}{}
		}
		for _, c := range e.Children {
			if !check(c) {
				return false
			}
		}
		return true
	}
	for {
		seen = make(map[cid.Cid]struct{})
		gend := gen()
		if check(gend) {
			return gend
		}
	}
}

// GenerateStrictlyNestedShardedDir is a wrapper around
// unixfsnode/testutil.GenerateDirectory that uses dark magic to repeatedly
// generate a sharded directory until it produces one that is strictly nested.
// That is, it produces a sharded directory structure with strictly at least one
// level of sharding with at least two child shards.
//
// Since it is possible to produce a sharded directory that is
// contained in a single block, this function provides a way to generate a
// sharded directory for cases where we need to test multi-level sharding.
func GenerateStrictlyNestedShardedDir(t *testing.T, linkSys *linking.LinkSystem, randReader io.Reader, targetSize int) unixfs.DirEntry {
	for {
		de := unixfs.GenerateDirectory(t, linkSys, randReader, targetSize, true)
		nd, err := linkSys.Load(linking.LinkContext{}, cidlink.Link{Cid: de.Root}, dagpb.Type.PBNode)
		require.NoError(t, err)
		ufsd, err := data.DecodeUnixFSData(nd.(dagpb.PBNode).Data.Must().Bytes())
		require.NoError(t, err)
		pfxLen := len(fmt.Sprintf("%X", ufsd.FieldFanout().Must().Int()-1))
		iter := nd.(dagpb.PBNode).Links.ListIterator()
		childShards := 0
		for !iter.Done() {
			_, lnk, err := iter.Next()
			require.NoError(t, err)
			nameLen := len(lnk.(dagpb.PBLink).Name.Must().String())
			if nameLen == pfxLen {
				// name is just a shard prefix, so we have at least one level of nesting
				childShards++
			}
		}
		if childShards >= 2 {
			return de
		}
	}
}
