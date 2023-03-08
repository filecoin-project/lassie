package bitswaphelpers_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/retriever/bitswaphelpers"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestIndexerRouting(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	cidToRetrievalIDs := make(map[cid.Cid][]types.RetrievalID)
	toRetrievalID := func(c cid.Cid) []types.RetrievalID {
		return cidToRetrievalIDs[c]
	}
	ir := bitswaphelpers.NewIndexerRouting(toRetrievalID)
	id1, err := types.NewRetrievalID()
	req.NoError(err)
	id2, err := types.NewRetrievalID()
	req.NoError(err)
	id3, err := types.NewRetrievalID()
	req.NoError(err)
	cids := testutil.GenerateCids(4)
	cidToRetrievalIDs[cids[0]] = []types.RetrievalID{id1}
	cidToRetrievalIDs[cids[1]] = []types.RetrievalID{id2}
	cidToRetrievalIDs[cids[2]] = []types.RetrievalID{id3}
	cidToRetrievalIDs[cids[3]] = []types.RetrievalID{id1, id3}
	// no candidates should be returned initially
	verifyCandidates(ctx, t, nil, ir.FindProvidersAsync(ctx, cids[0], 5))
	verifyCandidates(ctx, t, nil, ir.FindProvidersAsync(ctx, cids[1], 5))
	verifyCandidates(ctx, t, nil, ir.FindProvidersAsync(ctx, cids[2], 5))
	candidates1 := testutil.GenerateRetrievalCandidates(10)
	candidates2 := testutil.GenerateRetrievalCandidates(10)
	candidates3 := testutil.GenerateRetrievalCandidates(10)
	ir.AddProviders(id1, candidates1)
	ir.AddProviders(id2, candidates2)
	ir.AddProviders(id3, candidates3)
	verifyCandidates(ctx, t, candidates1[:5], ir.FindProvidersAsync(ctx, cids[0], 5))
	verifyCandidates(ctx, t, candidates2[:5], ir.FindProvidersAsync(ctx, cids[1], 5))
	verifyCandidates(ctx, t, candidates3[:5], ir.FindProvidersAsync(ctx, cids[2], 5))
	// add more to one retrieval
	extraCandidates := testutil.GenerateRetrievalCandidates(5)
	ir.AddProviders(id1, extraCandidates)
	// remove another retrieval
	ir.RemoveProviders(id2)
	// retrieval that had added candidates should include extra results across retrievals
	verifyCandidates(ctx, t, append(append(candidates1[5:], extraCandidates...), candidates3[5:7]...), ir.FindProvidersAsync(ctx, cids[3], 12))
	// retrieval that had candidates removed should include no results
	verifyCandidates(ctx, t, nil, ir.FindProvidersAsync(ctx, cids[1], 10))
}
func TestIndexerRoutingAsync(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	cidToRetrievalIDs := make(map[cid.Cid][]types.RetrievalID)
	toRetrievalID := func(c cid.Cid) []types.RetrievalID {
		return cidToRetrievalIDs[c]
	}
	ir := bitswaphelpers.NewIndexerRouting(toRetrievalID)
	id1, err := types.NewRetrievalID()
	req.NoError(err)
	id2, err := types.NewRetrievalID()
	req.NoError(err)
	id3, err := types.NewRetrievalID()
	req.NoError(err)
	cids := testutil.GenerateCids(4)
	cidToRetrievalIDs[cids[0]] = []types.RetrievalID{id1}
	cidToRetrievalIDs[cids[1]] = []types.RetrievalID{id2}
	cidToRetrievalIDs[cids[2]] = []types.RetrievalID{id3}
	cidToRetrievalIDs[cids[3]] = []types.RetrievalID{id1, id3}
	// no candidates should be returned initially
	ir.SignalIncomingRetrieval(id1)
	ir.SignalIncomingRetrieval(id2)
	ir.SignalIncomingRetrieval(id3)
	candidates1 := testutil.GenerateRetrievalCandidates(10)
	candidates2 := testutil.GenerateRetrievalCandidates(10)
	receivedCandidates := make(chan struct{}, 1)
	go func() {
		verifyCandidates(ctx, t, candidates1[:5], ir.FindProvidersAsync(ctx, cids[0], 5))
		verifyCandidates(ctx, t, candidates2[:5], ir.FindProvidersAsync(ctx, cids[1], 5))
		verifyCandidates(ctx, t, nil, ir.FindProvidersAsync(ctx, cids[2], 5))
		receivedCandidates <- struct{}{}
	}()
	ir.AddProviders(id1, candidates1)
	ir.AddProviders(id2, candidates2)
	ir.RemoveProviders(id3)
	select {
	case <-ctx.Done():
		req.FailNow("never received candidates")
	case <-receivedCandidates:
	}

	// insure subsequence operations work as expected

	// add more to one retrieval
	extraCandidates := testutil.GenerateRetrievalCandidates(5)
	ir.AddProviders(id1, extraCandidates)
	// remove another retrieval
	ir.RemoveProviders(id2)
	// retrieval that had added candidates should include extra results across retrievals
	verifyCandidates(ctx, t, append(append(candidates1[5:], extraCandidates...)), ir.FindProvidersAsync(ctx, cids[3], 12))
	// retrieval that had candidates removed should include no results
	verifyCandidates(ctx, t, nil, ir.FindProvidersAsync(ctx, cids[1], 10))
}

func verifyCandidates(ctx context.Context, t *testing.T, expectedCandidates []types.RetrievalCandidate, incoming <-chan peer.AddrInfo) {
	expectedAddrInfos := make([]peer.AddrInfo, 0, len(expectedCandidates))
	for _, candidate := range expectedCandidates {
		expectedAddrInfos = append(expectedAddrInfos, candidate.MinerPeer)
	}
	receivedAddrInfos := make([]peer.AddrInfo, 0, len(expectedCandidates))
addrInfosReceived:
	for {
		select {
		case <-ctx.Done():
			require.FailNow(t, "candidate channel failed to close")
		case next, ok := <-incoming:
			if !ok {
				break addrInfosReceived
			}
			receivedAddrInfos = append(receivedAddrInfos, next)
		}
	}
	require.Equal(t, expectedAddrInfos, receivedAddrInfos)
}
