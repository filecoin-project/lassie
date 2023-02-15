package bitswaphelpers_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/retriever/bitswaphelpers"
	"github.com/filecoin-project/lassie/pkg/retriever/testutil"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestIndexerRouting(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	ir := bitswaphelpers.NewIndexerRouting()
	id1, err := types.NewRetrievalID()
	req.NoError(err)
	id2, err := types.NewRetrievalID()
	req.NoError(err)
	id3, err := types.NewRetrievalID()
	req.NoError(err)
	c := testutil.GenerateCid()
	requestCtx1 := types.RegisterRetrievalIDToContext(ctx, id1)
	requestCtx2 := types.RegisterRetrievalIDToContext(ctx, id2)
	requestCtx3 := types.RegisterRetrievalIDToContext(ctx, id3)
	// no candidates should be returned initially
	verifyCandidates(ctx, t, nil, ir.FindProvidersAsync(ctx, c, 5))
	verifyCandidates(ctx, t, nil, ir.FindProvidersAsync(requestCtx1, c, 5))
	verifyCandidates(ctx, t, nil, ir.FindProvidersAsync(requestCtx2, c, 5))
	verifyCandidates(ctx, t, nil, ir.FindProvidersAsync(requestCtx3, c, 5))
	candidates1 := testutil.GenerateRetrievalCandidates(10)
	candidates2 := testutil.GenerateRetrievalCandidates(10)
	candidates3 := testutil.GenerateRetrievalCandidates(10)
	ir.AddProviders(id1, candidates1)
	ir.AddProviders(id2, candidates2)
	ir.AddProviders(id3, candidates3)
	verifyCandidates(ctx, t, nil, ir.FindProvidersAsync(ctx, c, 5))
	verifyCandidates(ctx, t, candidates1[:5], ir.FindProvidersAsync(requestCtx1, c, 5))
	verifyCandidates(ctx, t, candidates2[:5], ir.FindProvidersAsync(requestCtx2, c, 5))
	verifyCandidates(ctx, t, candidates3[:5], ir.FindProvidersAsync(requestCtx3, c, 5))
	// add more to one retrieval
	extraCandidates := testutil.GenerateRetrievalCandidates(5)
	ir.AddProviders(id1, extraCandidates)
	// remove another retrieval
	ir.RemoveProviders(id2)
	verifyCandidates(ctx, t, nil, ir.FindProvidersAsync(ctx, c, 10))
	// retrieval that had added candidates should include extra results
	verifyCandidates(ctx, t, append(candidates1[5:], extraCandidates...), ir.FindProvidersAsync(requestCtx1, c, 10))
	// retrieval that had candidates removed should include no results
	verifyCandidates(ctx, t, nil, ir.FindProvidersAsync(requestCtx2, c, 10))
	// retrieval that was left in place returns remaining candidates
	verifyCandidates(ctx, t, candidates3[5:], ir.FindProvidersAsync(requestCtx3, c, 10))
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
