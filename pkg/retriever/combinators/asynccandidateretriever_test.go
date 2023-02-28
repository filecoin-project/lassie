package combinators_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/retriever/combinators"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
)

func TestAsyncCandidateRetriever(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		name          string
		results       []types.RetrievalResult
		candidateSets [][]types.RetrievalCandidate
		expectedStats *types.RetrievalStats
		expectedErr   error
	}{
		{
			name: "two successes, returns first in sequence",
			candidateSets: [][]types.RetrievalCandidate{
				testutil.GenerateRetrievalCandidates(3),
				testutil.GenerateRetrievalCandidates(2),
			},
			results: []types.RetrievalResult{
				{
					Stats: &types.RetrievalStats{
						StorageProviderId: peer.ID("apples"),
					},
				},
				{
					Stats: &types.RetrievalStats{
						StorageProviderId: peer.ID("oranges"),
					},
				},
			},

			expectedStats: &types.RetrievalStats{
				StorageProviderId: peer.ID("apples"),
			},
		},
		{
			name: "two errors, returns multi error in sequence",
			candidateSets: [][]types.RetrievalCandidate{
				testutil.GenerateRetrievalCandidates(3),
				testutil.GenerateRetrievalCandidates(2),
			},
			results: []types.RetrievalResult{
				{
					Err: errors.New("something went wrong"),
				},
				{
					Err: errors.New("something else went wrong"),
				},
			},
			expectedErr: multierr.Append(errors.New("something went wrong"), errors.New("something else went wrong")),
		},
		{
			name: "error then success, returns success",
			candidateSets: [][]types.RetrievalCandidate{
				testutil.GenerateRetrievalCandidates(3),
				testutil.GenerateRetrievalCandidates(2),
			},
			results: []types.RetrievalResult{
				{
					Err: errors.New("something went wrong"),
				},
				{
					Stats: &types.RetrievalStats{
						StorageProviderId: peer.ID("oranges"),
					},
				},
			},
			expectedStats: &types.RetrievalStats{
				StorageProviderId: peer.ID("oranges"),
			},
		},
		{
			name: "success then error, returns success",
			candidateSets: [][]types.RetrievalCandidate{
				testutil.GenerateRetrievalCandidates(3),
				testutil.GenerateRetrievalCandidates(2),
			},
			results: []types.RetrievalResult{
				{
					Stats: &types.RetrievalStats{
						StorageProviderId: peer.ID("apples"),
					},
				},
				{
					Err: errors.New("something went wrong"),
				},
			},
			expectedStats: &types.RetrievalStats{
				StorageProviderId: peer.ID("apples"),
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			req := require.New(t)
			responses := make(map[string]types.RetrievalResult)
			incoming, outgoing := types.MakeAsyncCandidates(len(testCase.candidateSets))
			for i, candidates := range testCase.candidateSets {
				if i >= len(testCase.results) {
					continue
				}
				responses[setKey(candidates)] = testCase.results[i]
				outgoing <- candidates
			}
			close(outgoing)
			async := combinators.AsyncCandidateRetriever{
				CandidateRetriever: &testSyncRetriever{responses},
			}
			retrieval := async.Retrieve(ctx, testutil.GenerateRetrievalRequests(t, 1)[0], func(types.RetrievalEvent) {})
			stats, err := retrieval.RetrieveFromAsyncCandidates(incoming)
			req.Equal(testCase.expectedStats, stats)
			req.Equal(testCase.expectedErr, err)
		})
	}
}

type testSyncRetriever struct {
	responses map[string]types.RetrievalResult
}

func (s *testSyncRetriever) Retrieve(ctx context.Context, req types.RetrievalRequest, events func(types.RetrievalEvent)) types.CandidateRetrieval {
	return s
}

func (s *testSyncRetriever) RetrieveFromCandidates(candidates []types.RetrievalCandidate) (*types.RetrievalStats, error) {
	if response, ok := s.responses[setKey(candidates)]; ok {
		return response.Stats, response.Err
	}
	return nil, errors.New("no response found")
}

func setKey(candidates []types.RetrievalCandidate) string {
	key := ""
	for _, candidate := range candidates {
		key += candidate.MinerPeer.ID.String()
	}
	return key
}
