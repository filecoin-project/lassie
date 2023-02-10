package coordinators_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/retriever/coordinators"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
)

func TestSequence(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		name          string
		results       []types.RetrievalResult
		expectedStats *types.RetrievalStats
		expectedErr   error
	}{
		{
			name: "two successes, returns first in sequence",
			results: []types.RetrievalResult{
				{
					Value: &types.RetrievalStats{
						StorageProviderId: peer.ID("apples"),
					},
				},
				{
					Value: &types.RetrievalStats{
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
			results: []types.RetrievalResult{
				{
					Err: errors.New("something went wrong"),
				},
				{
					Value: &types.RetrievalStats{
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
			results: []types.RetrievalResult{
				{
					Value: &types.RetrievalStats{
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
			retrievalCalls := make([]types.CandidateRetrievalCall, 0, len(testCase.results))
			for _, result := range testCase.results {
				retrievalCalls = append(retrievalCalls, types.CandidateRetrievalCall{
					CandidateRetrieval: &stubRetriever{result},
				})
			}
			stats, err := coordinators.Sequence(ctx, retrievalCalls)
			require.Equal(t, testCase.expectedStats, stats)
			require.Equal(t, testCase.expectedErr, err)
		})
	}
}

type stubRetriever struct {
	types.RetrievalResult
}

func (s *stubRetriever) RetrieveFromCandidates(_ []types.RetrievalCandidate) (*types.RetrievalStats, error) {
	return s.Value, s.Err
}
