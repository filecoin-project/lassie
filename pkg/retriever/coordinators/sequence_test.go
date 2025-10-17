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
			results: []types.RetrievalResult{
				{
					Err: errors.New("something went wrong"),
				},
				{
					Err: errors.New("something else went wrong"),
				},
			},
			expectedErr: errors.Join(errors.New("something went wrong"), errors.New("something else went wrong")),
		},
		{
			name: "error then success, returns success",
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
			retrievalCalls := func(ctx context.Context, callRetrieval func(types.RetrievalTask)) {
				for _, result := range testCase.results {
					callRetrieval(types.AsyncRetrievalTask{
						AsyncCandidateRetrieval: &stubRetriever{result},
					})
				}
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

func (s *stubRetriever) RetrieveFromAsyncCandidates(_ types.InboundAsyncCandidates) (*types.RetrievalStats, error) {
	return s.Stats, s.Err
}
