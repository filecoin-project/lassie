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

func TestAsync(t *testing.T) {
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
			expectedErr: multierr.Append(errors.New("something went wrong"), errors.New("something else went wrong")),
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
			req := require.New(t)
			retrievalsQueued := make(chan struct{}, 1)
			retrievalResult := make(chan types.RetrievalResult, 1)
			unlockRetriever := make(chan struct{})
			retrievalCalls := func(ctx context.Context, callRetrieval func(types.Retrieval)) {
				for _, result := range testCase.results {
					callRetrieval(types.CandidateRetrievalCall{
						CandidateRetrieval: &slowRetriever{unlockRetriever, result},
					})
				}
				retrievalsQueued <- struct{}{}
			}
			go func() {
				stats, err := coordinators.Async(ctx, retrievalCalls)
				retrievalResult <- types.RetrievalResult{Stats: stats, Err: err}
			}()
			select {
			case <-ctx.Done():
				req.FailNow("did not queue retrievals")
			case <-retrievalsQueued:
			}
			close(unlockRetriever)
			var stats *types.RetrievalStats
			var err error
			select {
			case <-ctx.Done():
				req.FailNow("did not complete retrievals")
			case result := <-retrievalResult:
				stats, err = result.Stats, result.Err
			}
			req.Equal(testCase.expectedStats, stats)
			req.Equal(testCase.expectedErr, err)
		})
	}
}

type slowRetriever struct {
	unlock <-chan struct{}
	types.RetrievalResult
}

func (s *slowRetriever) RetrieveFromCandidates(_ []types.RetrievalCandidate) (*types.RetrievalStats, error) {
	<-s.unlock
	return s.Stats, s.Err
}
