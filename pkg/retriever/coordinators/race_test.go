package coordinators_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/filecoin-project/lassie/pkg/retriever/coordinators"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
)

func TestRace(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		name           string
		results        []timeoutResult
		getResultsBy   time.Duration
		contextCancels bool
		expectedStats  *types.RetrievalStats
		expectedErr    error
	}{
		{
			name: "two successes, returns first to finish",
			results: []timeoutResult{
				{
					duration: 200 * time.Millisecond,
					stats: &types.RetrievalStats{
						StorageProviderId: peer.ID("apples"),
					},
				},
				{
					duration: 400 * time.Millisecond,
					stats: &types.RetrievalStats{
						StorageProviderId: peer.ID("oranges"),
					},
				},
			},
			getResultsBy: 200 * time.Millisecond,
			expectedStats: &types.RetrievalStats{
				StorageProviderId: peer.ID("apples"),
			},
		},
		{
			name: "two errors, returns multi error in sequence",
			results: []timeoutResult{
				{
					duration: 200 * time.Millisecond,
					err:      errors.New("something went wrong"),
				},
				{
					duration: 400 * time.Millisecond,
					err:      errors.New("something else went wrong"),
				},
			},
			getResultsBy: 400 * time.Millisecond,
			expectedErr:  multierr.Append(errors.New("something went wrong"), errors.New("something else went wrong")),
		},
		{
			name: "error then success, returns success",
			results: []timeoutResult{
				{
					duration: 200 * time.Millisecond,
					err:      errors.New("something went wrong"),
				},
				{
					duration: 400 * time.Millisecond,
					stats: &types.RetrievalStats{
						StorageProviderId: peer.ID("oranges"),
					},
				},
			},
			getResultsBy: 400 * time.Millisecond,
			expectedStats: &types.RetrievalStats{
				StorageProviderId: peer.ID("oranges"),
			},
		},
		{
			name: "success then error, returns success",
			results: []timeoutResult{
				{
					duration: 200 * time.Millisecond,
					stats: &types.RetrievalStats{
						StorageProviderId: peer.ID("apples"),
					},
				},
				{
					duration: 400 * time.Millisecond,
					err:      errors.New("something went wrong"),
				},
			},
			getResultsBy: 200 * time.Millisecond,
			expectedStats: &types.RetrievalStats{
				StorageProviderId: peer.ID("apples"),
			},
		},
		{
			name: "context cancels before any finish, returns context error",
			results: []timeoutResult{
				{
					duration: 200 * time.Millisecond,
					stats: &types.RetrievalStats{
						StorageProviderId: peer.ID("apples"),
					},
				},
				{
					duration: 400 * time.Millisecond,
					stats: &types.RetrievalStats{
						StorageProviderId: peer.ID("oranges"),
					},
				},
			},
			getResultsBy:   100 * time.Millisecond,
			contextCancels: true,
			expectedErr:    context.Canceled,
		},
		{
			name: "context cancels after one error, returns context error",
			results: []timeoutResult{
				{
					duration: 200 * time.Millisecond,
					err:      errors.New("something went wrong"),
				},
				{
					duration: 400 * time.Millisecond,
					stats: &types.RetrievalStats{
						StorageProviderId: peer.ID("oranges"),
					},
				},
			},
			getResultsBy:   300 * time.Millisecond,
			contextCancels: true,
			expectedErr:    context.Canceled,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			clock := clock.NewMock()
			startChan := make(chan struct{})
			resultChan := make(chan types.RetrievalResult)
			childCtx, childCancel := context.WithCancel(ctx)
			defer childCancel()
			go func() {
				retrievalCalls := func(ctx context.Context, callRetrieval func(types.RetrievalTask)) {
					for _, result := range testCase.results {
						callRetrieval(types.AsyncRetrievalTask{
							AsyncCandidateRetrieval: &timeoutRetriever{result, ctx, clock, startChan},
						})
					}
				}
				stats, err := coordinators.Race(childCtx, retrievalCalls)
				select {
				case <-ctx.Done():
				case resultChan <- types.RetrievalResult{Stats: stats, Err: err}:
				}
			}()
			for range testCase.results {
				select {
				case <-ctx.Done():
					require.FailNow(t, "failed to start retrievers")
				case <-startChan:
				}
			}
			clock.Add(testCase.getResultsBy)
			if testCase.contextCancels {
				childCancel()
			}
			select {
			case <-ctx.Done():
				require.FailNow(t, "failed to receive result")
			case result := <-resultChan:
				require.Equal(t, testCase.expectedStats, result.Stats)
				require.Equal(t, testCase.expectedErr, result.Err)
			}
		})
	}
}

type timeoutResult struct {
	duration time.Duration
	stats    *types.RetrievalStats
	err      error
}

type timeoutRetriever struct {
	timeoutResult
	ctx       context.Context
	clock     clock.Clock
	startChan chan<- struct{}
}

func (t *timeoutRetriever) RetrieveFromAsyncCandidates(types.InboundAsyncCandidates) (*types.RetrievalStats, error) {
	timer := t.clock.Timer(t.duration)
	t.startChan <- struct{}{}
	select {
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	case <-timer.C:
		return t.stats, t.err
	}
}
