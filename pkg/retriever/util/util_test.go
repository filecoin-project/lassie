package util_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/lassie/pkg/retriever/util"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
)

func TestRaceRetrievers(t *testing.T) {
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
			childRetrievers := make([]types.Retriever, 0, len(testCase.results))
			for _, result := range testCase.results {
				childRetrievers = append(childRetrievers, (&timeoutRetriever{result, clock, startChan}).Retrieve)
			}
			retriever := util.RaceRetrievers(childRetrievers)
			resultChan := make(chan types.RetrievalResult)
			childCtx, childCancel := context.WithCancel(ctx)
			defer childCancel()
			go func() {
				stats, err := retriever(childCtx, types.RetrievalRequest{}, func(types.RetrievalEvent) {})
				select {
				case <-ctx.Done():
				case resultChan <- types.RetrievalResult{Stats: stats, Err: err}:
				}
			}()
			for range childRetrievers {
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

func TestSequenceRetrievers(t *testing.T) {
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
			childRetrievers := make([]types.Retriever, 0, len(testCase.results))
			for _, result := range testCase.results {
				childRetrievers = append(childRetrievers, (&stubRetriever{result}).Retrieve)
			}
			retriever := util.SequenceRetrievers(childRetrievers)
			stats, err := retriever(ctx, types.RetrievalRequest{}, func(types.RetrievalEvent) {})
			require.Equal(t, testCase.expectedStats, stats)
			require.Equal(t, testCase.expectedErr, err)
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
	clock     clock.Clock
	startChan chan<- struct{}
}

func (t *timeoutRetriever) Retrieve(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
	timer := t.clock.Timer(t.duration)
	t.startChan <- struct{}{}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return t.stats, t.err
	}
}

type stubRetriever struct {
	types.RetrievalResult
}

func (s *stubRetriever) Retrieve(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
	return s.Stats, s.Err
}
