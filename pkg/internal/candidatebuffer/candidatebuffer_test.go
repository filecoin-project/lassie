package candidatebuffer_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/lassie/pkg/internal/candidatebuffer"
	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCandidateBuffer(t *testing.T) {
	ctx := context.Background()
	candidates := testutil.GenerateRetrievalCandidates(t, 10)

	type candidateResultAt struct {
		timeDelta time.Duration
		cancel    bool
		result    types.FindCandidatesResult
	}
	testCases := []struct {
		name                  string
		bufferingTime         time.Duration
		incomingResults       []candidateResultAt
		expectedCandidateSets [][]types.RetrievalCandidate
		expectedErr           error
	}{

		{
			name:          "two time windows of candidates",
			bufferingTime: 200 * time.Millisecond,
			incomingResults: []candidateResultAt{
				{
					timeDelta: 50 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[0]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[1]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[2]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[3]},
				},
				{
					timeDelta: 500 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[4]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[5]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[6]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[7]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[8]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[9]},
				},
			},
			expectedCandidateSets: [][]types.RetrievalCandidate{
				candidates[:4],
				candidates[4:],
			},
		},
		{
			name:          "with error, stops consuming, returns error",
			bufferingTime: 200 * time.Millisecond,
			incomingResults: []candidateResultAt{
				{
					timeDelta: 50 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[0]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[1]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[2]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[3]},
				},
				{
					timeDelta: 500 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[4]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[5]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[6]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Err: errors.New("something went wrong")},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[8]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[9]},
				},
			},
			expectedCandidateSets: [][]types.RetrievalCandidate{
				candidates[:4],
				candidates[4:7],
			},
			expectedErr: errors.New("something went wrong"),
		},
		{
			name:          "with cancel, stops consuming, does not return buffer",
			bufferingTime: 200 * time.Millisecond,
			incomingResults: []candidateResultAt{
				{
					timeDelta: 50 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[0]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[1]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[2]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[3]},
				},
				{
					timeDelta: 500 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[4]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[5]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[6]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					cancel:    true,
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[8]},
				},
				{
					timeDelta: 5 * time.Millisecond,
					result:    types.FindCandidatesResult{Candidate: candidates[9]},
				},
			},
			expectedCandidateSets: [][]types.RetrievalCandidate{
				candidates[:4],
			},
			expectedErr: context.Canceled,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			req := require.New(t)
			var receivedCandidatesLk sync.Mutex
			var receivedCandidates [][]types.RetrievalCandidate
			clock := clock.NewMock()
			candididateBuffer := candidatebuffer.NewCandidateBuffer(func(next []types.RetrievalCandidate) {
				receivedCandidatesLk.Lock()
				receivedCandidates = append(receivedCandidates, next)
				receivedCandidatesLk.Unlock()
			}, clock)
			queueCandidates := func(ctx context.Context, onNextCandidate candidatebuffer.OnNextCandidate) error {
				for _, nextCandidateResultAt := range testCase.incomingResults {
					clock.Add(nextCandidateResultAt.timeDelta)
					if nextCandidateResultAt.cancel {
						cancel()
						return context.Canceled
					}
					select {
					case <-ctx.Done():
					default:
					}
					if nextCandidateResultAt.result.Err != nil {
						return nextCandidateResultAt.result.Err
					}
					onNextCandidate(nextCandidateResultAt.result.Candidate)
				}
				return nil
			}
			err := candididateBuffer.BufferStream(ctx, queueCandidates, testCase.bufferingTime)
			if testCase.expectedErr != nil {
				req.EqualError(err, testCase.expectedErr.Error())
			} else {
				req.NoError(err)
			}
			receivedCandidatesLk.Lock()
			req.Equal(testCase.expectedCandidateSets, receivedCandidates)
			receivedCandidatesLk.Unlock()
		})
	}
}
