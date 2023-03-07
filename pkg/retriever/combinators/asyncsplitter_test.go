package combinators_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/retriever/combinators"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestAsyncCandidateSplitter(t *testing.T) {
	ctx := context.Background()
	candidateSets := make([][]types.RetrievalCandidate, 0, 3)
	for i := 0; i < 3; i++ {
		candidateSets = append(candidateSets, testutil.GenerateRetrievalCandidates(t, (i+1)*2))
	}
	testCases := []struct {
		name                 string
		keys                 []string
		splitFunc            splitFunc
		expectedReceivedSets map[string][][]types.RetrievalCandidate
		expectedErr          error
	}{
		{
			name: "simple even split",
			keys: []string{"even", "odd"},
			splitFunc: func(candidates []types.RetrievalCandidate) (map[string][]types.RetrievalCandidate, error) {
				candidateMap := make(map[string][]types.RetrievalCandidate, 2)
				for i, candidate := range candidates {
					var key string
					if i%2 == 0 {
						key = "even"
					} else {
						key = "odd"
					}
					candidateMap[key] = append(candidateMap[key], candidate)
				}
				return candidateMap, nil
			},
			expectedReceivedSets: map[string][][]types.RetrievalCandidate{
				"even": {
					{candidateSets[0][0]},
					{candidateSets[1][0], candidateSets[1][2]},
					{candidateSets[2][0], candidateSets[2][2], candidateSets[2][4]},
				},
				"odd": {
					{candidateSets[0][1]},
					{candidateSets[1][1], candidateSets[1][3]},
					{candidateSets[2][1], candidateSets[2][3], candidateSets[2][5]},
				},
			},
		},
		{
			name:      "stateful",
			keys:      []string{"batch1", "batch2"},
			splitFunc: (&statefulSplit{splitPoint: 4}).splitFunc,
			expectedReceivedSets: map[string][][]types.RetrievalCandidate{
				"batch1": {
					{candidateSets[0][0], candidateSets[0][1]},
					{candidateSets[1][0], candidateSets[1][1]},
				},
				"batch2": {
					{candidateSets[1][2], candidateSets[1][3]},
					{candidateSets[2][0],
						candidateSets[2][1],
						candidateSets[2][2],
						candidateSets[2][3],
						candidateSets[2][4],
						candidateSets[2][5]},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			req := require.New(t)
			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			var wg sync.WaitGroup
			incoming, outgoing := types.MakeAsyncCandidates(0)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, candidates := range candidateSets {
					req.NoError(outgoing.SendNext(ctx, candidates))
				}
				close(outgoing)
			}()
			asyncCandidateSplitter := combinators.NewAsyncCandidateSplitter(testCase.keys, func([]string) types.CandidateSplitter[string] {
				return mockSplitter{testCase.splitFunc}
			})
			asyncRetrievalSplitter := asyncCandidateSplitter.SplitRetrievalRequest(ctx, testutil.GenerateRetrievalRequests(t, 1)[0], func(types.RetrievalEvent) {})
			streams, errChan := asyncRetrievalSplitter.SplitAsyncCandidates(incoming)
			req.Len(streams, len(testCase.keys))
			for _, key := range testCase.keys {
				key := key
				stream, ok := streams[key]
				req.True(ok)
				wg.Add(1)
				go func() {
					defer wg.Done()
					for _, candidates := range testCase.expectedReceivedSets[key] {
						hasCandidates, nextCandidates, err := stream.Next(ctx)
						req.NoError(err)
						req.True(hasCandidates)
						req.Equal(candidates, nextCandidates)
					}
					hasCandidates, nextCandidates, err := stream.Next(ctx)
					req.NoError(err)
					req.False(hasCandidates)
					req.Nil(nextCandidates)
				}()
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case <-ctx.Done():
					req.FailNow("did not receive on error channel")
				case err := <-errChan:
					if testCase.expectedErr != nil {
						req.EqualError(err, testCase.expectedErr.Error())
					} else {
						req.NoError(err)
					}
				}
			}()
			wg.Wait()
		})
	}
}

type splitFunc func([]types.RetrievalCandidate) (map[string][]types.RetrievalCandidate, error)

type mockSplitter struct {
	splitFunc splitFunc
}

func (ms mockSplitter) SplitRetrievalRequest(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) types.RetrievalSplitter[string] {
	return ms
}

func (ms mockSplitter) SplitCandidates(candidates []types.RetrievalCandidate) (map[string][]types.RetrievalCandidate, error) {
	return ms.splitFunc(candidates)
}

type statefulSplit struct {
	splitPoint    int
	receivedSoFar int
}

func (ss *statefulSplit) splitFunc(candidates []types.RetrievalCandidate) (map[string][]types.RetrievalCandidate, error) {
	candidateMap := make(map[string][]types.RetrievalCandidate, 2)
	for _, candidate := range candidates {
		var key string
		if ss.receivedSoFar >= ss.splitPoint {
			key = "batch2"
		} else {
			key = "batch1"
		}
		ss.receivedSoFar++
		candidateMap[key] = append(candidateMap[key], candidate)
	}
	return candidateMap, nil
}
