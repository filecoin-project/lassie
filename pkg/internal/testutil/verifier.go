package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

type ExpectedActionsAtTime struct {
	AfterStart           time.Duration
	ReceivedConnections  []peer.ID
	ReceivedRetrievals   []peer.ID
	CandidatesDiscovered []DiscoveredCandidate
	ExpectedEvents       []types.RetrievalEvent
}

type RetrievalVerifier struct {
	ExpectedSequence []ExpectedActionsAtTime
}

type RunRetrieval func(cb func(types.RetrievalEvent)) (*types.RetrievalStats, error)

func (rv RetrievalVerifier) RunWithVerification(ctx context.Context, t *testing.T, clock *clock.Mock, mockClient *MockClient, mockCandidateFinder *MockCandidateFinder, runRetrievals []RunRetrieval) []types.RetrievalResult {
	resultChan := make(chan types.RetrievalResult, len(runRetrievals))
	asyncCollectingEventsListener := NewAsyncCollectingEventsListener(ctx)
	for _, runRetrieval := range runRetrievals {
		runRetrieval := runRetrieval
		go func() {
			result, err := runRetrieval(asyncCollectingEventsListener.Collect)
			resultChan <- types.RetrievalResult{Stats: result, Err: err}
		}()
	}
	currentTime := time.Duration(0)
	for _, expectedActionsAtTime := range rv.ExpectedSequence {
		clock.Add(expectedActionsAtTime.AfterStart - currentTime)
		currentTime = expectedActionsAtTime.AfterStart
		t.Logf("current time: %s", clock.Now())
		asyncCollectingEventsListener.VerifyNextEvents(t, expectedActionsAtTime.ExpectedEvents)
		if mockCandidateFinder != nil {
			mockCandidateFinder.VerifyCandidatesDiscovered(ctx, t, expectedActionsAtTime.CandidatesDiscovered)
		}
		if mockClient != nil {
			mockClient.VerifyConnectionsReceived(ctx, t, expectedActionsAtTime.ReceivedConnections)
			mockClient.VerifyRetrievalsReceived(ctx, t, expectedActionsAtTime.ReceivedRetrievals)
		}
	}
	results := make([]types.RetrievalResult, 0, len(runRetrievals))
	for i := 0; i < len(runRetrievals); i++ {
		select {
		case result := <-resultChan:
			results = append(results, result)
		case <-ctx.Done():
			require.FailNow(t, "did not complete retrieval")
		}
	}
	return results
}
