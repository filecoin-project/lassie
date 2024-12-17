package testutil

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

type ExpectedActionsAtTime struct {
	AfterStart           time.Duration
	ReceivedConnections  []peer.ID
	ReceivedRetrievals   []peer.ID
	ServedRetrievals     []RemoteStats
	CompletedRetrievals  []peer.ID
	CandidatesDiscovered []DiscoveredCandidate
	ExpectedEvents       []types.RetrievalEvent
	ExpectedMetrics      []SessionMetric
}

type RemoteStats struct {
	Peer      peer.ID
	Root      cid.Cid
	ByteCount uint64
	Blocks    []cid.Cid
	Err       struct{}
}

type RetrievalVerifier struct {
	ExpectedSequence []ExpectedActionsAtTime
}

type RunRetrieval func(cb func(types.RetrievalEvent)) (*types.RetrievalStats, error)

type VerifierClient interface {
	VerifyConnectionsReceived(ctx context.Context, t *testing.T, afterStart time.Duration, expectedConnections []peer.ID)
	VerifyRetrievalsReceived(ctx context.Context, t *testing.T, afterStart time.Duration, expectedRetrievals []peer.ID)
	VerifyRetrievalsServed(ctx context.Context, t *testing.T, afterStart time.Duration, expectedServed []RemoteStats)
	VerifyRetrievalsCompleted(ctx context.Context, t *testing.T, afterStart time.Duration, expectedRetrievals []peer.ID)
}

func (rv RetrievalVerifier) RunWithVerification(
	ctx context.Context,
	t *testing.T,
	clock *clock.Mock,
	client VerifierClient,
	mockCandidateSource *MockCandidateSource,
	mockSession *MockSession,
	cancelFunc context.CancelFunc,
	cancelAfter time.Duration,
	runRetrievals []RunRetrieval,
) []types.RetrievalResult {

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
		if cancelAfter != 0 && cancelFunc != nil && expectedActionsAtTime.AfterStart >= cancelAfter {
			t.Logf("Cancelling context @ %s", cancelAfter)
			cancelFunc()
			cancelFunc = nil
		}
		clock.Add(expectedActionsAtTime.AfterStart - currentTime)
		currentTime = expectedActionsAtTime.AfterStart
		t.Logf("ExpectedAction.AfterStart=%s", expectedActionsAtTime.AfterStart)
		asyncCollectingEventsListener.VerifyNextEvents(t, expectedActionsAtTime.AfterStart, expectedActionsAtTime.ExpectedEvents)
		if mockSession != nil {
			mockSession.VerifyMetricsAt(ctx, t, expectedActionsAtTime.AfterStart, expectedActionsAtTime.ExpectedMetrics)
		}
		if mockCandidateSource != nil {
			mockCandidateSource.VerifyCandidatesDiscovered(ctx, t, expectedActionsAtTime.AfterStart, expectedActionsAtTime.CandidatesDiscovered)
		}
		if client != nil {
			client.VerifyConnectionsReceived(ctx, t, expectedActionsAtTime.AfterStart, expectedActionsAtTime.ReceivedConnections)
			client.VerifyRetrievalsReceived(ctx, t, expectedActionsAtTime.AfterStart, expectedActionsAtTime.ReceivedRetrievals)
			client.VerifyRetrievalsServed(ctx, t, expectedActionsAtTime.AfterStart, expectedActionsAtTime.ServedRetrievals)
			client.VerifyRetrievalsCompleted(ctx, t, expectedActionsAtTime.AfterStart, expectedActionsAtTime.CompletedRetrievals)
		}
	}
	results := make([]types.RetrievalResult, 0, len(runRetrievals))
	for i := 0; i < len(runRetrievals); i++ {
		select {
		case result := <-resultChan:
			results = append(results, result)
		case <-ctx.Done():
			require.FailNowf(t, "did not complete retrieval", "got %d of %d", i, len(runRetrievals))
		}
	}
	return results
}

func BlockReceivedActions(baseTime time.Time, baseAfterStart time.Duration, rid types.RetrievalID, candidate types.RetrievalCandidate, protocol multicodec.Code, blockTime time.Duration, blks []blocks.Block) []ExpectedActionsAtTime {
	var actions []ExpectedActionsAtTime
	for i, blk := range blks {
		actions = append(actions, ExpectedActionsAtTime{
			AfterStart: baseAfterStart + time.Duration(i)*blockTime,
			ExpectedEvents: []types.RetrievalEvent{
				events.BlockReceived(baseTime.Add(baseAfterStart+time.Duration(i)*blockTime), rid, candidate, protocol, uint64(len(blk.RawData()))),
			},
		})
	}
	return actions
}

func SortActions(actions []ExpectedActionsAtTime) []ExpectedActionsAtTime {
	sort.Slice(actions, func(i, j int) bool {
		return actions[i].AfterStart < actions[j].AfterStart
	})
	return actions
}
