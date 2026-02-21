package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

type SessionMetricType string

const (
	SessionMetric_Success   = SessionMetricType("success")
	SessionMetric_Failure   = SessionMetricType("failure")
	SessionMetric_Connect   = SessionMetricType("connect")
	SessionMetric_FirstByte = SessionMetricType("first-byte")
)

type SessionMetric struct {
	Type     SessionMetricType
	Provider peer.ID
	Duration time.Duration
	Value    float64
}

type MockSession struct {
	ctx                      context.Context
	blockList                map[peer.ID]bool
	candidatePreferenceOrder []types.RetrievalCandidate
	metricsCh                chan SessionMetric
}

func NewMockSession(ctx context.Context) *MockSession {
	return &MockSession{
		ctx:       ctx,
		metricsCh: make(chan SessionMetric, 100),
	}
}

func (ms *MockSession) SetCandidatePreferenceOrder(candidatePreferenceOrder []types.RetrievalCandidate) {
	ms.candidatePreferenceOrder = candidatePreferenceOrder
}

func (ms *MockSession) SetBlockList(blockList map[peer.ID]bool) {
	ms.blockList = blockList
}

func (ms *MockSession) VerifyMetricsAt(ctx context.Context, t *testing.T, afterStart time.Duration, expectedMetrics []SessionMetric) {
	metricsReceived := make([]SessionMetric, 0, len(expectedMetrics))
	for i := 0; i < len(expectedMetrics); i++ {
		select {
		case candidate := <-ms.metricsCh:
			metricsReceived = append(metricsReceived, candidate)
		case <-ctx.Done():
			require.FailNowf(t, "failed to receive expected metrics", "expected %d, received %d @ %s", len(expectedMetrics), i, afterStart)
		}
	}
	require.ElementsMatch(t, expectedMetrics, metricsReceived)
}

func (ms *MockSession) FilterIndexerCandidate(candidate types.RetrievalCandidate) (bool, types.RetrievalCandidate) {
	blocked := ms.blockList[candidate.MinerPeer.ID]
	return !blocked, candidate
}

func (ms *MockSession) RegisterRetrieval(retrievalId types.RetrievalID, cid cid.Cid, selector datamodel.Node) bool {
	return true
}

func (ms *MockSession) AddToRetrieval(retrievalId types.RetrievalID, storageProviderIds []peer.ID) error {
	return nil
}

func (ms *MockSession) EndRetrieval(retrievalId types.RetrievalID) error {
	return nil
}

func (ms *MockSession) RecordConnectTime(storageProviderId peer.ID, connectTime time.Duration) {
	ms.addMetric(SessionMetric{
		Type:     SessionMetric_Connect,
		Provider: storageProviderId,
		Duration: connectTime,
	})
}

func (ms *MockSession) RecordFirstByteTime(storageProviderId peer.ID, firstByteTime time.Duration) {
	ms.addMetric(SessionMetric{
		Type:     SessionMetric_FirstByte,
		Provider: storageProviderId,
		Duration: firstByteTime,
	})
}

func (ms *MockSession) RecordFailure(retrievalId types.RetrievalID, storageProviderId peer.ID) error {
	ms.addMetric(SessionMetric{
		Type:     SessionMetric_Failure,
		Provider: storageProviderId,
	})
	return nil
}

func (ms *MockSession) RecordSuccess(storageProviderId peer.ID, bandwidthBytesPerSecond uint64) {
	ms.addMetric(SessionMetric{
		Type:     SessionMetric_Success,
		Provider: storageProviderId,
		Value:    float64(bandwidthBytesPerSecond),
	})
}
func (ms *MockSession) ChooseNextProvider(peers []peer.ID, metadata []metadata.Protocol) int {
	for _, candidate := range ms.candidatePreferenceOrder {
		for i, peer := range peers {
			if candidate.MinerPeer.ID == peer {
				return i
			}
		}
	}
	return 0
}

func (ms *MockSession) addMetric(sm SessionMetric) {
	select {
	case <-ms.ctx.Done():
	case ms.metricsCh <- sm:
	}
}
