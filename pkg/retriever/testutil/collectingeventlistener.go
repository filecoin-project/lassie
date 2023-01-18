package testutil

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

type CollectedEvent struct {
	Name               string
	Phase              events.Phase
	RetrievalId        types.RetrievalID
	PhaseStart         time.Time
	EventTime          time.Time
	Cid                cid.Cid
	StorageProviderIds []peer.ID
	Stage              events.Code
	ErrorStr           string
	QueryResponse      *retrievalmarket.QueryResponse
	ReceivedSize       uint64
	ReceivedCids       uint64
	Confirmed          bool
}

type CollectingEventsListener struct {
	lk              sync.Mutex
	CollectedEvents []CollectedEvent
}

func NewCollectingEventsListener() *CollectingEventsListener {
	return &CollectingEventsListener{
		CollectedEvents: make([]CollectedEvent, 0),
	}
}

func (el *CollectingEventsListener) IndexerProgress(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, stage events.Code) {
	el.lk.Lock()
	defer el.lk.Unlock()
	el.CollectedEvents = append(el.CollectedEvents, CollectedEvent{
		Name:        "IndexerProgress",
		Phase:       events.IndexerPhase,
		RetrievalId: retrievalId,
		PhaseStart:  phaseStartTime,
		EventTime:   eventTime,
		Cid:         requestedCid,
		Stage:       stage,
	})
}
func (el *CollectingEventsListener) IndexerCandidates(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, stage events.Code, storageProviderIds []peer.ID) {
	el.lk.Lock()
	defer el.lk.Unlock()
	el.CollectedEvents = append(el.CollectedEvents, CollectedEvent{
		Name:               "IndexerCandidates",
		Phase:              events.IndexerPhase,
		RetrievalId:        retrievalId,
		PhaseStart:         phaseStartTime,
		EventTime:          eventTime,
		Cid:                requestedCid,
		Stage:              stage,
		StorageProviderIds: storageProviderIds,
	})
}
func (el *CollectingEventsListener) QueryProgress(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage events.Code) {
	el.lk.Lock()
	defer el.lk.Unlock()
	el.CollectedEvents = append(el.CollectedEvents, CollectedEvent{
		Name:               "QueryProgress",
		Phase:              events.QueryPhase,
		RetrievalId:        retrievalId,
		PhaseStart:         phaseStartTime,
		EventTime:          eventTime,
		Cid:                requestedCid,
		StorageProviderIds: []peer.ID{storageProviderId},
		Stage:              stage,
	})
}
func (el *CollectingEventsListener) QueryFailure(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, errString string) {
	el.lk.Lock()
	defer el.lk.Unlock()
	el.CollectedEvents = append(el.CollectedEvents, CollectedEvent{
		Name:               "QueryFailure",
		Phase:              events.QueryPhase,
		RetrievalId:        retrievalId,
		PhaseStart:         phaseStartTime,
		EventTime:          eventTime,
		Cid:                requestedCid,
		StorageProviderIds: []peer.ID{storageProviderId},
		ErrorStr:           errString,
	})
}
func (el *CollectingEventsListener) QuerySuccess(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse) {
	el.lk.Lock()
	defer el.lk.Unlock()
	el.CollectedEvents = append(el.CollectedEvents, CollectedEvent{
		Name:               "QuerySuccess",
		Phase:              events.QueryPhase,
		RetrievalId:        retrievalId,
		PhaseStart:         phaseStartTime,
		EventTime:          eventTime,
		Cid:                requestedCid,
		StorageProviderIds: []peer.ID{storageProviderId},
		QueryResponse:      &queryResponse,
	})
}
func (el *CollectingEventsListener) RetrievalProgress(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage events.Code) {
	el.lk.Lock()
	defer el.lk.Unlock()
	el.CollectedEvents = append(el.CollectedEvents, CollectedEvent{
		Name:               "RetrievalProgress",
		Phase:              events.RetrievalPhase,
		RetrievalId:        retrievalId,
		PhaseStart:         phaseStartTime,
		EventTime:          eventTime,
		Cid:                requestedCid,
		StorageProviderIds: []peer.ID{storageProviderId},
		Stage:              stage,
	})
}
func (el *CollectingEventsListener) RetrievalSuccess(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, receivedSize uint64, receivedCids uint64, confirmed bool) {
	el.lk.Lock()
	defer el.lk.Unlock()
	el.CollectedEvents = append(el.CollectedEvents, CollectedEvent{
		Name:               "RetrievalSuccess",
		Phase:              events.RetrievalPhase,
		RetrievalId:        retrievalId,
		PhaseStart:         phaseStartTime,
		EventTime:          eventTime,
		Cid:                requestedCid,
		StorageProviderIds: []peer.ID{storageProviderId},
		ReceivedSize:       receivedSize,
		ReceivedCids:       receivedCids,
		Confirmed:          confirmed,
	})
}
func (el *CollectingEventsListener) RetrievalFailure(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, errString string) {
	el.lk.Lock()
	defer el.lk.Unlock()
	el.CollectedEvents = append(el.CollectedEvents, CollectedEvent{
		Name:               "RetrievalFailure",
		Phase:              events.RetrievalPhase,
		RetrievalId:        retrievalId,
		PhaseStart:         phaseStartTime,
		EventTime:          eventTime,
		Cid:                requestedCid,
		StorageProviderIds: []peer.ID{storageProviderId},
		ErrorStr:           errString,
	})
}

func VerifyCollectedEventTimings(t *testing.T, events []CollectedEvent) {
	for i, event := range events {
		if i == 0 {
			continue
		}
		prevEvent := events[i-1]
		require.True(t, event.EventTime == prevEvent.EventTime || event.EventTime.After(prevEvent.EventTime))
		if event.Phase == prevEvent.Phase {
			require.Equal(t, event.PhaseStart, prevEvent.PhaseStart, "same phase start time for %s in %s vs %s", event.Phase, prevEvent.Name, event.Name)
		}
	}
}

func VerifyContainsCollectedEvent(t *testing.T, events []CollectedEvent, expected CollectedEvent) {
	for _, event := range events {
		// this matching might need to evolve to be more sophisticated, particularly SP ID
		if event.Name == expected.Name &&
			event.RetrievalId == expected.RetrievalId &&
			event.Cid == expected.Cid &&
			event.Stage == expected.Stage &&
			event.StorageProviderIds[0] == expected.StorageProviderIds[0] {
			VerifyCollectedEvent(t, event, expected)
			return
		}
	}
	require.Fail(t, "event not found", expected.Name)
}

func VerifyCollectedEvent(t *testing.T, actual CollectedEvent, expected CollectedEvent) {
	require.Equal(t, expected.Name, actual.Name, "event name")
	require.Equal(t, expected.RetrievalId, actual.RetrievalId, fmt.Sprintf("retrieval id for %s", expected.Name))
	require.Equal(t, expected.Cid, actual.Cid, fmt.Sprintf("cid for %s", expected.Name))
	require.Equal(t, expected.Stage, actual.Stage, fmt.Sprintf("stage for %s", expected.Name))
	require.Len(t, actual.StorageProviderIds, len(expected.StorageProviderIds), fmt.Sprintf("storage provider ids for %s", expected.Name))
	for ii, expectedProviderId := range expected.StorageProviderIds {
		require.Contains(t, actual.StorageProviderIds, expectedProviderId, fmt.Sprintf("storage provider id #%d for %s", ii, expected.Name))
	}
	require.Equal(t, expected.ErrorStr, actual.ErrorStr, fmt.Sprintf("error string for %s", expected.Name))
	require.Equal(t, expected.QueryResponse, actual.QueryResponse, fmt.Sprintf("query response for %s", expected.Name))
	require.Equal(t, expected.ReceivedSize, actual.ReceivedSize, fmt.Sprintf("received size for %s", expected.Name))
	require.Equal(t, expected.ReceivedCids, actual.ReceivedCids, fmt.Sprintf("received cids for %s", expected.Name))
	require.Equal(t, expected.Confirmed, actual.Confirmed, fmt.Sprintf("confirmed for %s", expected.Name))
}
