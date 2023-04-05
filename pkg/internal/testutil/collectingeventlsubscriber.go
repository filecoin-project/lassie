package testutil

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/stretchr/testify/require"
)

func NewAsyncCollectingEventsListener(ctx context.Context) *AsyncCollectingEventsListener {
	return &AsyncCollectingEventsListener{
		ctx:                ctx,
		retrievalEventChan: make(chan types.RetrievalEvent, 16),
	}
}

type AsyncCollectingEventsListener struct {
	ctx                context.Context
	retrievalEventChan chan types.RetrievalEvent
}

func (ev *AsyncCollectingEventsListener) Collect(evt types.RetrievalEvent) {
	select {
	case <-ev.ctx.Done():
	case ev.retrievalEventChan <- evt:
	}
}

func (ev *AsyncCollectingEventsListener) VerifyNextEvents(t *testing.T, expectedEvents []types.RetrievalEvent) {
	for i := 0; i < len(expectedEvents); i++ {
		select {
		case evt := <-ev.retrievalEventChan:
			t.Logf("received event: %s", evt)
			VerifyContainsCollectedEvent(t, expectedEvents, evt)
		case <-ev.ctx.Done():
			require.FailNow(t, "did not receive expected events")
		}
	}
}

type CollectingEventsListener struct {
	lk              sync.Mutex
	CollectedEvents []types.RetrievalEvent
}

func NewCollectingEventsListener() *CollectingEventsListener {
	return &CollectingEventsListener{
		CollectedEvents: make([]types.RetrievalEvent, 0),
	}
}

func (el *CollectingEventsListener) Collect(event types.RetrievalEvent) {
	el.lk.Lock()
	defer el.lk.Unlock()
	el.CollectedEvents = append(el.CollectedEvents, event)
}

func VerifyCollectedEventTimings(t *testing.T, events []types.RetrievalEvent) {
	for i, event := range events {
		if i == 0 {
			continue
		}
		prevEvent := events[i-1]
		// verify that each event comes after the previous one, but allow some
		// flexibility for overlapping event types
		if event.Code() != prevEvent.Code() {
			require.True(t, event.Time() == prevEvent.Time() || event.Time().After(prevEvent.Time()), "event time order for %s/%s vs %s/%s", prevEvent.Code(), prevEvent.Phase(), event.Code(), event.Phase())
		}
		if event.Phase() == prevEvent.Phase() {
			require.Equal(t, event.PhaseStartTime(), prevEvent.PhaseStartTime(), "same phase start time for %s in %s vs %s", event.Phase(), prevEvent.Code(), event.Code())
		}
	}
}

func VerifyContainsCollectedEvent(t *testing.T, actualList []types.RetrievalEvent, expected types.RetrievalEvent) {
	for _, actual := range actualList {
		// this matching might need to evolve to be more sophisticated, particularly SP ID
		if actual.Code() == expected.Code() &&
			actual.RetrievalId() == expected.RetrievalId() &&
			actual.PayloadCid() == expected.PayloadCid() &&
			actual.Phase() == expected.Phase() {
			if actual.StorageProviderId() == expected.StorageProviderId() {
				VerifyCollectedEvent(t, actual, expected)
				return
			}
		}
	}
	require.Fail(t, "event not found", expected.Code())
}

func VerifyCollectedEvent(t *testing.T, actual types.RetrievalEvent, expected types.RetrievalEvent) {
	require.Equal(t, expected.Code(), actual.Code(), "event code")
	require.Equal(t, expected.RetrievalId(), actual.RetrievalId(), fmt.Sprintf("retrieval id for %s", expected.Code()))
	require.Equal(t, expected.PayloadCid(), actual.PayloadCid(), fmt.Sprintf("cid for %s", expected.Code()))
	require.Equal(t, expected.Phase(), actual.Phase(), fmt.Sprintf("phase for %s", expected.Code()))
	require.Equal(t, expected.PhaseStartTime(), actual.PhaseStartTime(), fmt.Sprintf("phase start time for %s", expected.Code()))
	require.Equal(t, expected.Time(), actual.Time(), fmt.Sprintf("time for %s", expected.Code()))
	require.Equal(t, expected.StorageProviderId(), actual.StorageProviderId(), fmt.Sprintf("storage provider id for %s", expected.Code()))
	if ec, ok := expected.(events.EventWithCandidates); ok {
		if ac, ok := actual.(events.EventWithCandidates); ok {
			require.Len(t, ac.Candidates(), len(ec.Candidates()), fmt.Sprintf("candidate length for %s", expected.Code()))
			for ii, expectedCandidate := range ec.Candidates() {
				var found bool
				for _, actualCandidate := range ac.Candidates() {
					if expectedCandidate.MinerPeer.ID == actualCandidate.MinerPeer.ID &&
						expectedCandidate.RootCid == actualCandidate.RootCid {
						// only compare metadata if present on the expected candidate
						if (expectedCandidate.Metadata.Len() == 0) || reflect.DeepEqual(expectedCandidate.Metadata, actualCandidate.Metadata) {
							found = true
							break
						}
					}
				}
				if !found {
					require.Fail(t, fmt.Sprintf("candidate #%d not found for %s", ii, expected.Code()))
				}
			}
		} else {
			require.Fail(t, "wrong event type, no Candidates", expected.Code())
		}
	}
	if efail, ok := expected.(events.RetrievalEventFailed); ok {
		if afail, ok := actual.(events.RetrievalEventFailed); ok {
			require.Equal(t, efail.ErrorMessage(), afail.ErrorMessage(), fmt.Sprintf("error message for %s", expected.Code()))
		} else {
			require.Fail(t, "wrong event type", expected.Code())
		}
	}
	if esuccess, ok := expected.(events.RetrievalEventSuccess); ok {
		if asuccess, ok := actual.(events.RetrievalEventSuccess); ok {
			require.Equal(t, esuccess.ReceivedSize(), asuccess.ReceivedSize(), fmt.Sprintf("received size for %s", expected.Code()))
			require.Equal(t, esuccess.ReceivedCids(), asuccess.ReceivedCids(), fmt.Sprintf("received cids for %s", expected.Code()))
			require.Equal(t, esuccess.Duration(), asuccess.Duration(), fmt.Sprintf("duration for %s", expected.Code()))
		} else {
			require.Fail(t, "wrong event type", expected.Code())
		}
	}
}
