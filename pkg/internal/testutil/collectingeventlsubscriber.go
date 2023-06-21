package testutil

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

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

func (ev *AsyncCollectingEventsListener) VerifyNextEvents(t *testing.T, afterStart time.Duration, expectedEvents []types.RetrievalEvent) {
	got := make([]types.EventCode, 0)
	for i := 0; i < len(expectedEvents); i++ {
		select {
		case evt := <-ev.retrievalEventChan:
			t.Logf("received event: %s", evt)
			got = append(got, VerifyContainsCollectedEvent(t, afterStart, expectedEvents, evt))
		case <-ev.ctx.Done():
			// work out which codes we didn't have
			missing := make([]string, 0)
			for _, expected := range expectedEvents {
				found := false
				for _, g := range got {
					if g == expected.Code() {
						found = true
						break
					}
				}
				if !found {
					missing = append(missing, string(expected.Code()))
				}
			}
			require.FailNowf(t, "did not receive expected events", "missing: %s", strings.Join(missing, ", "))
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

// func VerifyCollectedEventTimings(t *testing.T, events []types.RetrievalEvent) {
// 	for i, event := range events {
// 		if i == 0 {
// 			continue
// 		}
// 		prevEvent := events[i-1]
// 		// verify that each event comes after the previous one, but allow some
// 		// flexibility for overlapping event types
// 		if event.Code() != prevEvent.Code() {
// 			require.True(t, event.Time() == prevEvent.Time() || event.Time().After(prevEvent.Time()), "event time order for %s/%s vs %s/%s", prevEvent.Code(), prevEvent.Phase(), event.Code(), event.Phase())
// 		}
// 		if event.Phase() == prevEvent.Phase() {
// 			require.Equal(t, event.PhaseStartTime(), prevEvent.PhaseStartTime(), "same phase start time for %s in %s vs %s", event.Phase(), prevEvent.Code(), event.Code())
// 		}
// 	}
// }

func VerifyContainsCollectedEvent(t *testing.T, afterStart time.Duration, expectedList []types.RetrievalEvent, actual types.RetrievalEvent) types.EventCode {
	for _, expected := range expectedList {
		if actual.Code() == expected.Code() &&
			actual.RetrievalId() == expected.RetrievalId() {

			actualCidEvent, acidOk := actual.(events.EventWithPayloadCid)
			expectedCidEvent, ecidOk := expected.(events.EventWithPayloadCid)
			haveMatchingCids := acidOk && ecidOk && expectedCidEvent.PayloadCid() == actualCidEvent.PayloadCid()
			haveNoCids := !acidOk && !ecidOk

			actualSpEvent, aspOk := actual.(events.EventWithSPID)
			expectedSpEvent, espOk := expected.(events.EventWithSPID)
			haveMatchingSpIDs := aspOk && espOk && expectedSpEvent.StorageProviderId() == actualSpEvent.StorageProviderId()
			haveNoSpIDs := !aspOk && !espOk

			if (haveMatchingSpIDs || haveNoSpIDs) && (haveMatchingCids || haveNoCids) {
				VerifyCollectedEvent(t, actual, expected)
				return actual.Code()
			}
		}
	}
	require.Failf(t, "unexpected event", "got '%s' @ %s", actual.Code(), afterStart)
	return ""
}

func VerifyCollectedEvent(t *testing.T, actual types.RetrievalEvent, expected types.RetrievalEvent) {
	require.Equal(t, expected.Code(), actual.Code(), "event code")
	require.Equal(t, expected.RetrievalId(), actual.RetrievalId(), fmt.Sprintf("retrieval id for %s", expected.Code()))
	require.Equal(t, expected.Time(), actual.Time(), fmt.Sprintf("time for %s", expected.Code()))

	if acid, ok := actual.(events.EventWithPayloadCid); ok {
		ecid, ok := expected.(events.EventWithPayloadCid)
		if !ok {
			require.Fail(t, fmt.Sprintf("expected event %s should be EventWithPayloadCid", expected.Code()))
		}
		require.Equal(t, ecid.PayloadCid().String(), acid.PayloadCid().String(), fmt.Sprintf("cid for %s", expected.Code()))
	}
	if asp, ok := actual.(events.EventWithSPID); ok {
		esp, ok := expected.(events.EventWithSPID)
		if !ok {
			require.Fail(t, fmt.Sprintf("expected event %s should be EventWithSPID", expected.Code()))
		}
		require.Equal(t, esp.StorageProviderId().String(), asp.StorageProviderId().String(), fmt.Sprintf("storage provider id for %s", expected.Code()))
	}
	if ec, ok := expected.(events.EventWithCandidates); ok {
		if ac, ok := actual.(events.EventWithCandidates); ok {
			require.Len(t, ac.Candidates(), len(ec.Candidates()), fmt.Sprintf("candidate length for %s", expected.Code()))
			for ii, expectedCandidate := range ec.Candidates() {
				var found bool
				for _, actualCandidate := range ac.Candidates() {
					if expectedCandidate.MinerPeer.ID == actualCandidate.MinerPeer.ID &&
						expectedCandidate.RootCid == actualCandidate.RootCid {
						found = true
						break
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
	if efb, ok := expected.(events.FirstByteEvent); ok {
		if afb, ok := actual.(events.FirstByteEvent); ok {
			require.Equal(t, efb.Duration(), afb.Duration(), fmt.Sprintf("duration for %s", expected.Code()))
		} else {
			require.Fail(t, "wrong event type", expected.Code())
		}
	}
	if efail, ok := expected.(events.FailedEvent); ok {
		if afail, ok := actual.(events.FailedEvent); ok {
			require.Equal(t, efail.ErrorMessage(), afail.ErrorMessage(), fmt.Sprintf("error message for %s", expected.Code()))
		} else {
			require.Fail(t, "wrong event type", expected.Code())
		}
	}
	if esuccess, ok := expected.(events.SucceededEvent); ok {
		if asuccess, ok := actual.(events.SucceededEvent); ok {
			require.Equal(t, esuccess.ReceivedBytesSize(), asuccess.ReceivedBytesSize(), fmt.Sprintf("received size for %s", expected.Code()))
			require.Equal(t, esuccess.ReceivedCidsCount(), asuccess.ReceivedCidsCount(), fmt.Sprintf("received cids for %s", expected.Code()))
			require.Equal(t, esuccess.Duration(), asuccess.Duration(), fmt.Sprintf("duration for %s", expected.Code()))
		} else {
			require.Fail(t, "wrong event type", expected.Code())
		}
	}
}
