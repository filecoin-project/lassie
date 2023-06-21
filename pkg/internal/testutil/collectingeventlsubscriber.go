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

func VerifyContainsCollectedEvent(t *testing.T, afterStart time.Duration, expectedList []types.RetrievalEvent, actual types.RetrievalEvent) types.EventCode {
	for _, expected := range expectedList {
		if actual.Code() == expected.Code() &&
			actual.RetrievalId() == expected.RetrievalId() {

			actualSpEvent, aspOk := actual.(events.EventWithProviderID)
			expectedSpEvent, espOk := expected.(events.EventWithProviderID)
			haveMatchingSpIDs := aspOk && espOk && expectedSpEvent.ProviderId() == actualSpEvent.ProviderId()
			haveNoSpIDs := !aspOk && !espOk

			if haveMatchingSpIDs || haveNoSpIDs {
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
	require.Equal(t, expected.PayloadCid(), actual.PayloadCid(), fmt.Sprintf("cid for %s", expected.Code()))
	require.Equal(t, expected.Time(), actual.Time(), fmt.Sprintf("time for %s", expected.Code()))

	if asp, ok := actual.(events.EventWithProviderID); ok {
		esp, ok := expected.(events.EventWithProviderID)
		if !ok {
			require.Fail(t, fmt.Sprintf("expected event %s should be EventWithSPID", expected.Code()))
		}
		require.Equal(t, esp.ProviderId().String(), asp.ProviderId().String(), fmt.Sprintf("storage provider id for %s", expected.Code()))
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
