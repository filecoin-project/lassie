package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
)

var (
	_ types.RetrievalEvent = FinishedEvent{}
	_ EventWithProviderID  = FinishedEvent{}
)

type FinishedEvent struct {
	providerRetrievalEvent
}

func (e FinishedEvent) Code() types.EventCode { return types.FinishedCode }
func (e FinishedEvent) String() string {
	return fmt.Sprintf("FinishedEvent<%s, %s, %s, %s>", e.eventTime, e.retrievalId, e.payloadCid, e.providerId)
}

func Finished(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate) FinishedEvent {
	return FinishedEvent{providerRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}}
}
