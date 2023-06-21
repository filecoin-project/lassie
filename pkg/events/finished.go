package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
)

var (
	_ types.RetrievalEvent = FinishedEvent{}
	_ EventWithSPID        = FinishedEvent{}
)

type FinishedEvent struct {
	spRetrievalEvent
}

func (e FinishedEvent) Code() types.EventCode { return types.FinishedCode }
func (e FinishedEvent) String() string {
	return fmt.Sprintf("FinishedEvent<%s, %s, %s, %s>", e.eventTime, e.retrievalId, e.payloadCid, e.storageProviderId)
}

func Finished(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate) FinishedEvent {
	return FinishedEvent{spRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}}
}
