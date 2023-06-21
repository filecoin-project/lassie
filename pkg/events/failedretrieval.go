package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
)

var (
	_ types.RetrievalEvent  = FailedRetrievalEvent{}
	_ EventWithSPID         = FailedRetrievalEvent{}
	_ EventWithErrorMessage = FailedRetrievalEvent{}
)

type FailedRetrievalEvent struct {
	spRetrievalEvent
	errorMessage string
}

func (e FailedRetrievalEvent) Code() types.EventCode { return types.FailedRetrievalCode }
func (e FailedRetrievalEvent) ErrorMessage() string  { return e.errorMessage }
func (e FailedRetrievalEvent) String() string {
	return fmt.Sprintf("FailedRetrievalEvent<%s, %s, %s, %v>", e.eventTime, e.retrievalId, e.payloadCid, e.errorMessage)
}

func FailedRetrieval(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate, errorMessage string) FailedRetrievalEvent {
	return FailedRetrievalEvent{spRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}, errorMessage}
}
