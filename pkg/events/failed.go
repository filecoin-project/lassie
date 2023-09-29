package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
)

var (
	_ types.RetrievalEvent  = FailedEvent{}
	_ EventWithErrorMessage = FailedEvent{}
)

type FailedEvent struct {
	retrievalEvent
	errorMessage string
}

func (e FailedEvent) Code() types.EventCode { return types.FailedCode }
func (e FailedEvent) ErrorMessage() string  { return e.errorMessage }
func (e FailedEvent) String() string {
	return fmt.Sprintf("FailedEvent<%s, %s, %s, %v>", e.eventTime, e.retrievalId, e.rootCid, e.errorMessage)
}

func Failed(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate, errorMessage string) FailedEvent {
	return FailedEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, errorMessage}
}
