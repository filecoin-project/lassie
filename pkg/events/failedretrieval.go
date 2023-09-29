package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/multiformats/go-multicodec"
)

var (
	_ types.RetrievalEvent  = FailedRetrievalEvent{}
	_ EventWithProviderID   = FailedRetrievalEvent{}
	_ EventWithErrorMessage = FailedRetrievalEvent{}
)

type FailedRetrievalEvent struct {
	providerRetrievalEvent
	protocol     multicodec.Code
	errorMessage string
}

func (e FailedRetrievalEvent) Code() types.EventCode     { return types.FailedRetrievalCode }
func (e FailedRetrievalEvent) ErrorMessage() string      { return e.errorMessage }
func (e FailedRetrievalEvent) Protocol() multicodec.Code { return e.protocol }
func (e FailedRetrievalEvent) String() string {
	return fmt.Sprintf("FailedRetrievalEvent<%s, %s, %s, %v>", e.eventTime, e.retrievalId, e.rootCid, e.errorMessage)
}

func FailedRetrieval(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate, protocol multicodec.Code, errorMessage string) FailedRetrievalEvent {
	return FailedRetrievalEvent{providerRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}, protocol, errorMessage}
}
