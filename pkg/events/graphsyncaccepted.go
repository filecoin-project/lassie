package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
)

var (
	_ types.RetrievalEvent = GraphsyncAcceptedEvent{}
	_ EventWithProviderID  = GraphsyncAcceptedEvent{}
)

type GraphsyncAcceptedEvent struct {
	providerRetrievalEvent
}

func (e GraphsyncAcceptedEvent) Code() types.EventCode { return types.AcceptedCode }
func (e GraphsyncAcceptedEvent) String() string {
	return fmt.Sprintf("GraphsyncAcceptedEvent<%s, %s, %s, %s>", e.eventTime, e.retrievalId, e.payloadCid, e.providerId)
}

func Accepted(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate) GraphsyncAcceptedEvent {
	return GraphsyncAcceptedEvent{providerRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}}
}
