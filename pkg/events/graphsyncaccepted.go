package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
)

var (
	_ types.RetrievalEvent = GraphsyncAcceptedEvent{}
	_ EventWithSPID        = GraphsyncAcceptedEvent{}
)

type GraphsyncAcceptedEvent struct {
	spRetrievalEvent
}

func (e GraphsyncAcceptedEvent) Code() types.EventCode { return types.AcceptedCode }
func (e GraphsyncAcceptedEvent) String() string {
	return fmt.Sprintf("GraphsyncAcceptedEvent<%s, %s, %s>", e.eventTime, e.retrievalId, e.storageProviderId)
}

func Accepted(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate) GraphsyncAcceptedEvent {
	return GraphsyncAcceptedEvent{spRetrievalEvent{retrievalEvent{at, retrievalId}, candidate.MinerPeer.ID}}
}
