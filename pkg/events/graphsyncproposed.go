package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
)

var (
	_ types.RetrievalEvent = GraphsyncProposedEvent{}
	_ EventWithSPID        = GraphsyncProposedEvent{}
)

type GraphsyncProposedEvent struct {
	spRetrievalEvent
}

func (e GraphsyncProposedEvent) Code() types.EventCode { return types.ProposedCode }
func (e GraphsyncProposedEvent) String() string {
	return fmt.Sprintf("GraphsyncProposedEvent<%s, %s, %s, %s>", e.eventTime, e.retrievalId, e.payloadCid, e.storageProviderId)
}

func Proposed(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate) GraphsyncProposedEvent {
	return GraphsyncProposedEvent{spRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}}
}
