package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
)

var (
	_ types.RetrievalEvent = ConnectedToSPEvent{}
	_ EventWithSPID        = ConnectedToSPEvent{}
)

type ConnectedToSPEvent struct {
	spRetrievalEvent
}

func (e ConnectedToSPEvent) Code() types.EventCode { return types.ConnectedToSPCode }
func (e ConnectedToSPEvent) String() string {
	return fmt.Sprintf("ConnectedToSPEvent<%s, %s, %s, %s>", e.eventTime, e.retrievalId, e.payloadCid, e.storageProviderId)
}

func Connected(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate) ConnectedToSPEvent {
	return ConnectedToSPEvent{spRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}}
}
