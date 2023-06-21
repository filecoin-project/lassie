package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
)

var (
	_ types.RetrievalEvent = ConnectedToProviderEvent{}
	_ EventWithProviderID  = ConnectedToProviderEvent{}
)

type ConnectedToProviderEvent struct {
	providerRetrievalEvent
}

func (e ConnectedToProviderEvent) Code() types.EventCode { return types.ConnectedToProviderCode }
func (e ConnectedToProviderEvent) String() string {
	return fmt.Sprintf("ConnectedToProviderEvent<%s, %s, %s, %s>", e.eventTime, e.retrievalId, e.payloadCid, e.providerId)
}

func ConnectedToProvider(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate) ConnectedToProviderEvent {
	return ConnectedToProviderEvent{providerRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}}
}
