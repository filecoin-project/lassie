package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/multiformats/go-multicodec"
)

var (
	_ types.RetrievalEvent = ConnectedToProviderEvent{}
	_ EventWithProviderID  = ConnectedToProviderEvent{}
)

type ConnectedToProviderEvent struct {
	providerRetrievalEvent
	protocol multicodec.Code
}

func (e ConnectedToProviderEvent) Code() types.EventCode     { return types.ConnectedToProviderCode }
func (e ConnectedToProviderEvent) Protocol() multicodec.Code { return e.protocol }
func (e ConnectedToProviderEvent) String() string {
	return fmt.Sprintf("ConnectedToProviderEvent<%s, %s, %s, %s>", e.eventTime, e.retrievalId, e.payloadCid, e.providerId)
}

func ConnectedToProvider(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate, protocol multicodec.Code) ConnectedToProviderEvent {
	return ConnectedToProviderEvent{providerRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}, protocol}
}
