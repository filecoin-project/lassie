package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/multiformats/go-multicodec"
)

var (
	_ types.RetrievalEvent = StartedRetrievalEvent{}
	_ EventWithProviderID  = StartedRetrievalEvent{}
	_ EventWithProtocol    = StartedRetrievalEvent{}
)

// StartedRetrievalEvent signals the start of a retrieval from a storage provider. It is emitted when the retrieval is started.
type StartedRetrievalEvent struct {
	providerRetrievalEvent
	protocol multicodec.Code
}

func (e StartedRetrievalEvent) Code() types.EventCode     { return types.StartedRetrievalCode }
func (e StartedRetrievalEvent) Protocol() multicodec.Code { return e.protocol }
func (e StartedRetrievalEvent) String() string {
	return fmt.Sprintf("StartedRetrievalEvent<%s, %s, %s, %s, %s>", e.eventTime, e.retrievalId, e.payloadCid, e.providerId, e.protocol)
}

func StartedRetrieval(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate, protocol multicodec.Code) StartedRetrievalEvent {
	return StartedRetrievalEvent{providerRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}, protocol}
}
