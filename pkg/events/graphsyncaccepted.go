package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/multiformats/go-multicodec"
)

var (
	_ types.RetrievalEvent = GraphsyncAcceptedEvent{}
	_ EventWithProviderID  = GraphsyncAcceptedEvent{}
)

type GraphsyncAcceptedEvent struct {
	providerRetrievalEvent
}

func (e GraphsyncAcceptedEvent) Code() types.EventCode { return types.AcceptedCode }
func (e GraphsyncAcceptedEvent) Protocol() multicodec.Code {
	return multicodec.TransportGraphsyncFilecoinv1
}
func (e GraphsyncAcceptedEvent) String() string {
	return fmt.Sprintf("GraphsyncAcceptedEvent<%s, %s, %s, %s>", e.eventTime, e.retrievalId, e.rootCid, e.providerId)
}

func Accepted(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate) GraphsyncAcceptedEvent {
	return GraphsyncAcceptedEvent{providerRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}}
}
