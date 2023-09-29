package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/multiformats/go-multicodec"
)

var (
	_ types.RetrievalEvent = GraphsyncProposedEvent{}
	_ EventWithProviderID  = GraphsyncProposedEvent{}
)

type GraphsyncProposedEvent struct {
	providerRetrievalEvent
}

func (e GraphsyncProposedEvent) Code() types.EventCode { return types.ProposedCode }
func (e GraphsyncProposedEvent) Protocol() multicodec.Code {
	return multicodec.TransportGraphsyncFilecoinv1
}
func (e GraphsyncProposedEvent) String() string {
	return fmt.Sprintf("GraphsyncProposedEvent<%s, %s, %s, %s>", e.eventTime, e.retrievalId, e.rootCid, e.providerId)
}

func Proposed(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate) GraphsyncProposedEvent {
	return GraphsyncProposedEvent{providerRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}}
}
