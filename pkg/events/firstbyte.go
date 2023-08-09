package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/multiformats/go-multicodec"
)

var (
	_ types.RetrievalEvent = FirstByteEvent{}
	_ EventWithProviderID  = FirstByteEvent{}
	_ EventWithProtocol    = FirstByteEvent{}
)

type FirstByteEvent struct {
	providerRetrievalEvent
	duration time.Duration
	protocol multicodec.Code
}

func (e FirstByteEvent) Code() types.EventCode     { return types.FirstByteCode }
func (e FirstByteEvent) Duration() time.Duration   { return e.duration }
func (e FirstByteEvent) Protocol() multicodec.Code { return e.protocol }
func (e FirstByteEvent) String() string {
	return fmt.Sprintf("FirstByteEvent<%s, %s, %s, %s, %s, %s>", e.eventTime, e.retrievalId, e.payloadCid, e.providerId, e.duration.String(), e.protocol.String())
}

func FirstByte(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate, duration time.Duration, protocol multicodec.Code) FirstByteEvent {
	return FirstByteEvent{providerRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}, duration, protocol}
}
