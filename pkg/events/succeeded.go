package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/multiformats/go-multicodec"
)

var (
	_ types.RetrievalEvent = SucceededEvent{}
	_ EventWithProviderID  = SucceededEvent{}
	_ EventWithProtocol    = SucceededEvent{}
)

type SucceededEvent struct {
	providerRetrievalEvent
	receivedBytesSize uint64
	receivedCidsCount uint64
	duration          time.Duration
	protocol          multicodec.Code
}

func (e SucceededEvent) Code() types.EventCode     { return types.SuccessCode }
func (e SucceededEvent) ReceivedBytesSize() uint64 { return e.receivedBytesSize }
func (e SucceededEvent) ReceivedCidsCount() uint64 { return e.receivedCidsCount }
func (e SucceededEvent) Duration() time.Duration   { return e.duration }
func (e SucceededEvent) Protocol() multicodec.Code { return e.protocol }
func (e SucceededEvent) String() string {
	return fmt.Sprintf("SucceededEvent<%s, %s, %s, %s, { %d, %d, %s, %s }>", e.eventTime, e.retrievalId, e.rootCid, e.providerId, e.receivedBytesSize, e.receivedCidsCount, e.protocol, e.duration)
}

func Success(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate, receivedBytesSize uint64, receivedCidsCount uint64, duration time.Duration, protocol multicodec.Code) SucceededEvent {
	return SucceededEvent{providerRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}, receivedBytesSize, receivedCidsCount, duration, protocol}
}
