package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/multiformats/go-multicodec"
)

var (
	_ types.RetrievalEvent = FirstByteEvent{}
	_ EventWithSPID        = FirstByteEvent{}
	_ EventWithProtocol    = FirstByteEvent{}
)

type FirstByteEvent struct {
	spRetrievalEvent
	duration time.Duration
	protocol multicodec.Code // TODO: remove when we can get the storage provider id from bitswap. This is required
}

func (e FirstByteEvent) Code() types.EventCode     { return types.FirstByteCode }
func (e FirstByteEvent) Duration() time.Duration   { return e.duration }
func (e FirstByteEvent) Protocol() multicodec.Code { return e.protocol }
func (e FirstByteEvent) String() string {
	return fmt.Sprintf("FirstByteEvent<%s, %s, %s, %s, %s, %s>", e.eventTime, e.retrievalId, e.payloadCid, e.storageProviderId, e.duration.String(), e.protocol.String())
}

func FirstByte(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate, duration time.Duration, protocol multicodec.Code) FirstByteEvent {
	return FirstByteEvent{spRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}, duration, protocol}
}
