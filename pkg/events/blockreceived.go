package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/multiformats/go-multicodec"
)

var (
	_ types.RetrievalEvent = BlockReceivedEvent{}
	_ EventWithProtocol    = BlockReceivedEvent{}
	_ EventWithProviderID  = BlockReceivedEvent{}
)

// BlockReceivedEvent records new data received from a provider. It is used to track how much is downloaded from each peer in a retrieval
type BlockReceivedEvent struct {
	providerRetrievalEvent
	protocol  multicodec.Code
	byteCount uint64
}

func (e BlockReceivedEvent) Code() types.EventCode     { return types.BlockReceivedCode }
func (e BlockReceivedEvent) ByteCount() uint64         { return e.byteCount }
func (e BlockReceivedEvent) Protocol() multicodec.Code { return e.protocol }
func (e BlockReceivedEvent) String() string {
	return fmt.Sprintf("BlockReceivedEvent<%s, %s, %s, %s, %s, %d>", e.eventTime, e.retrievalId, e.payloadCid, e.providerId, e.protocol, e.byteCount)
}

func BlockReceived(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate, protocol multicodec.Code, byteCount uint64) BlockReceivedEvent {
	return BlockReceivedEvent{providerRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}, protocol, byteCount}
}
