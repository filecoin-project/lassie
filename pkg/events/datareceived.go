package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/multiformats/go-multicodec"
)

var (
	_ types.RetrievalEvent = DataReceivedEvent{}
	_ EventWithProtocol    = DataReceivedEvent{}
	_ EventWithProviderID  = DataReceivedEvent{}
)

// DataReceivedEvent records new data received from a provider. It is used to track how much is downloaded from each peer in a retrieval
type DataReceivedEvent struct {
	providerRetrievalEvent
	protocol  multicodec.Code
	byteCount uint64
}

func (e DataReceivedEvent) Code() types.EventCode     { return types.DataReceivedCode }
func (e DataReceivedEvent) ByteCount() uint64         { return e.byteCount }
func (e DataReceivedEvent) Protocol() multicodec.Code { return e.protocol }
func (e DataReceivedEvent) String() string {
	return fmt.Sprintf("DataReceivedEvent<%s, %s, %s, %s, %s, %d>", e.eventTime, e.retrievalId, e.payloadCid, e.providerId, e.protocol, e.byteCount)
}

func DataReceived(at time.Time, retrievalId types.RetrievalID, candidate types.RetrievalCandidate, protocol multicodec.Code, byteCount uint64) DataReceivedEvent {
	return DataReceivedEvent{providerRetrievalEvent{retrievalEvent{at, retrievalId, candidate.RootCid}, candidate.MinerPeer.ID}, protocol, byteCount}
}
