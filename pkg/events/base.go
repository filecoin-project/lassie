package events

import (
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

type retrievalEvent struct {
	eventTime   time.Time
	retrievalId types.RetrievalID
}

func (r retrievalEvent) Time() time.Time                { return r.eventTime }
func (r retrievalEvent) RetrievalId() types.RetrievalID { return r.retrievalId }

type spRetrievalEvent struct {
	retrievalEvent
	storageProviderId peer.ID
}

func (e spRetrievalEvent) StorageProviderId() peer.ID { return e.storageProviderId }

type EventWithPayloadCid interface {
	types.RetrievalEvent
	PayloadCid() cid.Cid
}

type EventWithSPID interface {
	types.RetrievalEvent
	StorageProviderId() peer.ID
}

type EventWithCandidates interface {
	types.RetrievalEvent
	Candidates() []types.RetrievalCandidate
}

type EventWithProtocol interface {
	types.RetrievalEvent
	Protocol() multicodec.Code
}

type EventWithProtocols interface {
	types.RetrievalEvent
	Protocols() []multicodec.Code
}

type EventWithErrorMessage interface {
	types.RetrievalEvent
	ErrorMessage() string
}
