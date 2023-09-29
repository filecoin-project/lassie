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
	rootCid     cid.Cid
}

func (r retrievalEvent) Time() time.Time                { return r.eventTime }
func (r retrievalEvent) RetrievalId() types.RetrievalID { return r.retrievalId }
func (r retrievalEvent) RootCid() cid.Cid               { return r.rootCid }

type providerRetrievalEvent struct {
	retrievalEvent
	providerId peer.ID
}

func (e providerRetrievalEvent) ProviderId() peer.ID { return e.providerId }

type EventWithProviderID interface {
	types.RetrievalEvent
	ProviderId() peer.ID
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
