package session

import (
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
)

var _ State = nilstate{}

type nilstate struct{}

func (ns nilstate) RecordFailure(retrievalId types.RetrievalID, storageProviderId peer.ID) error {
	return nil
}

func (ns nilstate) RecordSuccess(storageProviderId peer.ID) {}

func (ns nilstate) RemoveFromRetrieval(retrievalId types.RetrievalID, storageProviderId peer.ID) error {
	return nil
}

func (ns nilstate) GetConcurrency(storageProviderId peer.ID) uint {
	return 0
}

func (ns nilstate) AddToRetrieval(retrievalId types.RetrievalID, storageProviderIds []peer.ID) error {
	return nil
}

func (ns nilstate) EndRetrieval(retrievalId types.RetrievalID) error {
	return nil
}

func (ns nilstate) RegisterRetrieval(retrievalId types.RetrievalID, cid cid.Cid, selector datamodel.Node) bool {
	return true
}

func (ns nilstate) RecordConnectTime(storageProviderId peer.ID, connectTime time.Duration) {}

func (ns nilstate) ChooseNextProvider(peers []peer.ID, mda []metadata.Protocol) int {
	return 0
}
