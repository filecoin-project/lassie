package session

import (
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/libp2p/go-libp2p/core/peer"
)

type nilstate struct {
}

func (ns *nilstate) RecordFailure(storageProviderId peer.ID, retrievalId types.RetrievalID) error {
	return nil
}

func (ns *nilstate) RemoveStorageProviderFromRetrieval(storageProviderId peer.ID, retrievalId types.RetrievalID) error {
	return nil
}

func (ns *nilstate) IsSuspended(storageProviderId peer.ID) bool {
	return false
}

func (ns *nilstate) GetConcurrency(storageProviderId peer.ID) uint {
	return 0
}

func (ns *nilstate) AddToRetrieval(retrievalId types.RetrievalID, storageProviderIds []peer.ID) error {
	return nil
}

func (ns *nilstate) EndRetrieval(retrievalId types.RetrievalID) error {
	return nil
}

func (ns *nilstate) RegisterRetrieval(retrievalId types.RetrievalID, cid cid.Cid, selector datamodel.Node) bool {
	return true
}
