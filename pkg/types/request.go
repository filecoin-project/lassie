package types

import (
	"github.com/filecoin-project/lassie/pkg/retriever/selectorutils"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
)

type ReadableWritableStorage interface {
	storage.ReadableStorage
	storage.WritableStorage
}

type RetrievalID uuid.UUID

func NewRetrievalID() (RetrievalID, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return RetrievalID{}, err
	}
	return RetrievalID(u), nil
}

func (id RetrievalID) String() string {
	return uuid.UUID(id).String()
}

func (id RetrievalID) MarshalText() ([]byte, error) {
	return uuid.UUID(id).MarshalText()
}

func (id *RetrievalID) UnmarshalText(data []byte) error {
	return (*uuid.UUID)(id).UnmarshalText(data)
}

// RetrievalRequest is the top level parameters for a request --
// this should be left unchanged as you move down a retriever tree
type RetrievalRequest struct {
	RetrievalID RetrievalID
	Cid         cid.Cid
	LinkSystem  ipld.LinkSystem
	Selector    ipld.Node
}

// NewRequestForPath creates a new RetrievalRequest from the provided parameters
// and assigns a new RetrievalID to it.
//
// The LinkSystem is configured to use the provided store for both reading
// and writing and it is explicitly set to be trusted (i.e. it will not
// check CIDs match bytes). If the storage is not truested,
// request.LinkSystem.TrustedStore should be set to false after this call.
func NewRequestForPath(store ReadableWritableStorage, cid cid.Cid, path string, full bool) (RetrievalRequest, error) {
	retrievalId, err := NewRetrievalID()
	if err != nil {
		return RetrievalRequest{}, err
	}

	// Turn the path into a selector
	selector, err := selectorutils.UnixfsPathToSelector(path, full)
	if err != nil {
		return RetrievalRequest{}, err
	}

	linkSystem := cidlink.DefaultLinkSystem()
	linkSystem.SetReadStorage(store)
	linkSystem.SetWriteStorage(store)
	linkSystem.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&linkSystem)

	return RetrievalRequest{
		RetrievalID: retrievalId,
		Cid:         cid,
		Selector:    selector,
		LinkSystem:  linkSystem,
	}, nil
}

// GetSelector will safely return a selector for this request. If none has been
// set, it will return explore-all (full DAG).
func (r RetrievalRequest) GetSelector() ipld.Node {
	if r.Selector != nil {
		return r.Selector
	}
	return selectorparse.CommonSelector_ExploreAllRecursively
}
