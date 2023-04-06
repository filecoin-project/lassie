package types

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	ipldstorage "github.com/ipld/go-ipld-prime/storage"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/multiformats/go-multicodec"
)

type ReadableWritableStorage interface {
	ipldstorage.ReadableStorage
	ipldstorage.WritableStorage
	ipldstorage.StreamingReadableStorage
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
	RetrievalID       RetrievalID
	Cid               cid.Cid
	LinkSystem        ipld.LinkSystem
	Selector          ipld.Node
	Protocols         []multicodec.Code
	PreloadLinkSystem ipld.LinkSystem
}

// NewRequestForPath creates a new RetrievalRequest from the provided parameters
// and assigns a new RetrievalID to it.
//
// The LinkSystem is configured to use the provided store for both reading
// and writing and it is explicitly set to be trusted (i.e. it will not
// check CIDs match bytes). If the storage is not truested,
// request.LinkSystem.TrustedStore should be set to false after this call.
func NewRequestForPath(store ipldstorage.WritableStorage, cid cid.Cid, path string, carScope CarScope) (RetrievalRequest, error) {
	retrievalId, err := NewRetrievalID()
	if err != nil {
		return RetrievalRequest{}, err
	}

	// Turn the path into a selector

	targetSelector := unixfsnode.ExploreAllRecursivelySelector // all
	switch carScope {
	case CarScopeAll:
	case CarScopeFile:
		targetSelector = unixfsnode.MatchUnixFSPreloadSelector // file
	case CarScopeRoot:
		ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
		targetSelector = ssb.Matcher() // root
	}

	selector := unixfsnode.UnixFSPathSelectorBuilder(path, targetSelector, false)

	linkSystem := cidlink.DefaultLinkSystem()
	linkSystem.SetWriteStorage(store)
	if read, ok := store.(ipldstorage.ReadableStorage); ok {
		linkSystem.SetReadStorage(read)
	}
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

// GetSupportedProtocols will safely return the supported protocols for a specific request.
// It takes a list of all supported protocols, and
// -- if the request has protocols, it will return all the request protocols that are in the supported list
// -- if the request has no protocols, it will return the entire supported protocol list
func (r RetrievalRequest) GetSupportedProtocols(allSupportedProtocols []multicodec.Code) []multicodec.Code {
	if len(r.Protocols) == 0 {
		return allSupportedProtocols
	}
	supportedProtocols := make([]multicodec.Code, 0, len(r.Protocols))
	for _, protocol := range r.Protocols {
		for _, supportedProtocol := range allSupportedProtocols {
			if protocol == supportedProtocol {
				supportedProtocols = append(supportedProtocols, protocol)
				break
			}
		}
	}
	return supportedProtocols
}

func ParseProtocolsString(v string) ([]multicodec.Code, error) {
	vs := strings.Split(v, ",")
	protocols := make([]multicodec.Code, 0, len(vs))
	for _, v := range vs {
		var protocol multicodec.Code
		switch v {
		case "bitswap":
			protocol = multicodec.TransportBitswap
		case "graphsync":
			protocol = multicodec.TransportGraphsyncFilecoinv1
		default:
			return nil, fmt.Errorf("unrecognized protocol: %s", v)
		}
		protocols = append(protocols, protocol)
	}
	return protocols, nil
}
