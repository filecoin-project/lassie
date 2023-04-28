package types

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	ipldstorage "github.com/ipld/go-ipld-prime/storage"
	"github.com/libp2p/go-libp2p/core/peer"
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
	Path              string
	Scope             CarScope
	Protocols         []multicodec.Code
	PreloadLinkSystem ipld.LinkSystem
	MaxBlocks         uint64
	FixedPeers        []peer.AddrInfo
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
		Path:        path,
		Scope:       carScope,
		LinkSystem:  linkSystem,
	}, nil
}

// GetSelector will safely return a selector for this request. If none has been
// set, it will generate one for the path & scope.
func (r RetrievalRequest) GetSelector() ipld.Node {
	if r.Selector != nil { // custom selector
		return r.Selector
	}
	// Turn the path / scope into a selector
	return unixfsnode.UnixFSPathSelectorBuilder(r.Path, r.Scope.TerminalSelectorSpec(), false)
}

// GetUrlPath returns a URL path and query string valid with the Trusted HTTP
// Gateway spec by combining the Path and the Scope of this request. If this
// request uses an explicit Selector rather than a Path, an error will be
// returned.
func (r RetrievalRequest) GetUrlPath() (string, error) {
	if r.Selector != nil {
		return "", errors.New("RetrievalRequest uses an explicit selector, can't generate a URL path for it")
	}
	scope := r.Scope
	if r.Scope == "" {
		scope = CarScopeAll
	}
	return fmt.Sprintf("%s?car-scope=%s", r.Path, scope), nil
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
		case "http":
			protocol = multicodec.TransportIpfsGatewayHttp
		default:
			return nil, fmt.Errorf("unrecognized protocol: %s", v)
		}
		protocols = append(protocols, protocol)
	}
	return protocols, nil
}

func ParseProviderStrings(v string) ([]peer.AddrInfo, error) {
	vs := strings.Split(v, ",")
	providerAddrInfos := make([]peer.AddrInfo, 0, len(vs))

	for _, v := range vs {
		providerAddrInfo, err := peer.AddrInfoFromString(v)
		if err != nil {
			return nil, err
		}
		providerAddrInfos = append(providerAddrInfos, *providerAddrInfo)
	}
	return providerAddrInfos, nil
}
