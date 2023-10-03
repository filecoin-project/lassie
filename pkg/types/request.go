package types

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	ipldstorage "github.com/ipld/go-ipld-prime/storage"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/ipni/go-libipni/maurl"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
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

// RetrievalRequest describes the parameters of a request. It is intended to be
// immutable.
type RetrievalRequest struct {
	trustlessutils.Request

	// RetrievalID is a unique identifier for this request.
	RetrievalID RetrievalID

	// LinkSystem is the destination for the blocks to fetch, it may be
	// pre-populated with existing blocks in the DAG, in which case they may
	// be used to satisfy the request (except in the case of an HTTP retrieval,
	// which will fetch the entire DAG, regardless).
	LinkSystem ipld.LinkSystem

	// Selector is the IPLD selector to use when fetching the DAG. If nil, the
	// Path and Scope will be used to generate a selector.
	Selector ipld.Node

	// Protocols is an optional list of protocols to use when fetching the DAG.
	// If nil, the default protocols will be used.
	Protocols []multicodec.Code

	// PreloadLinkSystem must be setup to enable Bitswap preload behavior. This
	// LinkSystem must be thread-safe as multiple goroutines may be using it to
	// store and retrieve blocks concurrently.
	PreloadLinkSystem ipld.LinkSystem

	// MaxBlocks optionally specifies the maximum number of blocks to fetch.
	// If zero, no limit is applied.
	MaxBlocks uint64

	// FixedPeers optionally specifies a list of peers to use when fetching
	// blocks. If nil, the default peer discovery mechanism will be used.
	FixedPeers []peer.AddrInfo
}

// NewRequestForPath creates a new RetrievalRequest for the given root CID as
// the head of the graph to fetch, the path within that graph to fetch, the
// scope that dictates the depth of fetching withint he graph and the byte
// range to fetch if intending to fetch part of a large UnixFS file.
//
// The byteRange parameter should be left nil if this is not a request for a
// partial UnixFS file; and if it is set, the dagScope should be DagScopeEntity.
//
// The LinkSystem is configured to use the provided store for both reading
// and writing and it is explicitly set to be trusted (i.e. it will not
// check CIDs match bytes). If the storage is not truested,
// request.LinkSystem.TrustedStore should be set to false after this call.
func NewRequestForPath(
	store ipldstorage.WritableStorage,
	rootCid cid.Cid,
	path string,
	dagScope trustlessutils.DagScope,
	byteRange *trustlessutils.ByteRange,
) (RetrievalRequest, error) {

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
		Request: trustlessutils.Request{
			Root:       rootCid,
			Path:       path,
			Scope:      dagScope,
			Bytes:      byteRange,
			Duplicates: false,
		},
		RetrievalID: retrievalId,
		LinkSystem:  linkSystem,
	}, nil
}

// GetSelector will safely return a selector for this request. If none has been
// set, it will generate one for the path & scope.
func (r RetrievalRequest) GetSelector() ipld.Node {
	if r.Selector != nil { // custom selector
		return r.Selector
	}
	return r.Request.Selector()
}

// GetDescriptorString returns a URL and query string-style descriptor string
// for the request. This is different from GetUrlPath as it is not intended
// (nor safe) to use as an HTTP request. Instead, this should be used for
// logging and other descriptive purposes.
//
// If this request uses an explicit Selector rather than a Path, an error will
// be returned.
func (r RetrievalRequest) GetDescriptorString() (string, error) {
	if r.Selector != nil {
		return "", errors.New("RetrievalRequest uses an explicit selector, can't generate a descriptor path for it")
	}
	scope := r.Scope
	if r.Scope == "" {
		scope = trustlessutils.DagScopeAll
	}
	path := trustlessutils.PathEscape(r.Path)
	byteRange := ""
	if !r.Bytes.IsDefault() {
		byteRange = "&entity-bytes=" + r.Bytes.String()
	}
	dups := "y"
	if !r.Duplicates {
		dups = "n"
	}
	var blockLimit string
	if r.MaxBlocks > 0 {
		blockLimit = fmt.Sprintf("&blockLimit=%d", r.MaxBlocks)
	}
	var protocols string
	if len(r.Protocols) > 0 {
		var sb strings.Builder
		sb.WriteString("&protocols=")
		for i, protocol := range r.Protocols {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(protocol.String())
		}
		protocols = sb.String()
	}
	var providers string
	if len(r.FixedPeers) > 0 {
		ps, err := ToProviderString(r.FixedPeers)
		if err != nil {
			return "", err
		}
		providers = "&providers=" + ps
	}
	return fmt.Sprintf("/ipfs/%s%s?dag-scope=%s%s&dups=%s%s%s%s", r.Root.String(), path, scope, byteRange, dups, blockLimit, protocols, providers), nil

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

func (r RetrievalRequest) HasPreloadLinkSystem() bool {
	return r.PreloadLinkSystem.StorageReadOpener != nil && r.PreloadLinkSystem.StorageWriteOpener != nil
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
		var maddr ma.Multiaddr

		// http:// style provider has been specified, parse it as a URL and
		// transform into a multiaddr with made-up peer ID
		if strings.HasPrefix(v, "http://") || strings.HasPrefix(v, "https://") {
			u, err := url.Parse(v)
			if err != nil {
				return nil, err
			}
			if datamodel.ParsePath(u.Path).Len() != 0 {
				return nil, fmt.Errorf("invalid provider URL, paths not supported: %s", v)
			}
			u.Path = "" // just in case..
			maddr, err = maurl.FromURL(u)
			if err != nil {
				return nil, err
			}
		} else {
			var err error
			maddr, err = ma.NewMultiaddr(v)
			if err != nil {
				return nil, err
			}
		}
		transport, id := peer.SplitAddr(maddr)
		if transport == nil {
			return nil, fmt.Errorf("%w: missing transport", peer.ErrInvalidAddr)
		}
		if id == "" {
			for _, proto := range transport.Protocols() {
				if proto.Name == "http" || proto.Name == "https" {
					id = nextUnknownPeerID()
					break
				}
			}
			if id == "" {
				return nil, fmt.Errorf("%w: missing peer id", peer.ErrInvalidAddr)
			}
		}
		providerAddrInfo := &peer.AddrInfo{ID: id, Addrs: []ma.Multiaddr{transport}}
		providerAddrInfos = append(providerAddrInfos, *providerAddrInfo)
	}
	return providerAddrInfos, nil
}

// Make a new random, but valid peer ID for a provider we don't have an ID for
// (i.e. an HTTP provider the user has specified without a peer ID). Ideally
// it's human-identifiable as made-up, so we generate one that has "unknown"
// near the beginning of the string using a fixed prefix, with random trailing
// bytes.
//
// This is only useful where the peer ID doesn't matter, specifically HTTP
// retrievals where there is limited negotiation. But we still want a peer ID
// because it's assumed through much of Lassie.

const unknownPeerID = "\x00\x16v\xa5\x9c\xd4\"="

func nextUnknownPeerID() peer.ID {
	b := make([]byte, 16)
	rand.Read(b)
	return peer.ID(append([]byte(unknownPeerID), b...))
}

func IsUnknownPeerID(p peer.ID) bool {
	return p[0:len(unknownPeerID)] == unknownPeerID
}

func ToProviderString(ai []peer.AddrInfo) (string, error) {
	var sb strings.Builder
	for i, v := range ai {
		if i > 0 {
			sb.WriteString(",")
		}
		ma, err := peer.AddrInfoToP2pAddrs(&v)
		if err != nil {
			return "", err
		}
		sb.WriteString(ma[0].String())
	}
	return sb.String(), nil
}
