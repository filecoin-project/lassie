package types

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	ipldstorage "github.com/ipld/go-ipld-prime/storage"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
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

// RetrievalRequest describes the parameters of a request. It is intended to be
// immutable.
type RetrievalRequest struct {
	// RetrievalID is a unique identifier for this request.
	RetrievalID RetrievalID

	// Cid is the root CID to fetch.
	Cid cid.Cid

	// LinkSystem is the destination for the blocks to fetch, it may be
	// pre-populated with existing blocks in the DAG, in which case they may
	// be used to satisfy the request (except in the case of an HTTP retrieval,
	// which will fetch the entire DAG, regardless).
	LinkSystem ipld.LinkSystem

	// Selector is the IPLD selector to use when fetching the DAG. If nil, the
	// Path and Scope will be used to generate a selector.
	Selector ipld.Node

	// Path is the optional path within the DAG to fetch.
	Path string

	// Scope describes the scope of the DAG to fetch. If the Selector parameter
	// is not set, Scope and Path will be used to construct a selector.
	Scope DagScope

	// Bytes is the optional byte range within the DAG to fetch. If not set
	// the default byte range will fetch the entire file.
	Bytes *ByteRange

	// Duplicates is a flag that indicates whether duplicate blocks should be
	// stored into the LinkSystem where they occur in the traversal.
	Duplicates bool

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

// NewRequestForPath creates a new RetrievalRequest from the provided parameters
// and assigns a new RetrievalID to it.
//
// The LinkSystem is configured to use the provided store for both reading
// and writing and it is explicitly set to be trusted (i.e. it will not
// check CIDs match bytes). If the storage is not truested,
// request.LinkSystem.TrustedStore should be set to false after this call.
func NewRequestForPath(store ipldstorage.WritableStorage, cid cid.Cid, path string, dagScope DagScope) (RetrievalRequest, error) {
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
		Scope:       dagScope,
		LinkSystem:  linkSystem,
	}, nil
}

// PathScopeSelector generates a selector for the given path, scope and byte
// range. Use DefaultByteRange() for the default byte range value if none is
// specified.
func PathScopeSelector(path string, scope DagScope, bytes ByteRange) ipld.Node {
	// Turn the path / scope into a selector
	terminal := scope.TerminalSelectorSpec()
	if !bytes.IsDefault() {
		ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
		to := bytes.To
		if to != math.MaxInt64 {
			to++ // selector is exclusive, so increment the end
		}
		terminal = ssb.ExploreInterpretAs("unixfs", ssb.MatcherSubset(bytes.From, to))
	}
	return unixfsnode.UnixFSPathSelectorBuilder(path, terminal, false)
}

// GetSelector will safely return a selector for this request. If none has been
// set, it will generate one for the path & scope.
func (r RetrievalRequest) GetSelector() ipld.Node {
	if r.Selector != nil { // custom selector
		return r.Selector
	}
	br := DefaultByteRange()
	if r.Bytes != nil {
		br = *r.Bytes
	}
	return PathScopeSelector(r.Path, r.Scope, br)
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
		scope = DagScopeAll
	}
	// TODO: remove once relevant endpoints support dag-scope
	legacyScope := string(scope)
	if legacyScope == string(DagScopeEntity) {
		legacyScope = "file"
	}
	path := r.Path
	if path != "" {
		path = "/" + path
	}
	return fmt.Sprintf("%s?dag-scope=%s&car-scope=%s", path, scope, legacyScope), nil
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

func (r RetrievalRequest) Etag() string {
	// https://github.com/ipfs/boxo/pull/303/commits/f61f95481041406df46a1781b1daab34b6605650#r1213918777
	sb := strings.Builder{}
	sb.WriteString("/ipfs/")
	sb.WriteString(r.Cid.String())
	if r.Path != "" {
		sb.WriteString("/")
		sb.WriteString(datamodel.ParsePath(r.Path).String())
	}
	if r.Scope != DagScopeAll {
		sb.WriteString(".")
		sb.WriteString(string(r.Scope))
	}
	if r.Duplicates {
		sb.WriteString(".dups")
	}
	sb.WriteString(".dfs")
	// range bytes would go here: `.from.to`
	suffix := strconv.FormatUint(xxhash.Sum64([]byte(sb.String())), 32)
	return `"` + r.Cid.String() + ".car." + suffix + `"`
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
		providerAddrInfo, err := peer.AddrInfoFromString(v)
		if err != nil {
			return nil, err
		}
		providerAddrInfos = append(providerAddrInfos, *providerAddrInfo)
	}
	return providerAddrInfos, nil
}
