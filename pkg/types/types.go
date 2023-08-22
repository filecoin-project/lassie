package types

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/ipni/go-libipni/maurl"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type Fetcher interface {
	Fetch(context.Context, RetrievalRequest, func(RetrievalEvent)) (*RetrievalStats, error)
}

type RetrievalCandidate struct {
	MinerPeer peer.AddrInfo
	RootCid   cid.Cid
	Metadata  metadata.Metadata
}

func NewRetrievalCandidate(pid peer.ID, addrs []multiaddr.Multiaddr, rootCid cid.Cid, protocols ...metadata.Protocol) RetrievalCandidate {
	md := metadata.Default.New(protocols...)
	return RetrievalCandidate{
		MinerPeer: peer.AddrInfo{ID: pid, Addrs: addrs},
		RootCid:   rootCid,
		Metadata:  md,
	}
}

// ToURL generates a valid HTTP URL from the candidate if possible
func (rc RetrievalCandidate) ToURL() (*url.URL, error) {
	var err error
	var url *url.URL
	for _, addr := range rc.MinerPeer.Addrs {
		url, err = maurl.ToURL(addr)
		if err == nil && url != nil {
			return url, nil
		}
	}
	if err == nil && url == nil {
		return nil, errors.New("no valid multiaddrs")
	}
	// we have to do this because we get ws and wss from maurl.ToURL
	if url != nil && !(url.Scheme == "http" || url.Scheme == "https") {
		return nil, errors.New("no valid HTTP multiaddrs")
	}
	return url, err
}

// retrieval task is any task that can be run to produce a result
type RetrievalTask interface {
	Run() (*RetrievalStats, error)
}

type Retriever interface {
	Retrieve(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) (*RetrievalStats, error)
}

type CandidateFinder interface {
	FindCandidates(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent), onCandidates func([]RetrievalCandidate)) error
}

func MakeAsyncCandidates(buffer int) (InboundAsyncCandidates, OutboundAsyncCandidates) {
	asyncCandidates := make(chan []RetrievalCandidate, buffer)
	return asyncCandidates, asyncCandidates
}

type InboundAsyncCandidates <-chan []RetrievalCandidate

func (ias InboundAsyncCandidates) Next(ctx context.Context) (bool, []RetrievalCandidate, error) {
	// prioritize cancelled context
	select {
	case <-ctx.Done():
		return false, nil, ctx.Err()
	default:
	}
	select {
	case <-ctx.Done():
		return false, nil, ctx.Err()
	case next, ok := <-ias:
		return ok, next, nil
	}
}

type OutboundAsyncCandidates chan<- []RetrievalCandidate

func (oas OutboundAsyncCandidates) SendNext(ctx context.Context, next []RetrievalCandidate) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case oas <- next:
		return nil
	}
}

type CandidateRetrieval interface {
	RetrieveFromAsyncCandidates(asyncCandidates InboundAsyncCandidates) (*RetrievalStats, error)
}

type CandidateRetriever interface {
	Retrieve(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) CandidateRetrieval
}

type RetrievalSplitter[T comparable] interface {
	SplitCandidates([]RetrievalCandidate) (map[T][]RetrievalCandidate, error)
}

type CandidateSplitter[T comparable] interface {
	SplitRetrievalRequest(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) RetrievalSplitter[T]
}

type AsyncRetrievalSplitter[T comparable] interface {
	SplitAsyncCandidates(asyncCandidates InboundAsyncCandidates) (map[T]InboundAsyncCandidates, <-chan error)
}

type AsyncCandidateSplitter[T comparable] interface {
	SplitRetrievalRequest(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) AsyncRetrievalSplitter[T]
}

type RetrievalStats struct {
	StorageProviderId peer.ID
	RootCid           cid.Cid
	Size              uint64
	Blocks            uint64
	Duration          time.Duration
	AverageSpeed      uint64
	TotalPayment      abi.TokenAmount
	NumPayments       int
	AskPrice          abi.TokenAmount
	TimeToFirstByte   time.Duration
	Selector          string
	CarProperties     *CarProperties // only set if CAR file is fetched via http and the provider provides the metadata
}

// CarProperties is supplied by the storage provider during HTTP fetches and
// provides metadata about the CAR file.
type CarProperties struct {
	CarBytes          int64
	DataBytes         int64
	BlockCount        int64
	ChecksumMultihash []byte
}

func (cp CarProperties) String() string {
	return fmt.Sprintf("CarProperties<car bytes: %d, data bytes: %d, block count: %d, checksum: %x>", cp.CarBytes, cp.DataBytes, cp.BlockCount, cp.ChecksumMultihash)
}

type RetrievalResult struct {
	Stats *RetrievalStats
	Err   error
}

var _ RetrievalTask = AsyncRetrievalTask{}

// AsyncRetrievalTask runs an asynchronous retrieval and returns a result
type AsyncRetrievalTask struct {
	Candidates              InboundAsyncCandidates
	AsyncCandidateRetrieval CandidateRetrieval
}

// Run executes the asychronous retrieval task
func (art AsyncRetrievalTask) Run() (*RetrievalStats, error) {
	return art.AsyncCandidateRetrieval.RetrieveFromAsyncCandidates(art.Candidates)
}

var _ RetrievalTask = DeferredErrorTask{}

// DeferredErrorTask simply reads from an error channel and returns the result as an error
type DeferredErrorTask struct {
	Ctx     context.Context
	ErrChan <-chan error
}

// Run reads the error channel and returns a result
func (det DeferredErrorTask) Run() (*RetrievalStats, error) {
	select {
	case <-det.Ctx.Done():
		return nil, det.Ctx.Err()
	case err := <-det.ErrChan:
		return nil, err
	}
}

type QueueRetrievalsFn func(ctx context.Context, nextRetrievalCall func(RetrievalTask))

type RetrievalCoordinator func(context.Context, QueueRetrievalsFn) (*RetrievalStats, error)

type CoordinationKind string

const (
	RaceCoordination       = "race"
	SequentialCoordination = "sequential"
)

type EventCode string

const (
	CandidatesFoundCode          EventCode = "candidates-found"
	CandidatesFilteredCode       EventCode = "candidates-filtered"
	StartedCode                  EventCode = "started"
	StartedFetchCode             EventCode = "started-fetch"
	StartedFindingCandidatesCode EventCode = "started-finding-candidates"
	StartedRetrievalCode         EventCode = "started-retrieval"
	ConnectedToProviderCode      EventCode = "connected-to-provider"
	QueryAskedCode               EventCode = "query-asked"
	QueryAskedFilteredCode       EventCode = "query-asked-filtered"
	ProposedCode                 EventCode = "proposed"
	AcceptedCode                 EventCode = "accepted"
	FirstByteCode                EventCode = "first-byte-received"
	FailedCode                   EventCode = "failed"
	FailedRetrievalCode          EventCode = "failed-retrieval"
	SuccessCode                  EventCode = "success"
	FinishedCode                 EventCode = "finished"
)

type RetrievalEvent interface {
	fmt.Stringer

	// Time returns the time that the event occurred
	Time() time.Time
	// RetrievalId returns the unique ID for this retrieval
	RetrievalId() RetrievalID
	// Code returns the type of event this is
	Code() EventCode
	// PayloadCid returns the CID being requested
	PayloadCid() cid.Cid
}

const BitswapIndentifier = "Bitswap"

// RetrievalEventSubscriber is a function that receives a stream of retrieval
// events from all retrievals that are in progress. Various different types
// implement the RetrievalEvent interface and may contain additional information
// about the event beyond what is available on the RetrievalEvent interface.
type RetrievalEventSubscriber func(event RetrievalEvent)

type FindCandidatesResult struct {
	Candidate RetrievalCandidate
	Err       error
}

type contextKey string

const retrievalIDKey = contextKey("retrieval-id-key")

func RegisterRetrievalIDToContext(parentCtx context.Context, id RetrievalID) context.Context {
	ctx := context.WithValue(parentCtx, retrievalIDKey, id)
	return ctx
}

// ErrMissingContextKey indicates no retrieval context key was present for a given context
var ErrMissingContextKey = errors.New("context key for retrieval is missing")

// ErrIncorrectContextValue indicates a value for the retrieval id context key that wasn't a retrieval id
var ErrIncorrectContextValue = errors.New("context key does not point to a valid retrieval id")

func RetrievalIDFromContext(ctx context.Context) (RetrievalID, error) {
	sk := ctx.Value(retrievalIDKey)
	if sk == nil {
		return RetrievalID{}, ErrMissingContextKey
	}
	id, ok := sk.(RetrievalID)
	if !ok {
		return RetrievalID{}, ErrIncorrectContextValue
	}
	return id, nil
}

type DagScope string

const DagScopeAll DagScope = "all"
const DagScopeEntity DagScope = "entity"
const DagScopeBlock DagScope = "block"

var matcherSelector = builder.NewSelectorSpecBuilder(basicnode.Prototype.Any).Matcher()

func (ds DagScope) TerminalSelectorSpec() builder.SelectorSpec {
	switch ds {
	case DagScopeAll:
		return unixfsnode.ExploreAllRecursivelySelector
	case DagScopeEntity:
		return unixfsnode.MatchUnixFSPreloadSelector // file
	case DagScopeBlock:
		return matcherSelector
	case DagScope(""):
		return unixfsnode.ExploreAllRecursivelySelector // default to explore-all for zero-value DagScope
	}
	panic(fmt.Sprintf("unknown DagScope: [%s]", string(ds)))
}
