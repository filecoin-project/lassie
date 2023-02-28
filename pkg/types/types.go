package types

import (
	"context"
	"errors"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	"github.com/ipni/index-provider/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

type RetrievalCandidate struct {
	MinerPeer peer.AddrInfo
	RootCid   cid.Cid
	Metadata  metadata.Metadata
}

func NewRetrievalCandidate(pid peer.ID, rootCid cid.Cid, protocols ...metadata.Protocol) RetrievalCandidate {
	md := metadata.Default.New(protocols...)
	return RetrievalCandidate{
		MinerPeer: peer.AddrInfo{ID: pid},
		RootCid:   rootCid,
		Metadata:  md,
	}
}

type Retrieval interface {
	Retrieve() (*RetrievalStats, error)
}

type Retriever interface {
	Retrieve(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) (*RetrievalStats, error)
}

type CandidateFinder interface {
	FindCandidates(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent), onCandidates func([]RetrievalCandidate)) error
}

type CandidateRetrieval interface {
	RetrieveFromCandidates([]RetrievalCandidate) (*RetrievalStats, error)
}

type CandidateRetriever interface {
	Retrieve(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) CandidateRetrieval
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

type AsyncCandidateRetrieval interface {
	RetrieveFromAsyncCandidates(asyncCandidates InboundAsyncCandidates) (*RetrievalStats, error)
}

type AsyncCandidateRetriever interface {
	Retrieve(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) AsyncCandidateRetrieval
}

type RetrievalSplitter[T comparable] interface {
	SplitCandidates([]RetrievalCandidate) (map[T][]RetrievalCandidate, error)
}

type CandidateSplitter[T comparable] interface {
	SplitRetrieval(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) RetrievalSplitter[T]
}

type AsyncRetrievalSplitter[T comparable] interface {
	SplitAsyncCandidates(asyncCandidates InboundAsyncCandidates) (map[T]InboundAsyncCandidates, <-chan error)
}

type AsyncCandidateSplitter[T comparable] interface {
	SplitRetrieval(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) AsyncRetrievalSplitter[T]
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
}

type RetrievalResult struct {
	Stats *RetrievalStats
	Err   error
}

type CandidateRetrievalCall struct {
	Candidates         []RetrievalCandidate
	CandidateRetrieval CandidateRetrieval
}

func (crc CandidateRetrievalCall) Retrieve() (*RetrievalStats, error) {
	return crc.CandidateRetrieval.RetrieveFromCandidates(crc.Candidates)
}

type AsyncCandidateRetrievalCall struct {
	Candidates              InboundAsyncCandidates
	AsyncCandidateRetrieval AsyncCandidateRetrieval
}

func (acrc AsyncCandidateRetrievalCall) Retrieve() (*RetrievalStats, error) {
	return acrc.AsyncCandidateRetrieval.RetrieveFromAsyncCandidates(acrc.Candidates)
}

type DeferredErrorRetrieval struct {
	Ctx     context.Context
	ErrChan <-chan error
}

func (der DeferredErrorRetrieval) Retrieve() (*RetrievalStats, error) {
	select {
	case <-der.Ctx.Done():
		return nil, der.Ctx.Err()
	case err := <-der.ErrChan:
		return nil, err
	}
}

type QueueRetrievalsFn func(ctx context.Context, nextRetrievalCall func(Retrieval))

type RetrievalCoordinator func(context.Context, QueueRetrievalsFn) (*RetrievalStats, error)

type CoordinationKind string

const (
	RaceCoordination       = "race"
	SequentialCoordination = "sequential"
	AsyncCoordination      = "async"
)

type Phase string

const (
	// IndexerPhase involves a candidates-found|failure
	IndexerPhase Phase = "indexer"
	// QueryPhase involves a connect, query-asked|failure
	QueryPhase Phase = "query"
	// RetrievalPhase involves the full data retrieval: connect, proposed, accepted, first-byte-received, success|failure
	RetrievalPhase Phase = "retrieval"
)

type EventCode string

const (
	CandidatesFoundCode    EventCode = "candidates-found"
	CandidatesFilteredCode EventCode = "candidates-filtered"
	StartedCode            EventCode = "started"
	ConnectedCode          EventCode = "connected"
	QueryAskedCode         EventCode = "query-asked"
	QueryAskedFilteredCode EventCode = "query-asked-filtered"
	ProposedCode           EventCode = "proposed"
	AcceptedCode           EventCode = "accepted"
	FirstByteCode          EventCode = "first-byte-received"
	FailedCode             EventCode = "failure"
	SuccessCode            EventCode = "success"
)

type RetrievalEvent interface {
	// Time returns the time that the event occurred
	Time() time.Time
	// RetrievalId returns the unique ID for this retrieval
	RetrievalId() RetrievalID
	// Code returns the type of event this is
	Code() EventCode
	// Phase returns what phase of a retrieval this even occurred on
	Phase() Phase
	// PhaseStartTime returns the time that the phase started for this storage provider
	PhaseStartTime() time.Time
	// PayloadCid returns the CID being requested
	PayloadCid() cid.Cid
	// StorageProviderId returns the peer ID of the storage provider if this
	// retrieval was requested via peer ID
	StorageProviderId() peer.ID
	// Protocol
	Protocols() []multicodec.Code
}

const BitswapIndentifier = "Bitswap"

func Identifier(event RetrievalEvent) string {
	if event.StorageProviderId() != peer.ID("") {
		return event.StorageProviderId().String()
	}
	protocols := event.Protocols()
	if len(protocols) == 1 && protocols[0] == multicodec.TransportBitswap && event.Phase() != IndexerPhase {
		return BitswapIndentifier
	}
	return ""
}

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
