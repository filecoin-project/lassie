package types

import (
	"context"
	"errors"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

type Result[T any] struct {
	Value T
	Err   error
}

func Value[T any](value T) Result[T] {
	return Result[T]{Value: value}
}

func Error[T any](err error) Result[T] {
	return Result[T]{Err: err}
}

type GracefulCanceller interface {
	TearDown() error
}

type gracefulCanceler struct {
	tearDownFn func() error
}

func (gc gracefulCanceler) TearDown() error {
	return gc.tearDownFn()
}

func OnTearDown(tearDownFn func() error) GracefulCanceller {
	return gracefulCanceler{tearDownFn: tearDownFn}
}

type Stream[T any] interface {
	Subscribe(StreamSubscriber[T]) GracefulCanceller
}

type StreamSubscriber[T any] interface {
	Next(T)
	Error(error)
	Complete()
}

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
}

type CandidateStream = Stream[[]RetrievalCandidate]

type AsyncRetrieval = Stream[*RetrievalStats]

func GetResults(asyncRetrieval AsyncRetrieval) (*RetrievalStats, error) {
	var returnedStats *RetrievalStats
	var returnedError error
	asyncRetrieval.Subscribe(&asyncRetrievalSubscriber{
		onComplete: func(stats *RetrievalStats, err error) {
			returnedStats = stats
			returnedError = err
		},
	})
	return returnedStats, returnedError
}

type asyncRetrievalSubscriber struct {
	received   *RetrievalStats
	onComplete func(*RetrievalStats, error)
}

func (ars *asyncRetrievalSubscriber) Next(stats *RetrievalStats) {
	ars.received = stats
}

func (ars *asyncRetrievalSubscriber) Error(err error) {
	ars.onComplete(nil, err)
}
func (ars *asyncRetrievalSubscriber) Complete() {
	ars.onComplete(ars.received, nil)
}

type Retriever interface {
	Retrieve(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) (*RetrievalStats, error)
}

type AsyncRetriever interface {
	RetrieveAsync(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) AsyncRetrieval
}

type CandidateRetrieval interface {
	RetrieveFromCandidates([]RetrievalCandidate) (*RetrievalStats, error)
}

type CandidateRetriever interface {
	Retrieve(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) CandidateRetrieval
}

type AsyncCandidateRetrieval interface {
	RetrieveFromCandidatesAsync(stream CandidateStream) AsyncRetrieval
}

type AsyncCandidateRetriever interface {
	RetrieveAsync(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) AsyncCandidateRetrieval
}

type CandidateFinder interface {
	FindCandidates(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) (CandidateStream, error, <-chan error)
}

type RetrievalSplitter interface {
	SplitCandidates([]RetrievalCandidate) ([][]RetrievalCandidate, error)
}

type CandidateSplitter interface {
	SplitRetrieval(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) RetrievalSplitter
}

type AsyncRetrievalSplitter interface {
	SplitCandidatesAsync(candidates CandidateStream) []CandidateStream
}

type AsyncCandidateSplitter interface {
	SplitRetrievalAsync(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) AsyncRetrievalSplitter
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
}

type RetrievalResult = Result[*RetrievalStats]
type FindCandidatesResult = Result[RetrievalCandidate]
type CandidateRetrievalCall struct {
	Candidates         []RetrievalCandidate
	CandidateRetrieval CandidateRetrieval
}

type RetrievalCoordinator func(context.Context, []CandidateRetrievalCall) (*RetrievalStats, error)

type CoordinationKind string

const (
	RaceCoordination       = "race"
	SequentialCoordination = "sequential"
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
