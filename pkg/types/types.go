package types

import (
	"context"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/peer"
)

type RetrievalCandidate struct {
	MinerPeer peer.AddrInfo
	RootCid   cid.Cid
	Metadata  metadata.Metadata
}

func NewRetrievalCandidate(pid peer.ID, rootCid cid.Cid) RetrievalCandidate {
	return RetrievalCandidate{
		MinerPeer: peer.AddrInfo{ID: pid},
		RootCid:   rootCid,
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

type Retriever interface {
	Retrieve(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) (*RetrievalStats, error)
}

type CandidateFinder interface {
	FindCandidates(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) ([]RetrievalCandidate, error)
}

type CandidateRetrieval interface {
	RetrieveFromCandidates([]RetrievalCandidate) (*RetrievalStats, error)
}

type CandidateRetriever interface {
	Retrieve(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) CandidateRetrieval
}

type RetrievalSplitter interface {
	SplitCandidates([]RetrievalCandidate) ([][]RetrievalCandidate, error)
}

type CandidateSplitter interface {
	SplitRetrieval(ctx context.Context, request RetrievalRequest, events func(RetrievalEvent)) RetrievalSplitter
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

	// TODO: we should be able to get this if we hook into the graphsync event stream
	// TimeToFirstByte time.Duration
}

type RetrievalResult struct {
	Stats *RetrievalStats
	Err   error
}

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
