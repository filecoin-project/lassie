package eventpublisher

import (
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
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

type Code string

const (
	CandidatesFoundCode    Code = "candidates-found"
	CandidatesFilteredCode Code = "candidates-filtered"
	StartedCode            Code = "started"
	ConnectedCode          Code = "connected"
	QueryAskedCode         Code = "query-asked"
	QueryAskedFilteredCode Code = "query-asked-filtered"
	ProposedCode           Code = "proposed"
	AcceptedCode           Code = "accepted"
	FirstByteCode          Code = "first-byte-received"
	FailureCode            Code = "failure"
	SuccessCode            Code = "success"
)

type RetrievalEvent interface {
	// Code returns the type of event this is
	Code() Code
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

var (
	_ RetrievalEvent = RetrievalEventCandidatesFound{}
	_ RetrievalEvent = RetrievalEventCandidatesFiltered{}
	_ RetrievalEvent = RetrievalEventConnect{}
	_ RetrievalEvent = RetrievalEventQueryAsk{}
	_ RetrievalEvent = RetrievalEventQueryAskFiltered{}
	_ RetrievalEvent = RetrievalEventProposed{}
	_ RetrievalEvent = RetrievalEventAccepted{}
	_ RetrievalEvent = RetrievalEventFirstByte{}
	_ RetrievalEvent = RetrievalEventFailure{}
	_ RetrievalEvent = RetrievalEventSuccess{}
)

type RetrievalEventCandidatesFound struct {
	phaseStartTime time.Time
	payloadCid     cid.Cid
	candidates     []types.RetrievalCandidate
}

func CandidatesFound(phaseStartTime time.Time, payloadCid cid.Cid, candidates []types.RetrievalCandidate) RetrievalEventCandidatesFound {
	c := make([]types.RetrievalCandidate, len(candidates))
	copy(c, candidates)
	return RetrievalEventCandidatesFound{phaseStartTime, payloadCid, c}
}

type RetrievalEventCandidatesFiltered struct {
	phaseStartTime time.Time
	payloadCid     cid.Cid
	candidates     []types.RetrievalCandidate
}

func CandidatesFiltered(phaseStartTime time.Time, payloadCid cid.Cid, candidates []types.RetrievalCandidate) RetrievalEventCandidatesFiltered {
	c := make([]types.RetrievalCandidate, len(candidates))
	copy(c, candidates)
	return RetrievalEventCandidatesFiltered{phaseStartTime, payloadCid, c}
}

type RetrievalEventConnect struct {
	phase             Phase
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func Connect(phaseStartTime time.Time, phase Phase, candidate types.RetrievalCandidate) RetrievalEventConnect {
	return RetrievalEventConnect{phase, phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID}
}

type RetrievalEventQueryAsk struct {
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
	queryResponse     retrievalmarket.QueryResponse
}

func QueryAsk(phaseStartTime time.Time, candidate types.RetrievalCandidate, queryResponse retrievalmarket.QueryResponse) RetrievalEventQueryAsk {
	return RetrievalEventQueryAsk{phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID, queryResponse}
}

type RetrievalEventQueryAskFiltered struct {
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
	queryResponse     retrievalmarket.QueryResponse
}

func QueryAskFiltered(phaseStartTime time.Time, candidate types.RetrievalCandidate, queryResponse retrievalmarket.QueryResponse) RetrievalEventQueryAskFiltered {
	return RetrievalEventQueryAskFiltered{phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID, queryResponse}
}

type RetrievalEventProposed struct {
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func Proposed(phaseStartTime time.Time, candidate types.RetrievalCandidate) RetrievalEventProposed {
	return RetrievalEventProposed{phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID}
}

type RetrievalEventStarted struct {
	phase             Phase
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func Started(phaseStartTime time.Time, phase Phase, candidate types.RetrievalCandidate) RetrievalEventStarted {
	return RetrievalEventStarted{phase, phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID}
}

type RetrievalEventAccepted struct {
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func Accepted(phaseStartTime time.Time, candidate types.RetrievalCandidate) RetrievalEventAccepted {
	return RetrievalEventAccepted{phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID}
}

type RetrievalEventFirstByte struct {
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func FirstByte(phaseStartTime time.Time, candidate types.RetrievalCandidate) RetrievalEventFirstByte {
	return RetrievalEventFirstByte{phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID}
}

type RetrievalEventFailure struct {
	phase             Phase
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
	errorMessage      string
}

func Failure(phaseStartTime time.Time, phase Phase, candidate types.RetrievalCandidate, errorMessage string) RetrievalEventFailure {
	return RetrievalEventFailure{phase, phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID, errorMessage}
}

type RetrievalEventSuccess struct {
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
	receivedSize      uint64
	receivedCids      uint64
	duration          time.Duration
	totalPayment      big.Int
}

func Success(phaseStartTime time.Time, candidate types.RetrievalCandidate, receivedSize uint64, receivedCids uint64, duration time.Duration, totalPayment big.Int) RetrievalEventSuccess {
	return RetrievalEventSuccess{phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID, receivedSize, receivedCids, duration, totalPayment}
}

func (r RetrievalEventCandidatesFound) Code() Code                 { return CandidatesFoundCode }
func (r RetrievalEventCandidatesFound) Phase() Phase               { return IndexerPhase }
func (r RetrievalEventCandidatesFound) PhaseStartTime() time.Time  { return r.phaseStartTime }
func (r RetrievalEventCandidatesFound) PayloadCid() cid.Cid        { return r.payloadCid }
func (r RetrievalEventCandidatesFound) StorageProviderId() peer.ID { return peer.ID("") }
func (r RetrievalEventCandidatesFound) StorageProviderAddr() address.Address {
	return address.Address{}
}
func (r RetrievalEventCandidatesFound) Candidates() []types.RetrievalCandidate { return r.candidates }
func (r RetrievalEventCandidatesFiltered) Code() Code                          { return CandidatesFilteredCode }
func (r RetrievalEventCandidatesFiltered) Phase() Phase                        { return IndexerPhase }
func (r RetrievalEventCandidatesFiltered) PhaseStartTime() time.Time           { return r.phaseStartTime }
func (r RetrievalEventCandidatesFiltered) PayloadCid() cid.Cid                 { return r.payloadCid }
func (r RetrievalEventCandidatesFiltered) StorageProviderId() peer.ID          { return peer.ID("") }
func (r RetrievalEventCandidatesFiltered) StorageProviderAddr() address.Address {
	return address.Address{}
}
func (r RetrievalEventCandidatesFiltered) Candidates() []types.RetrievalCandidate {
	return r.candidates
}
func (r RetrievalEventStarted) Code() Code                                    { return StartedCode }
func (r RetrievalEventStarted) Phase() Phase                                  { return r.phase }
func (r RetrievalEventStarted) PhaseStartTime() time.Time                     { return r.phaseStartTime }
func (r RetrievalEventStarted) PayloadCid() cid.Cid                           { return r.payloadCid }
func (r RetrievalEventStarted) StorageProviderId() peer.ID                    { return r.storageProviderId }
func (r RetrievalEventConnect) Code() Code                                    { return ConnectedCode }
func (r RetrievalEventConnect) Phase() Phase                                  { return r.phase }
func (r RetrievalEventConnect) PhaseStartTime() time.Time                     { return r.phaseStartTime }
func (r RetrievalEventConnect) PayloadCid() cid.Cid                           { return r.payloadCid }
func (r RetrievalEventConnect) StorageProviderId() peer.ID                    { return r.storageProviderId }
func (r RetrievalEventQueryAsk) Code() Code                                   { return QueryAskedCode }
func (r RetrievalEventQueryAsk) Phase() Phase                                 { return QueryPhase }
func (r RetrievalEventQueryAsk) PhaseStartTime() time.Time                    { return r.phaseStartTime }
func (r RetrievalEventQueryAsk) PayloadCid() cid.Cid                          { return r.payloadCid }
func (r RetrievalEventQueryAsk) StorageProviderId() peer.ID                   { return r.storageProviderId }
func (r RetrievalEventQueryAsk) QueryResponse() retrievalmarket.QueryResponse { return r.queryResponse } // QueryResponse returns the response from a storage provider to a query-ask
func (r RetrievalEventQueryAskFiltered) Code() Code                           { return QueryAskedFilteredCode }
func (r RetrievalEventQueryAskFiltered) Phase() Phase                         { return QueryPhase }
func (r RetrievalEventQueryAskFiltered) PhaseStartTime() time.Time            { return r.phaseStartTime }
func (r RetrievalEventQueryAskFiltered) PayloadCid() cid.Cid                  { return r.payloadCid }
func (r RetrievalEventQueryAskFiltered) StorageProviderId() peer.ID           { return r.storageProviderId }
func (r RetrievalEventQueryAskFiltered) QueryResponse() retrievalmarket.QueryResponse {
	return r.queryResponse
}                                                            // QueryResponse returns the response from a storage provider to a query-ask
func (r RetrievalEventProposed) Code() Code                  { return ProposedCode }
func (r RetrievalEventProposed) Phase() Phase                { return RetrievalPhase }
func (r RetrievalEventProposed) PhaseStartTime() time.Time   { return r.phaseStartTime }
func (r RetrievalEventProposed) PayloadCid() cid.Cid         { return r.payloadCid }
func (r RetrievalEventProposed) StorageProviderId() peer.ID  { return r.storageProviderId }
func (r RetrievalEventAccepted) Code() Code                  { return AcceptedCode }
func (r RetrievalEventAccepted) Phase() Phase                { return RetrievalPhase }
func (r RetrievalEventAccepted) PhaseStartTime() time.Time   { return r.phaseStartTime }
func (r RetrievalEventAccepted) PayloadCid() cid.Cid         { return r.payloadCid }
func (r RetrievalEventAccepted) StorageProviderId() peer.ID  { return r.storageProviderId }
func (r RetrievalEventFirstByte) Code() Code                 { return FirstByteCode }
func (r RetrievalEventFirstByte) Phase() Phase               { return RetrievalPhase }
func (r RetrievalEventFirstByte) PhaseStartTime() time.Time  { return r.phaseStartTime }
func (r RetrievalEventFirstByte) PayloadCid() cid.Cid        { return r.payloadCid }
func (r RetrievalEventFirstByte) StorageProviderId() peer.ID { return r.storageProviderId }
func (r RetrievalEventFailure) Code() Code                   { return FailureCode }
func (r RetrievalEventFailure) Phase() Phase                 { return r.phase }
func (r RetrievalEventFailure) PhaseStartTime() time.Time    { return r.phaseStartTime }
func (r RetrievalEventFailure) PayloadCid() cid.Cid          { return r.payloadCid }
func (r RetrievalEventFailure) StorageProviderId() peer.ID   { return r.storageProviderId }

// ErrorMessage returns a string form of the error that caused the retrieval
// failure
func (r RetrievalEventFailure) ErrorMessage() string       { return r.errorMessage }
func (r RetrievalEventSuccess) Code() Code                 { return SuccessCode }
func (r RetrievalEventSuccess) Phase() Phase               { return RetrievalPhase }
func (r RetrievalEventSuccess) PhaseStartTime() time.Time  { return r.phaseStartTime }
func (r RetrievalEventSuccess) PayloadCid() cid.Cid        { return r.payloadCid }
func (r RetrievalEventSuccess) StorageProviderId() peer.ID { return r.storageProviderId }
func (r RetrievalEventSuccess) Duration() time.Duration    { return r.duration }
func (r RetrievalEventSuccess) TotalPayment() big.Int      { return r.totalPayment }

// ReceivedSize returns the number of bytes received
func (r RetrievalEventSuccess) ReceivedSize() uint64 { return r.receivedSize }

// ReceivedCids returns the number of (non-unique) CIDs received so far - note
// that a block can exist in more than one place in the DAG so this may not
// equal the total number of blocks transferred
func (r RetrievalEventSuccess) ReceivedCids() uint64 { return r.receivedCids }
