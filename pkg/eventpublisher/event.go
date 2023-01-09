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
	payloadCid cid.Cid
	candidates []types.RetrievalCandidate
}

func NewRetrievalEventCandidatesFound(payloadCid cid.Cid, candidates []types.RetrievalCandidate) RetrievalEventCandidatesFound {
	c := make([]types.RetrievalCandidate, len(candidates))
	copy(c, candidates)
	return RetrievalEventCandidatesFound{payloadCid, c}
}

type RetrievalEventCandidatesFiltered struct {
	payloadCid cid.Cid
	candidates []types.RetrievalCandidate
}

func NewRetrievalEventCandidatesFiltered(payloadCid cid.Cid, candidates []types.RetrievalCandidate) RetrievalEventCandidatesFiltered {
	c := make([]types.RetrievalCandidate, len(candidates))
	copy(c, candidates)
	return RetrievalEventCandidatesFiltered{payloadCid, c}
}

type RetrievalEventConnect struct {
	phase             Phase
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func NewRetrievalEventConnect(phase Phase, payloadCid cid.Cid, storageProviderId peer.ID) RetrievalEventConnect {
	return RetrievalEventConnect{phase, payloadCid, storageProviderId}
}

type RetrievalEventQueryAsk struct {
	payloadCid        cid.Cid
	storageProviderId peer.ID
	queryResponse     retrievalmarket.QueryResponse
}

func NewRetrievalEventQueryAsk(payloadCid cid.Cid, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse) RetrievalEventQueryAsk {
	return RetrievalEventQueryAsk{payloadCid, storageProviderId, queryResponse}
}

type RetrievalEventQueryAskFiltered struct {
	payloadCid        cid.Cid
	storageProviderId peer.ID
	queryResponse     retrievalmarket.QueryResponse
}

func NewRetrievalEventQueryAskFiltered(payloadCid cid.Cid, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse) RetrievalEventQueryAskFiltered {
	return RetrievalEventQueryAskFiltered{payloadCid, storageProviderId, queryResponse}
}

type RetrievalEventProposed struct {
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func NewRetrievalEventProposed(payloadCid cid.Cid, storageProviderId peer.ID) RetrievalEventProposed {
	return RetrievalEventProposed{payloadCid, storageProviderId}
}

type RetrievalEventStarted struct {
	phase             Phase
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func NewRetrievalEventStarted(phase Phase, payloadCid cid.Cid, storageProviderId peer.ID) RetrievalEventStarted {
	return RetrievalEventStarted{phase, payloadCid, storageProviderId}
}

type RetrievalEventAccepted struct {
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func NewRetrievalEventAccepted(payloadCid cid.Cid, storageProviderId peer.ID) RetrievalEventAccepted {
	return RetrievalEventAccepted{payloadCid, storageProviderId}
}

type RetrievalEventFirstByte struct {
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func NewRetrievalEventFirstByte(payloadCid cid.Cid, storageProviderId peer.ID) RetrievalEventFirstByte {
	return RetrievalEventFirstByte{payloadCid, storageProviderId}
}

type RetrievalEventFailure struct {
	phase             Phase
	payloadCid        cid.Cid
	storageProviderId peer.ID
	errorMessage      string
}

func NewRetrievalEventFailure(phase Phase, payloadCid cid.Cid, storageProviderId peer.ID, errorMessage string) RetrievalEventFailure {
	return RetrievalEventFailure{phase, payloadCid, storageProviderId, errorMessage}
}

type RetrievalEventSuccess struct {
	payloadCid        cid.Cid
	storageProviderId peer.ID
	receivedSize      uint64
	receivedCids      uint64
	duration          time.Duration
	totalPayment      big.Int
}

func NewRetrievalEventSuccess(payloadCid cid.Cid, storageProviderId peer.ID, receivedSize uint64, receivedCids uint64, duration time.Duration, totalPayment big.Int) RetrievalEventSuccess {
	return RetrievalEventSuccess{payloadCid, storageProviderId, receivedSize, receivedCids, duration, totalPayment}
}

func (r RetrievalEventCandidatesFound) Code() Code                 { return CandidatesFoundCode }
func (r RetrievalEventCandidatesFound) Phase() Phase               { return IndexerPhase }
func (r RetrievalEventCandidatesFound) PayloadCid() cid.Cid        { return r.payloadCid }
func (r RetrievalEventCandidatesFound) StorageProviderId() peer.ID { return peer.ID("") }
func (r RetrievalEventCandidatesFound) StorageProviderAddr() address.Address {
	return address.Address{}
}
func (r RetrievalEventCandidatesFound) Candidates() []types.RetrievalCandidate { return r.candidates }
func (r RetrievalEventCandidatesFiltered) Code() Code                          { return CandidatesFilteredCode }
func (r RetrievalEventCandidatesFiltered) Phase() Phase                        { return IndexerPhase }
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
func (r RetrievalEventStarted) PayloadCid() cid.Cid                           { return r.payloadCid }
func (r RetrievalEventStarted) StorageProviderId() peer.ID                    { return r.storageProviderId }
func (r RetrievalEventConnect) Code() Code                                    { return ConnectedCode }
func (r RetrievalEventConnect) Phase() Phase                                  { return r.phase }
func (r RetrievalEventConnect) PayloadCid() cid.Cid                           { return r.payloadCid }
func (r RetrievalEventConnect) StorageProviderId() peer.ID                    { return r.storageProviderId }
func (r RetrievalEventQueryAsk) Code() Code                                   { return QueryAskedCode }
func (r RetrievalEventQueryAsk) Phase() Phase                                 { return QueryPhase }
func (r RetrievalEventQueryAsk) PayloadCid() cid.Cid                          { return r.payloadCid }
func (r RetrievalEventQueryAsk) StorageProviderId() peer.ID                   { return r.storageProviderId }
func (r RetrievalEventQueryAsk) QueryResponse() retrievalmarket.QueryResponse { return r.queryResponse } // QueryResponse returns the response from a storage provider to a query-ask
func (r RetrievalEventQueryAskFiltered) Code() Code                           { return QueryAskedFilteredCode }
func (r RetrievalEventQueryAskFiltered) Phase() Phase                         { return QueryPhase }
func (r RetrievalEventQueryAskFiltered) PayloadCid() cid.Cid                  { return r.payloadCid }
func (r RetrievalEventQueryAskFiltered) StorageProviderId() peer.ID           { return r.storageProviderId }
func (r RetrievalEventQueryAskFiltered) QueryResponse() retrievalmarket.QueryResponse {
	return r.queryResponse
}                                                            // QueryResponse returns the response from a storage provider to a query-ask
func (r RetrievalEventProposed) Code() Code                  { return ProposedCode }
func (r RetrievalEventProposed) Phase() Phase                { return RetrievalPhase }
func (r RetrievalEventProposed) PayloadCid() cid.Cid         { return r.payloadCid }
func (r RetrievalEventProposed) StorageProviderId() peer.ID  { return r.storageProviderId }
func (r RetrievalEventAccepted) Code() Code                  { return AcceptedCode }
func (r RetrievalEventAccepted) Phase() Phase                { return RetrievalPhase }
func (r RetrievalEventAccepted) PayloadCid() cid.Cid         { return r.payloadCid }
func (r RetrievalEventAccepted) StorageProviderId() peer.ID  { return r.storageProviderId }
func (r RetrievalEventFirstByte) Code() Code                 { return FirstByteCode }
func (r RetrievalEventFirstByte) Phase() Phase               { return RetrievalPhase }
func (r RetrievalEventFirstByte) PayloadCid() cid.Cid        { return r.payloadCid }
func (r RetrievalEventFirstByte) StorageProviderId() peer.ID { return r.storageProviderId }
func (r RetrievalEventFailure) Code() Code                   { return FailureCode }
func (r RetrievalEventFailure) Phase() Phase                 { return r.phase }
func (r RetrievalEventFailure) PayloadCid() cid.Cid          { return r.payloadCid }
func (r RetrievalEventFailure) StorageProviderId() peer.ID   { return r.storageProviderId }

// ErrorMessage returns a string form of the error that caused the retrieval
// failure
func (r RetrievalEventFailure) ErrorMessage() string       { return r.errorMessage }
func (r RetrievalEventSuccess) Code() Code                 { return SuccessCode }
func (r RetrievalEventSuccess) Phase() Phase               { return RetrievalPhase }
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
