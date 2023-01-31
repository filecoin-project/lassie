package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	_ types.RetrievalEvent = RetrievalEventCandidatesFound{}
	_ types.RetrievalEvent = RetrievalEventCandidatesFiltered{}
	_ types.RetrievalEvent = RetrievalEventConnected{}
	_ types.RetrievalEvent = RetrievalEventQueryAsked{}
	_ types.RetrievalEvent = RetrievalEventQueryAskedFiltered{}
	_ types.RetrievalEvent = RetrievalEventProposed{}
	_ types.RetrievalEvent = RetrievalEventAccepted{}
	_ types.RetrievalEvent = RetrievalEventFirstByte{}
	_ types.RetrievalEvent = RetrievalEventFailed{}
	_ types.RetrievalEvent = RetrievalEventSuccess{}
)

type EventWithCandidates interface {
	Candidates() []types.RetrievalCandidate
}

type EventWithQueryResponse interface {
	QueryResponse() retrievalmarket.QueryResponse
}

type RetrievalEventCandidatesFound struct {
	eventTime      time.Time
	retrievalId    types.RetrievalID
	phaseStartTime time.Time
	payloadCid     cid.Cid
	candidates     []types.RetrievalCandidate
}

func CandidatesFound(retrievalId types.RetrievalID, phaseStartTime time.Time, payloadCid cid.Cid, candidates []types.RetrievalCandidate) RetrievalEventCandidatesFound {
	c := make([]types.RetrievalCandidate, len(candidates))
	copy(c, candidates)
	return RetrievalEventCandidatesFound{time.Now(), retrievalId, phaseStartTime, payloadCid, c}
}

type RetrievalEventCandidatesFiltered struct {
	eventTime      time.Time
	retrievalId    types.RetrievalID
	phaseStartTime time.Time
	payloadCid     cid.Cid
	candidates     []types.RetrievalCandidate
}

func CandidatesFiltered(retrievalId types.RetrievalID, phaseStartTime time.Time, payloadCid cid.Cid, candidates []types.RetrievalCandidate) RetrievalEventCandidatesFiltered {
	c := make([]types.RetrievalCandidate, len(candidates))
	copy(c, candidates)
	return RetrievalEventCandidatesFiltered{time.Now(), retrievalId, phaseStartTime, payloadCid, c}
}

type RetrievalEventConnected struct {
	eventTime         time.Time
	retrievalId       types.RetrievalID
	phase             types.Phase
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func Connected(retrievalId types.RetrievalID, phaseStartTime time.Time, phase types.Phase, candidate types.RetrievalCandidate) RetrievalEventConnected {
	return RetrievalEventConnected{time.Now(), retrievalId, phase, phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID}
}

type RetrievalEventQueryAsked struct {
	eventTime         time.Time
	retrievalId       types.RetrievalID
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
	queryResponse     retrievalmarket.QueryResponse
}

func QueryAsked(retrievalId types.RetrievalID, phaseStartTime time.Time, candidate types.RetrievalCandidate, queryResponse retrievalmarket.QueryResponse) RetrievalEventQueryAsked {
	return RetrievalEventQueryAsked{time.Now(), retrievalId, phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID, queryResponse}
}

type RetrievalEventQueryAskedFiltered struct {
	eventTime         time.Time
	retrievalId       types.RetrievalID
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
	queryResponse     retrievalmarket.QueryResponse
}

func QueryAskedFiltered(retrievalId types.RetrievalID, phaseStartTime time.Time, candidate types.RetrievalCandidate, queryResponse retrievalmarket.QueryResponse) RetrievalEventQueryAskedFiltered {
	return RetrievalEventQueryAskedFiltered{time.Now(), retrievalId, phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID, queryResponse}
}

type RetrievalEventProposed struct {
	eventTime         time.Time
	retrievalId       types.RetrievalID
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func Proposed(retrievalId types.RetrievalID, phaseStartTime time.Time, candidate types.RetrievalCandidate) RetrievalEventProposed {
	return RetrievalEventProposed{time.Now(), retrievalId, phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID}
}

type RetrievalEventStarted struct {
	eventTime         time.Time
	retrievalId       types.RetrievalID
	phase             types.Phase
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func Started(retrievalId types.RetrievalID, phaseStartTime time.Time, phase types.Phase, candidate types.RetrievalCandidate) RetrievalEventStarted {
	return RetrievalEventStarted{time.Now(), retrievalId, phase, phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID}
}

type RetrievalEventAccepted struct {
	eventTime         time.Time
	retrievalId       types.RetrievalID
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func Accepted(retrievalId types.RetrievalID, phaseStartTime time.Time, candidate types.RetrievalCandidate) RetrievalEventAccepted {
	return RetrievalEventAccepted{time.Now(), retrievalId, phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID}
}

type RetrievalEventFirstByte struct {
	eventTime         time.Time
	retrievalId       types.RetrievalID
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
}

func FirstByte(retrievalId types.RetrievalID, phaseStartTime time.Time, candidate types.RetrievalCandidate) RetrievalEventFirstByte {
	return RetrievalEventFirstByte{time.Now(), retrievalId, phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID}
}

type RetrievalEventFailed struct {
	eventTime         time.Time
	retrievalId       types.RetrievalID
	phase             types.Phase
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
	errorMessage      string
}

func Failed(retrievalId types.RetrievalID, phaseStartTime time.Time, phase types.Phase, candidate types.RetrievalCandidate, errorMessage string) RetrievalEventFailed {
	return RetrievalEventFailed{time.Now(), retrievalId, phase, phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID, errorMessage}
}

type RetrievalEventSuccess struct {
	eventTime         time.Time
	retrievalId       types.RetrievalID
	phaseStartTime    time.Time
	payloadCid        cid.Cid
	storageProviderId peer.ID
	receivedSize      uint64
	receivedCids      uint64
	duration          time.Duration
	totalPayment      big.Int
}

func Success(retrievalId types.RetrievalID, phaseStartTime time.Time, candidate types.RetrievalCandidate, receivedSize uint64, receivedCids uint64, duration time.Duration, totalPayment big.Int) RetrievalEventSuccess {
	return RetrievalEventSuccess{time.Now(), retrievalId, phaseStartTime, candidate.RootCid, candidate.MinerPeer.ID, receivedSize, receivedCids, duration, totalPayment}
}

func (r RetrievalEventCandidatesFound) Code() types.EventCode                  { return types.CandidatesFoundCode }
func (r RetrievalEventCandidatesFound) Time() time.Time                        { return r.eventTime }
func (r RetrievalEventCandidatesFound) RetrievalId() types.RetrievalID         { return r.retrievalId }
func (r RetrievalEventCandidatesFound) Phase() types.Phase                     { return types.IndexerPhase }
func (r RetrievalEventCandidatesFound) PhaseStartTime() time.Time              { return r.phaseStartTime }
func (r RetrievalEventCandidatesFound) PayloadCid() cid.Cid                    { return r.payloadCid }
func (r RetrievalEventCandidatesFound) StorageProviderId() peer.ID             { return peer.ID("") }
func (r RetrievalEventCandidatesFound) Candidates() []types.RetrievalCandidate { return r.candidates }
func (r RetrievalEventCandidatesFound) String() string {
	return fmt.Sprintf("CandidatesFoundEvent<%s, %s, %s, %d>", r.eventTime, r.retrievalId, r.payloadCid, len(r.candidates))
}
func (r RetrievalEventCandidatesFiltered) Code() types.EventCode          { return types.CandidatesFilteredCode }
func (r RetrievalEventCandidatesFiltered) Time() time.Time                { return r.eventTime }
func (r RetrievalEventCandidatesFiltered) RetrievalId() types.RetrievalID { return r.retrievalId }
func (r RetrievalEventCandidatesFiltered) Phase() types.Phase             { return types.IndexerPhase }
func (r RetrievalEventCandidatesFiltered) PhaseStartTime() time.Time      { return r.phaseStartTime }
func (r RetrievalEventCandidatesFiltered) PayloadCid() cid.Cid            { return r.payloadCid }
func (r RetrievalEventCandidatesFiltered) StorageProviderId() peer.ID     { return peer.ID("") }
func (r RetrievalEventCandidatesFiltered) Candidates() []types.RetrievalCandidate {
	return r.candidates
}
func (r RetrievalEventCandidatesFiltered) String() string {
	return fmt.Sprintf("CandidatesFilteredEvent<%s, %s, %s, %d>", r.eventTime, r.retrievalId, r.payloadCid, len(r.candidates))
}
func (r RetrievalEventStarted) Code() types.EventCode          { return types.StartedCode }
func (r RetrievalEventStarted) Time() time.Time                { return r.eventTime }
func (r RetrievalEventStarted) RetrievalId() types.RetrievalID { return r.retrievalId }
func (r RetrievalEventStarted) Phase() types.Phase             { return r.phase }
func (r RetrievalEventStarted) PhaseStartTime() time.Time      { return r.phaseStartTime }
func (r RetrievalEventStarted) PayloadCid() cid.Cid            { return r.payloadCid }
func (r RetrievalEventStarted) StorageProviderId() peer.ID     { return r.storageProviderId }
func (r RetrievalEventStarted) String() string {
	return fmt.Sprintf("StartedEvent<%s, %s, %s, %s, %s>", r.phase, r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId)
}
func (r RetrievalEventConnected) Code() types.EventCode          { return types.ConnectedCode }
func (r RetrievalEventConnected) Time() time.Time                { return r.eventTime }
func (r RetrievalEventConnected) RetrievalId() types.RetrievalID { return r.retrievalId }
func (r RetrievalEventConnected) Phase() types.Phase             { return r.phase }
func (r RetrievalEventConnected) PhaseStartTime() time.Time      { return r.phaseStartTime }
func (r RetrievalEventConnected) PayloadCid() cid.Cid            { return r.payloadCid }
func (r RetrievalEventConnected) StorageProviderId() peer.ID     { return r.storageProviderId }
func (r RetrievalEventConnected) String() string {
	return fmt.Sprintf("ConnectedEvent<%s, %s, %s, %s, %s>", r.phase, r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId)
}
func (r RetrievalEventQueryAsked) Code() types.EventCode          { return types.QueryAskedCode }
func (r RetrievalEventQueryAsked) Time() time.Time                { return r.eventTime }
func (r RetrievalEventQueryAsked) RetrievalId() types.RetrievalID { return r.retrievalId }
func (r RetrievalEventQueryAsked) Phase() types.Phase             { return types.QueryPhase }
func (r RetrievalEventQueryAsked) PhaseStartTime() time.Time      { return r.phaseStartTime }
func (r RetrievalEventQueryAsked) PayloadCid() cid.Cid            { return r.payloadCid }
func (r RetrievalEventQueryAsked) StorageProviderId() peer.ID     { return r.storageProviderId }
func (r RetrievalEventQueryAsked) QueryResponse() retrievalmarket.QueryResponse {
	return r.queryResponse
}
func (r RetrievalEventQueryAsked) String() string {
	return fmt.Sprintf("QueryAsked<%s, %s, %s, %s, {%d, %d, %s, %d, %d}>", r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId, r.queryResponse.Status, r.queryResponse.Size, r.queryResponse.MinPricePerByte, r.queryResponse.MaxPaymentInterval, r.queryResponse.MaxPaymentIntervalIncrease)
}
func (r RetrievalEventQueryAskedFiltered) Code() types.EventCode          { return types.QueryAskedFilteredCode }
func (r RetrievalEventQueryAskedFiltered) Time() time.Time                { return r.eventTime }
func (r RetrievalEventQueryAskedFiltered) RetrievalId() types.RetrievalID { return r.retrievalId }
func (r RetrievalEventQueryAskedFiltered) Phase() types.Phase             { return types.QueryPhase }
func (r RetrievalEventQueryAskedFiltered) PhaseStartTime() time.Time      { return r.phaseStartTime }
func (r RetrievalEventQueryAskedFiltered) PayloadCid() cid.Cid            { return r.payloadCid }
func (r RetrievalEventQueryAskedFiltered) StorageProviderId() peer.ID     { return r.storageProviderId }
func (r RetrievalEventQueryAskedFiltered) QueryResponse() retrievalmarket.QueryResponse {
	return r.queryResponse
} // QueryResponse returns the response from a storage provider to a query-ask
func (r RetrievalEventQueryAskedFiltered) String() string {
	return fmt.Sprintf("QueryAskedFiltered<%s, %s, %s, %s, {%d, %d, %s, %d, %d}>", r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId, r.queryResponse.Status, r.queryResponse.Size, r.queryResponse.MinPricePerByte, r.queryResponse.MaxPaymentInterval, r.queryResponse.MaxPaymentIntervalIncrease)
}
func (r RetrievalEventProposed) Code() types.EventCode          { return types.ProposedCode }
func (r RetrievalEventProposed) Time() time.Time                { return r.eventTime }
func (r RetrievalEventProposed) RetrievalId() types.RetrievalID { return r.retrievalId }
func (r RetrievalEventProposed) Phase() types.Phase             { return types.RetrievalPhase }
func (r RetrievalEventProposed) PhaseStartTime() time.Time      { return r.phaseStartTime }
func (r RetrievalEventProposed) PayloadCid() cid.Cid            { return r.payloadCid }
func (r RetrievalEventProposed) StorageProviderId() peer.ID     { return r.storageProviderId }
func (r RetrievalEventProposed) String() string {
	return fmt.Sprintf("ProposedEvent<%s, %s, %s, %s>", r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId)
}
func (r RetrievalEventAccepted) Code() types.EventCode          { return types.AcceptedCode }
func (r RetrievalEventAccepted) Time() time.Time                { return r.eventTime }
func (r RetrievalEventAccepted) RetrievalId() types.RetrievalID { return r.retrievalId }
func (r RetrievalEventAccepted) Phase() types.Phase             { return types.RetrievalPhase }
func (r RetrievalEventAccepted) PhaseStartTime() time.Time      { return r.phaseStartTime }
func (r RetrievalEventAccepted) PayloadCid() cid.Cid            { return r.payloadCid }
func (r RetrievalEventAccepted) StorageProviderId() peer.ID     { return r.storageProviderId }
func (r RetrievalEventAccepted) String() string {
	return fmt.Sprintf("AcceptedEvent<%s, %s, %s, %s>", r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId)
}
func (r RetrievalEventFirstByte) Code() types.EventCode          { return types.FirstByteCode }
func (r RetrievalEventFirstByte) Time() time.Time                { return r.eventTime }
func (r RetrievalEventFirstByte) RetrievalId() types.RetrievalID { return r.retrievalId }
func (r RetrievalEventFirstByte) Phase() types.Phase             { return types.RetrievalPhase }
func (r RetrievalEventFirstByte) PhaseStartTime() time.Time      { return r.phaseStartTime }
func (r RetrievalEventFirstByte) PayloadCid() cid.Cid            { return r.payloadCid }
func (r RetrievalEventFirstByte) StorageProviderId() peer.ID     { return r.storageProviderId }
func (r RetrievalEventFirstByte) String() string {
	return fmt.Sprintf("FirstByteEvent<%s, %s, %s, %s>", r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId)
}
func (r RetrievalEventFailed) Code() types.EventCode          { return types.FailedCode }
func (r RetrievalEventFailed) Time() time.Time                { return r.eventTime }
func (r RetrievalEventFailed) RetrievalId() types.RetrievalID { return r.retrievalId }
func (r RetrievalEventFailed) Phase() types.Phase             { return r.phase }
func (r RetrievalEventFailed) PhaseStartTime() time.Time      { return r.phaseStartTime }
func (r RetrievalEventFailed) PayloadCid() cid.Cid            { return r.payloadCid }
func (r RetrievalEventFailed) StorageProviderId() peer.ID     { return r.storageProviderId }

// ErrorMessage returns a string form of the error that caused the retrieval
// failure
func (r RetrievalEventFailed) ErrorMessage() string { return r.errorMessage }
func (r RetrievalEventFailed) String() string {
	return fmt.Sprintf("FailedEvent<%s, %s, %s, %s, %s>", r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId, r.errorMessage)
}
func (r RetrievalEventSuccess) Code() types.EventCode          { return types.SuccessCode }
func (r RetrievalEventSuccess) Time() time.Time                { return r.eventTime }
func (r RetrievalEventSuccess) RetrievalId() types.RetrievalID { return r.retrievalId }
func (r RetrievalEventSuccess) Phase() types.Phase             { return types.RetrievalPhase }
func (r RetrievalEventSuccess) PhaseStartTime() time.Time      { return r.phaseStartTime }
func (r RetrievalEventSuccess) PayloadCid() cid.Cid            { return r.payloadCid }
func (r RetrievalEventSuccess) StorageProviderId() peer.ID     { return r.storageProviderId }
func (r RetrievalEventSuccess) Duration() time.Duration        { return r.duration }
func (r RetrievalEventSuccess) TotalPayment() big.Int          { return r.totalPayment }

// ReceivedSize returns the number of bytes received
func (r RetrievalEventSuccess) ReceivedSize() uint64 { return r.receivedSize }

// ReceivedCids returns the number of (non-unique) CIDs received so far - note
// that a block can exist in more than one place in the DAG so this may not
// equal the total number of blocks transferred
func (r RetrievalEventSuccess) ReceivedCids() uint64 { return r.receivedCids }
func (r RetrievalEventSuccess) String() string {
	return fmt.Sprintf("SuccessEvent<%s, %s, %s, %s, { %s, %s, %d, %d }>", r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId, r.duration, r.totalPayment, r.receivedSize, r.receivedCids)
}
