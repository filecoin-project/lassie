package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

var (
	_ types.RetrievalEvent = RetrievalEventCandidatesFound{}
	_ types.RetrievalEvent = RetrievalEventCandidatesFiltered{}
	_ types.RetrievalEvent = RetrievalEventConnected{}
	_ types.RetrievalEvent = RetrievalEventProposed{}
	_ types.RetrievalEvent = RetrievalEventAccepted{}
	_ types.RetrievalEvent = RetrievalEventFirstByte{}
	_ types.RetrievalEvent = RetrievalEventFailed{}
	_ types.RetrievalEvent = RetrievalEventSuccess{}
	_ types.RetrievalEvent = RetrievalEventFinished{}
)

type EventWithCandidates interface {
	types.RetrievalEvent
	Candidates() []types.RetrievalCandidate
}

type baseEvent struct {
	eventTime      time.Time
	retrievalId    types.RetrievalID
	phaseStartTime time.Time
	payloadCid     cid.Cid
	protocols      []multicodec.Code
}

func (r baseEvent) Time() time.Time                { return r.eventTime }
func (r baseEvent) RetrievalId() types.RetrievalID { return r.retrievalId }
func (r baseEvent) PhaseStartTime() time.Time      { return r.phaseStartTime }
func (r baseEvent) PayloadCid() cid.Cid            { return r.payloadCid }
func (r baseEvent) Protocols() []multicodec.Code   { return r.protocols }

type indexerEvent struct {
	baseEvent
	candidates []types.RetrievalCandidate
}

func (r indexerEvent) StorageProviderId() peer.ID             { return peer.ID("") }
func (r indexerEvent) Candidates() []types.RetrievalCandidate { return r.candidates }

type RetrievalEventCandidatesFound struct {
	indexerEvent
}

func collectProtocols(candidates []types.RetrievalCandidate) []multicodec.Code {
	allProtocols := make(map[multicodec.Code]struct{})
	for _, candidate := range candidates {
		for _, protocol := range candidate.Metadata.Protocols() {
			allProtocols[protocol] = struct{}{}
		}
	}
	allProtocolsArr := make([]multicodec.Code, 0, len(allProtocols))
	for protocol := range allProtocols {
		allProtocolsArr = append(allProtocolsArr, protocol)
	}
	return allProtocolsArr
}

func CandidatesFound(at time.Time, retrievalId types.RetrievalID, phaseStartTime time.Time, payloadCid cid.Cid, candidates []types.RetrievalCandidate) RetrievalEventCandidatesFound {
	c := make([]types.RetrievalCandidate, len(candidates))
	copy(c, candidates)
	return RetrievalEventCandidatesFound{indexerEvent{baseEvent{at, retrievalId, phaseStartTime, payloadCid, collectProtocols(candidates)}, c}}
}

type RetrievalEventCandidatesFiltered struct {
	indexerEvent
}

func CandidatesFiltered(at time.Time, retrievalId types.RetrievalID, phaseStartTime time.Time, payloadCid cid.Cid, candidates []types.RetrievalCandidate) RetrievalEventCandidatesFiltered {
	c := make([]types.RetrievalCandidate, len(candidates))
	copy(c, candidates)
	return RetrievalEventCandidatesFiltered{indexerEvent{baseEvent{at, retrievalId, phaseStartTime, payloadCid, collectProtocols(candidates)}, c}}
}

type spBaseEvent struct {
	baseEvent
	storageProviderId peer.ID
}

type RetrievalEventConnected struct {
	spBaseEvent
	phase types.Phase
}

func (r spBaseEvent) StorageProviderId() peer.ID { return r.storageProviderId }

func Connected(at time.Time, retrievalId types.RetrievalID, phaseStartTime time.Time, phase types.Phase, candidate types.RetrievalCandidate) RetrievalEventConnected {
	candidate.Metadata.Protocols()
	return RetrievalEventConnected{spBaseEvent{baseEvent{at, retrievalId, phaseStartTime, candidate.RootCid, candidate.Metadata.Protocols()}, candidate.MinerPeer.ID}, phase}
}

// RetrievalEventProposed describes when the proposal took place during the RetrievalPhase
type RetrievalEventProposed struct {
	spBaseEvent
}

func Proposed(at time.Time, retrievalId types.RetrievalID, phaseStartTime time.Time, candidate types.RetrievalCandidate) RetrievalEventProposed {
	return RetrievalEventProposed{spBaseEvent{baseEvent{at, retrievalId, phaseStartTime, candidate.RootCid, candidate.Metadata.Protocols()}, candidate.MinerPeer.ID}}
}

// RetrievalEventStarted describes when a phase starts
type RetrievalEventStarted struct {
	spBaseEvent
	phase types.Phase
}

func Started(at time.Time, retrievalId types.RetrievalID, phaseStartTime time.Time, phase types.Phase, candidate types.RetrievalCandidate) RetrievalEventStarted {
	return RetrievalEventStarted{spBaseEvent{baseEvent{at, retrievalId, phaseStartTime, candidate.RootCid, candidate.Metadata.Protocols()}, candidate.MinerPeer.ID}, phase}
}

// RetrievalEventFirstByte describes when the first byte of data was received during the RetrievalPhase
type RetrievalEventAccepted struct {
	spBaseEvent
}

func Accepted(at time.Time, retrievalId types.RetrievalID, phaseStartTime time.Time, candidate types.RetrievalCandidate) RetrievalEventAccepted {
	return RetrievalEventAccepted{spBaseEvent{baseEvent{at, retrievalId, phaseStartTime, candidate.RootCid, candidate.Metadata.Protocols()}, candidate.MinerPeer.ID}}
}

// RetrievalEventFirstByte describes when the first byte of data was received during the RetrievalPhase
type RetrievalEventFirstByte struct {
	spBaseEvent
}

func FirstByte(at time.Time, retrievalId types.RetrievalID, phaseStartTime time.Time, candidate types.RetrievalCandidate) RetrievalEventFirstByte {
	return RetrievalEventFirstByte{spBaseEvent{baseEvent{at, retrievalId, phaseStartTime, candidate.RootCid, candidate.Metadata.Protocols()}, candidate.MinerPeer.ID}}
}

// RetrievalEventFailed describes a phase agnostic failure
type RetrievalEventFailed struct {
	spBaseEvent
	phase        types.Phase
	errorMessage string
}

func Failed(at time.Time, retrievalId types.RetrievalID, phaseStartTime time.Time, phase types.Phase, candidate types.RetrievalCandidate, errorMessage string) RetrievalEventFailed {
	return RetrievalEventFailed{spBaseEvent{baseEvent{at, retrievalId, phaseStartTime, candidate.RootCid, candidate.Metadata.Protocols()}, candidate.MinerPeer.ID}, phase, errorMessage}
}

// RetrievalEventSuccess describes a successful retrieval of data during the RetrievalPhase
type RetrievalEventSuccess struct {
	spBaseEvent
	receivedSize            uint64
	receivedCids            uint64
	duration                time.Duration
	totalPayment            big.Int
	bitswapPreloadedPercent uint64
}

func Success(at time.Time, retrievalId types.RetrievalID, phaseStartTime time.Time, candidate types.RetrievalCandidate, receivedSize uint64, receivedCids uint64, duration time.Duration, totalPayment big.Int, bitswapPreloadedPercent uint64) RetrievalEventSuccess {
	return RetrievalEventSuccess{spBaseEvent{baseEvent{at, retrievalId, phaseStartTime, candidate.RootCid, candidate.Metadata.Protocols()}, candidate.MinerPeer.ID}, receivedSize, receivedCids, duration, totalPayment, bitswapPreloadedPercent}
}

// RetrievalEventFinished describes when an entire fetch finishes
type RetrievalEventFinished struct {
	spBaseEvent
}

func Finished(at time.Time, retrievalId types.RetrievalID, phaseStartTime time.Time, candidate types.RetrievalCandidate) RetrievalEventFinished {
	return RetrievalEventFinished{spBaseEvent{baseEvent{at, retrievalId, phaseStartTime, candidate.RootCid, candidate.Metadata.Protocols()}, candidate.MinerPeer.ID}}
}

func (r RetrievalEventCandidatesFound) Code() types.EventCode { return types.CandidatesFoundCode }
func (r RetrievalEventCandidatesFound) Phase() types.Phase    { return types.IndexerPhase }
func (r RetrievalEventCandidatesFound) String() string {
	return fmt.Sprintf("CandidatesFoundEvent<%s, %s, %s, %d, %v>", r.eventTime, r.retrievalId, r.payloadCid, len(r.candidates), r.protocols)
}
func (r RetrievalEventCandidatesFiltered) Code() types.EventCode { return types.CandidatesFilteredCode }
func (r RetrievalEventCandidatesFiltered) Phase() types.Phase    { return types.IndexerPhase }
func (r RetrievalEventCandidatesFiltered) String() string {
	return fmt.Sprintf("CandidatesFilteredEvent<%s, %s, %s, %d, %v>", r.eventTime, r.retrievalId, r.payloadCid, len(r.candidates), r.Protocols())
}
func (r RetrievalEventStarted) Code() types.EventCode { return types.StartedCode }
func (r RetrievalEventStarted) Phase() types.Phase    { return r.phase }
func (r RetrievalEventStarted) String() string {
	return fmt.Sprintf("StartedEvent<%s, %s, %s, %s, %s, %v>", r.phase, r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId, r.protocols)
}
func (r RetrievalEventConnected) Code() types.EventCode { return types.ConnectedCode }
func (r RetrievalEventConnected) Phase() types.Phase    { return r.phase }
func (r RetrievalEventConnected) String() string {
	return fmt.Sprintf("ConnectedEvent<%s, %s, %s, %s, %s, %v>", r.phase, r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId, r.protocols)
}
func (r RetrievalEventProposed) Code() types.EventCode { return types.ProposedCode }
func (r RetrievalEventProposed) Phase() types.Phase    { return types.RetrievalPhase }
func (r RetrievalEventProposed) String() string {
	return fmt.Sprintf("ProposedEvent<%s, %s, %s, %s, %v>", r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId, r.protocols)
}
func (r RetrievalEventAccepted) Code() types.EventCode { return types.AcceptedCode }
func (r RetrievalEventAccepted) Phase() types.Phase    { return types.RetrievalPhase }
func (r RetrievalEventAccepted) String() string {
	return fmt.Sprintf("AcceptedEvent<%s, %s, %s, %s, %v>", r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId, r.protocols)
}
func (r RetrievalEventFirstByte) Code() types.EventCode { return types.FirstByteCode }
func (r RetrievalEventFirstByte) Phase() types.Phase    { return types.RetrievalPhase }
func (r RetrievalEventFirstByte) String() string {
	return fmt.Sprintf("FirstByteEvent<%s, %s, %s, %s, %v>", r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId, r.protocols)
}
func (r RetrievalEventFailed) Code() types.EventCode { return types.FailedCode }
func (r RetrievalEventFailed) Phase() types.Phase    { return r.phase }

// ErrorMessage returns a string form of the error that caused the retrieval
// failure
func (r RetrievalEventFailed) ErrorMessage() string { return r.errorMessage }
func (r RetrievalEventFailed) String() string {
	return fmt.Sprintf("FailedEvent<%s, %s, %s, %s, %v, %s>", r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId, r.protocols, r.errorMessage)
}
func (r RetrievalEventSuccess) Code() types.EventCode           { return types.SuccessCode }
func (r RetrievalEventSuccess) Phase() types.Phase              { return types.RetrievalPhase }
func (r RetrievalEventSuccess) Duration() time.Duration         { return r.duration }
func (r RetrievalEventSuccess) TotalPayment() big.Int           { return r.totalPayment }
func (r RetrievalEventSuccess) BitswapPreloadedPercent() uint64 { return r.bitswapPreloadedPercent }

// ReceivedSize returns the number of bytes received
func (r RetrievalEventSuccess) ReceivedSize() uint64 { return r.receivedSize }

// ReceivedCids returns the number of (non-unique) CIDs received so far - note
// that a block can exist in more than one place in the DAG so this may not
// equal the total number of blocks transferred
func (r RetrievalEventSuccess) ReceivedCids() uint64 { return r.receivedCids }
func (r RetrievalEventSuccess) String() string {
	return fmt.Sprintf("SuccessEvent<%s, %s, %s, %s, %v, { %s, %s, %d, %d }>", r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId, r.protocols, r.duration, r.totalPayment, r.receivedSize, r.receivedCids)
}
func (r RetrievalEventFinished) Code() types.EventCode { return types.FinishedCode }
func (r RetrievalEventFinished) Phase() types.Phase    { return types.FetchPhase }
func (r RetrievalEventFinished) String() string {
	return fmt.Sprintf("FinishedEvent<%s, %s, %s, %s, %v>", r.eventTime, r.retrievalId, r.payloadCid, r.storageProviderId, r.protocols)
}
