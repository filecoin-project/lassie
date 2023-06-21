package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
)

var (
	_ types.RetrievalEvent = CandidatesFoundEvent{}
	_ EventWithPayloadCid  = CandidatesFoundEvent{}
	_ EventWithCandidates  = CandidatesFoundEvent{}
)

type CandidatesFoundEvent struct {
	retrievalEvent
	payloadCid cid.Cid
	candidates []types.RetrievalCandidate
}

func (e CandidatesFoundEvent) Code() types.EventCode                  { return types.CandidatesFoundCode }
func (e CandidatesFoundEvent) PayloadCid() cid.Cid                    { return e.payloadCid }
func (e CandidatesFoundEvent) Candidates() []types.RetrievalCandidate { return e.candidates }
func (e CandidatesFoundEvent) String() string {
	return fmt.Sprintf("CandidatesFoundEvent<%s, %s, %s, %d>", e.eventTime, e.retrievalId, e.payloadCid, len(e.candidates))
}

func CandidatesFound(at time.Time, retrievalId types.RetrievalID, payloadCid cid.Cid, candidates []types.RetrievalCandidate) CandidatesFoundEvent {
	c := make([]types.RetrievalCandidate, len(candidates))
	copy(c, candidates)
	return CandidatesFoundEvent{retrievalEvent{at, retrievalId}, payloadCid, c}
}
