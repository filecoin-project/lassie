package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
)

var (
	_ types.RetrievalEvent = CandidatesFilteredEvent{}
	_ EventWithCandidates  = CandidatesFilteredEvent{}
	_ EventWithProtocols   = CandidatesFilteredEvent{}
)

type CandidatesFilteredEvent struct {
	retrievalEvent
	candidates []types.RetrievalCandidate
	protocols  []multicodec.Code
}

func (e CandidatesFilteredEvent) Code() types.EventCode                  { return types.CandidatesFilteredCode }
func (e CandidatesFilteredEvent) Candidates() []types.RetrievalCandidate { return e.candidates }
func (e CandidatesFilteredEvent) Protocols() []multicodec.Code           { return e.protocols }
func (e CandidatesFilteredEvent) String() string {
	return fmt.Sprintf("CandidatesFilteredEvent<%s, %s, %s, %d>", e.eventTime, e.retrievalId, e.rootCid, len(e.candidates))
}

func CandidatesFiltered(at time.Time, retrievalId types.RetrievalID, rootCid cid.Cid, candidates []types.RetrievalCandidate) CandidatesFilteredEvent {
	c := make([]types.RetrievalCandidate, len(candidates))
	copy(c, candidates)
	return CandidatesFilteredEvent{retrievalEvent{at, retrievalId, rootCid}, c, collectProtocols(c)}
}
