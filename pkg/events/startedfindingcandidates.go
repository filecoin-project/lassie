package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
)

var (
	_ types.RetrievalEvent = StartedFindingCandidatesEvent{}
	_ EventWithPayloadCid  = StartedFindingCandidatesEvent{}
)

// StartedFindingCandidatesEvent signals the start of finding candidates for a fetch.
type StartedFindingCandidatesEvent struct {
	retrievalEvent
	payloadCid cid.Cid
}

func (e StartedFindingCandidatesEvent) Code() types.EventCode {
	return types.StartedFindingCandidatesCode
}
func (e StartedFindingCandidatesEvent) PayloadCid() cid.Cid { return e.payloadCid }
func (e StartedFindingCandidatesEvent) String() string {
	return fmt.Sprintf("StartedFindingCandidatesEvent<%s, %s, %s>", e.eventTime, e.retrievalId, e.payloadCid)
}

func StartedFindingCandidates(at time.Time, retrievalId types.RetrievalID, payloadCid cid.Cid) StartedFindingCandidatesEvent {
	return StartedFindingCandidatesEvent{retrievalEvent{at, retrievalId}, payloadCid}
}
