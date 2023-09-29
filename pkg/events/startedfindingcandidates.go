package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
)

var _ types.RetrievalEvent = StartedFindingCandidatesEvent{}

// StartedFindingCandidatesEvent signals the start of finding candidates for a fetch.
type StartedFindingCandidatesEvent struct {
	retrievalEvent
}

func (e StartedFindingCandidatesEvent) Code() types.EventCode {
	return types.StartedFindingCandidatesCode
}
func (e StartedFindingCandidatesEvent) String() string {
	return fmt.Sprintf("StartedFindingCandidatesEvent<%s, %s, %s>", e.eventTime, e.retrievalId, e.rootCid)
}

func StartedFindingCandidates(at time.Time, retrievalId types.RetrievalID, rootCid cid.Cid) StartedFindingCandidatesEvent {
	return StartedFindingCandidatesEvent{retrievalEvent{at, retrievalId, rootCid}}
}
