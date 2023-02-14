package retriever

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
)

// AssignableCandidateFinder finds and filters candidates for a given retrieval
type AssignableCandidateFinder struct {
	isAcceptableStorageProvider IsAcceptableStorageProvider
	candidateFinder             CandidateFinder
}

func NewAssignableCandidateFinder(candidateFinder CandidateFinder, isAcceptableStorageProvider IsAcceptableStorageProvider) AssignableCandidateFinder {
	return AssignableCandidateFinder{candidateFinder: candidateFinder, isAcceptableStorageProvider: isAcceptableStorageProvider}
}
func (rcf AssignableCandidateFinder) FindCandidates(ctx context.Context, request types.RetrievalRequest, eventsCallback func(types.RetrievalEvent)) ([]types.RetrievalCandidate, error) {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()
	phaseStarted := time.Now()

	eventsCallback(events.Started(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}))

	candidates, err := rcf.candidateFinder.FindCandidates(ctx, request.Cid)
	if err != nil {
		eventsCallback(events.Failed(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}, err.Error()))
		return nil, fmt.Errorf("could not get retrieval candidates for %s: %w", request.Cid, err)
	}

	if len(candidates) == 0 {
		eventsCallback(events.Failed(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}, types.ErrNoCandidates.Error()))
		return nil, types.ErrNoCandidates
	}

	eventsCallback(events.CandidatesFound(request.RetrievalID, phaseStarted, request.Cid, candidates))

	acceptableCandidates := make([]types.RetrievalCandidate, 0)
	for _, candidate := range candidates {
		if rcf.isAcceptableStorageProvider == nil || rcf.isAcceptableStorageProvider(candidate.MinerPeer.ID) {
			acceptableCandidates = append(acceptableCandidates, candidate)
		}
	}

	if len(acceptableCandidates) == 0 {
		eventsCallback(events.Failed(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}, types.ErrNoCandidates.Error()))
		return nil, types.ErrNoCandidates
	}

	eventsCallback(events.CandidatesFiltered(request.RetrievalID, phaseStarted, request.Cid, acceptableCandidates))

	return acceptableCandidates, nil
}
