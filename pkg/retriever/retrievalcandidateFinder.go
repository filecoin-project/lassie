package retriever

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
)

// RetrievalCandidateFinder finds and filters candidates for a given retrieval
type RetrievalCandidateFinder struct {
	isAcceptableStorageProvider IsAcceptableStorageProvider
	candidateFinder             CandidateFinder
}

func NewRetrievalCandidateFinder(candidateFinder CandidateFinder, isAcceptableStorageProvider IsAcceptableStorageProvider) *RetrievalCandidateFinder {
	return &RetrievalCandidateFinder{candidateFinder: candidateFinder, isAcceptableStorageProvider: isAcceptableStorageProvider}
}
func (rcf *RetrievalCandidateFinder) FindCandidates(ctx context.Context, request types.RetrievalRequest, eventsCallback func(types.RetrievalEvent)) (types.CandidateStream, error) {
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
		eventsCallback(events.Failed(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}, ErrNoCandidates.Error()))
		return nil, ErrNoCandidates
	}

	eventsCallback(events.CandidatesFound(request.RetrievalID, phaseStarted, request.Cid, candidates))

	acceptableCandidates := make([]types.RetrievalCandidate, 0)
	for _, candidate := range candidates {
		if rcf.isAcceptableStorageProvider == nil || rcf.isAcceptableStorageProvider(candidate.MinerPeer.ID) {
			acceptableCandidates = append(acceptableCandidates, candidate)
		}
	}

	if len(acceptableCandidates) == 0 {
		eventsCallback(events.Failed(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}, ErrNoCandidates.Error()))
		return nil, ErrNoCandidates
	}

	eventsCallback(events.CandidatesFiltered(request.RetrievalID, phaseStarted, request.Cid, acceptableCandidates))

	return acceptableCandidates, nil
}
