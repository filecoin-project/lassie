package retriever

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
)

// WithCandidateFinding adds fetching from a candidate finder to convert a CandidateRetriever
// to a general retriever
func WithCandidateFinding(
	isAcceptableStorageProvider IsAcceptableStorageProvider,
	candidateFinder CandidateFinder,
	childRetriever types.CandidateRetriever,
) types.Retriever {
	return func(ctx context.Context, request types.RetrievalRequest, eventsCallback func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
		ctx, cancelCtx := context.WithCancel(ctx)
		defer cancelCtx()
		phaseStarted := time.Now()

		eventsCallback(events.Started(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}))

		candidates, err := candidateFinder.FindCandidates(ctx, request.Cid)
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
			if isAcceptableStorageProvider == nil || isAcceptableStorageProvider(candidate.MinerPeer.ID) {
				acceptableCandidates = append(acceptableCandidates, candidate)
			}
		}

		if len(acceptableCandidates) == 0 {
			eventsCallback(events.Failed(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}, ErrNoCandidates.Error()))
			return nil, ErrNoCandidates
		}

		eventsCallback(events.CandidatesFiltered(request.RetrievalID, phaseStarted, request.Cid, acceptableCandidates))

		return childRetriever(ctx, request, acceptableCandidates, eventsCallback)
	}
}
