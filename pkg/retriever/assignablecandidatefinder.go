package retriever

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/internal/candidatebuffer"
	"github.com/filecoin-project/lassie/pkg/types"
)

type FilterIndexerCandidate func(types.RetrievalCandidate) (bool, types.RetrievalCandidate)

// AssignableCandidateFinder finds and filters candidates for a given retrieval
type AssignableCandidateFinder struct {
	filterIndexerCandidate FilterIndexerCandidate
	candidateFinder        CandidateFinder
	clock                  clock.Clock
}

const BufferWindow = 5 * time.Millisecond

func NewAssignableCandidateFinder(candidateFinder CandidateFinder, filterIndexerCandidate FilterIndexerCandidate) AssignableCandidateFinder {
	return NewAssignableCandidateFinderWithClock(candidateFinder, filterIndexerCandidate, clock.New())
}
func NewAssignableCandidateFinderWithClock(candidateFinder CandidateFinder, filterIndexerCandidate FilterIndexerCandidate, clock clock.Clock) AssignableCandidateFinder {
	return AssignableCandidateFinder{candidateFinder: candidateFinder, filterIndexerCandidate: filterIndexerCandidate, clock: clock}
}
func (acf AssignableCandidateFinder) FindCandidates(ctx context.Context, request types.RetrievalRequest, eventsCallback func(types.RetrievalEvent), onCandidates func([]types.RetrievalCandidate)) error {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()
	phaseStarted := time.Now()

	eventsCallback(events.Started(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}))

	candidateStream, err := acf.candidateFinder.FindCandidatesAsync(ctx, request.Cid)

	if err != nil {
		eventsCallback(events.Failed(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}, err.Error()))
		return fmt.Errorf("could not get retrieval candidates for %s: %w", request.Cid, err)
	}

	var totalCandidates atomic.Uint64
	candidateBuffer := candidatebuffer.NewCandidateBuffer(func(candidates []types.RetrievalCandidate) {
		eventsCallback(events.CandidatesFound(request.RetrievalID, phaseStarted, request.Cid, candidates))

		acceptableCandidates := make([]types.RetrievalCandidate, 0)
		for _, candidate := range candidates {
			hasFilterCandidateFn := acf.filterIndexerCandidate != nil
			keepCandidate := true
			if hasFilterCandidateFn {
				keepCandidate, candidate = acf.filterIndexerCandidate(candidate)
			}
			if keepCandidate {
				acceptableCandidates = append(acceptableCandidates, candidate)
			}
		}

		if len(acceptableCandidates) == 0 {
			return
		}

		eventsCallback(events.CandidatesFiltered(request.RetrievalID, phaseStarted, request.Cid, acceptableCandidates))
		totalCandidates.Add(uint64(len(acceptableCandidates)))
		onCandidates(acceptableCandidates)
	}, acf.clock)

	err = candidateBuffer.BufferStream(ctx, candidateStream, BufferWindow)
	if err != nil {
		eventsCallback(events.Failed(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}, err.Error()))
		return err
	}
	if totalCandidates.Load() == 0 {
		eventsCallback(events.Failed(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}, ErrNoCandidates.Error()))
		return ErrNoCandidates
	}
	return nil
}
