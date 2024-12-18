package retriever

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/internal/candidatebuffer"
	"github.com/filecoin-project/lassie/pkg/types"
)

type FilterIndexerCandidate func(types.RetrievalCandidate) (bool, types.RetrievalCandidate)

// AssignableCandidateFinder finds and filters candidates for a given retrieval
type AssignableCandidateFinder struct {
	filterIndexerCandidate FilterIndexerCandidate
	candidateSource        types.CandidateSource
	clock                  clock.Clock
}

const BufferWindow = 5 * time.Millisecond

func NewAssignableCandidateFinder(candidateSource types.CandidateSource, filterIndexerCandidate FilterIndexerCandidate) AssignableCandidateFinder {
	return NewAssignableCandidateFinderWithClock(candidateSource, filterIndexerCandidate, clock.New())
}
func NewAssignableCandidateFinderWithClock(candidateSource types.CandidateSource, filterIndexerCandidate FilterIndexerCandidate, clock clock.Clock) AssignableCandidateFinder {
	return AssignableCandidateFinder{candidateSource: candidateSource, filterIndexerCandidate: filterIndexerCandidate, clock: clock}
}
func (acf AssignableCandidateFinder) FindCandidates(ctx context.Context, request types.RetrievalRequest, eventsCallback func(types.RetrievalEvent), onCandidates func([]types.RetrievalCandidate)) error {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	eventsCallback(events.StartedFindingCandidates(acf.clock.Now(), request.RetrievalID, request.Root))

	var totalCandidates atomic.Uint64
	candidateBuffer := candidatebuffer.NewCandidateBuffer(func(candidates []types.RetrievalCandidate) {
		eventsCallback(events.CandidatesFound(acf.clock.Now(), request.RetrievalID, request.Root, candidates))

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

		eventsCallback(events.CandidatesFiltered(acf.clock.Now(), request.RetrievalID, request.Root, acceptableCandidates))
		totalCandidates.Add(uint64(len(acceptableCandidates)))
		onCandidates(acceptableCandidates)
	}, acf.clock)

	candidateSource := acf.candidateSource
	if len(request.Providers) > 0 {
		candidateSource = NewDirectCandidateSource(request.Providers)
	}
	err := candidateBuffer.BufferStream(ctx, func(ctx context.Context, onNextCandidate candidatebuffer.OnNextCandidate) error {
		return candidateSource.FindCandidates(ctx, request.Root, onNextCandidate)
	}, BufferWindow)

	if err != nil {
		eventsCallback(events.Failed(acf.clock.Now(), request.RetrievalID, types.RetrievalCandidate{RootCid: request.Root}, err.Error()))
		return fmt.Errorf("could not get retrieval candidates for %s: %w", request.Root, err)
	}

	if totalCandidates.Load() == 0 {
		eventsCallback(events.Failed(acf.clock.Now(), request.RetrievalID, types.RetrievalCandidate{RootCid: request.Root}, ErrNoCandidates.Error()))
		return ErrNoCandidates
	}
	return nil
}
