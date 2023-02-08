package retriever

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/libp2p/go-libp2p/core/peer"
)

type IsAcceptableCandidate func(peer peer.ID) bool

// AcceptableCandidateFinder finds and filters candidates for a given retrieval
type AcceptableCandidateFinder struct {
	candidateFinder       types.CandidateFinder
	isAcceptableCandidate IsAcceptableCandidate
}

func NewAcceptableCandidateFinder(candidateFinder types.CandidateFinder, isAcceptableCandidate IsAcceptableCandidate) *AcceptableCandidateFinder {
	return &AcceptableCandidateFinder{candidateFinder: candidateFinder, isAcceptableCandidate: isAcceptableCandidate}
}

func (rcf *AcceptableCandidateFinder) FindCandidates(ctx context.Context, request types.RetrievalRequest, eventsCallback func(types.RetrievalEvent)) ([]types.RetrievalCandidate, error) {
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
		if rcf.isAcceptableCandidate == nil || rcf.isAcceptableCandidate(candidate.MinerPeer.ID) {
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

func (rcf *AcceptableCandidateFinder) FindCandidatesAsync(ctx context.Context, request types.RetrievalRequest, eventsCallback func(types.RetrievalEvent)) (<-chan types.FindCandidatesResult, error) {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()
	phaseStarted := time.Now()

	eventsCallback(events.Started(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}))

	candidatesChan, err := rcf.candidateFinder.FindCandidatesAsync(ctx, request.Cid)
	if candidatesChan == nil && err == nil {
		eventsCallback(events.Failed(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}, ErrNoCandidates.Error()))
		return nil, ErrNoCandidates
	}

	if err != nil {
		eventsCallback(events.Failed(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}, err.Error()))
		return nil, fmt.Errorf("could not get retrieval candidates for %s: %w", request.Cid, err)
	}

	ch := make(chan types.FindCandidatesResult, 1)
	go func() {
		defer close(ch)

		// I'm unsure what the "all finished" signal is
		counter := 0
		for {
			select {
			case <-ctx.Done():
				break
			case result := <-candidatesChan:
				eventsCallback(events.CandidateFound(request.RetrievalID, phaseStarted, request.Cid, result.Candidate))
				if rcf.isAcceptableCandidate == nil || rcf.isAcceptableCandidate(result.Candidate.MinerPeer.ID) {
					eventsCallback(events.CandidateFiltered(request.RetrievalID, phaseStarted, request.Cid, result.Candidate))
					counter++
					ch <- result
				}
			default:
			}
		}

		if counter == 0 {
			eventsCallback(events.Failed(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}, ErrNoCandidates.Error()))
			return
		}
	}()
	return ch, nil
}
