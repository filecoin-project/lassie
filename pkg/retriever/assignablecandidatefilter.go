package retriever

import (
	"context"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/internal/stream"
	"github.com/filecoin-project/lassie/pkg/types"
)

// AssignableCandidateFinder finds and filters candidates for a given retrieval
type AssignableCandidateFinder struct {
	candidateFinder             CandidateFinder
	isAcceptableStorageProvider IsAcceptableStorageProvider
}

func NewAssignableCandidateFinder(candidateFinder CandidateFinder, isAcceptableStorageProvider IsAcceptableStorageProvider) types.CandidateFinder {
	return &AssignableCandidateFinder{candidateFinder: candidateFinder, isAcceptableStorageProvider: isAcceptableStorageProvider}
}

func (rcf *AssignableCandidateFinder) FindCandidates(ctx context.Context, request types.RetrievalRequest, eventsCallback func(types.RetrievalEvent)) (types.CandidateStream, error, <-chan error) {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()
	phaseStarted := time.Now()

	eventsCallback(events.Started(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}))

	// start the call to get candidates
	individualCandidateChan, err := rcf.candidateFinder.FindCandidatesAsync(ctx, request.Cid)

	// if we get an immediate error, stop
	if err != nil {
		eventsCallback(events.Failed(request.RetrievalID, phaseStarted, types.IndexerPhase, types.RetrievalCandidate{RootCid: request.Cid}, err.Error()))
		return nil, fmt.Errorf("could not get retrieval candidates for %s: %w", request.Cid, err), nil
	}

	// convert our candidate result channel to a stream
	individualCandidateStream := stream.NewResultChannelStream(ctx, individualCandidateChan)

	// buffer candidates into sets (essentially, if they all arrive at once, let's group them)
	candidateStream := stream.BufferDebounce[types.RetrievalCandidate](ctx, individualCandidateStream, 2*time.Millisecond, clock.New())

	// run the assignable filter, mapping each candidate set to only the ones that are assignable
	assignableCandidateStream := stream.Map(candidateStream, func(candidates []types.RetrievalCandidate) ([]types.RetrievalCandidate, error) {
		eventsCallback(events.CandidatesFound(request.RetrievalID, phaseStarted, request.Cid, candidates))

		acceptableCandidates := make([]types.RetrievalCandidate, 0)
		for _, candidate := range candidates {
			if rcf.isAcceptableStorageProvider == nil || rcf.isAcceptableStorageProvider(candidate.MinerPeer.ID) {
				acceptableCandidates = append(acceptableCandidates, candidate)
			}
		}
		return acceptableCandidates, nil
	})
	// then filter out any sets that contain no assignable candidates
	assignableCandidateStream = stream.Filter(assignableCandidateStream, func(candidates []types.RetrievalCandidate) bool { return len(candidates) > 0 })

	// setup an observer on successful emissions
	observer, errChan := newCandidateObserver(ctx, request, eventsCallback, phaseStarted)

	// observe the stream and catch errors here -- so they don't filter down to the retrievers
	observedCandidateStream := stream.CatchError(stream.Observe(assignableCandidateStream, observer), nil)

	return observedCandidateStream, nil, errChan
}

func newCandidateObserver(ctx context.Context, request types.RetrievalRequest, eventsCallback func(types.RetrievalEvent), phaseStartTime time.Time) (types.StreamSubscriber[[]types.RetrievalCandidate], <-chan error) {
	co := &candidateObserver{
		phaseStartTime: phaseStartTime,
		ctx:            ctx,
		request:        request,
		eventsCallback: eventsCallback,
		errChan:        make(chan error, 1),
	}
	return co, co.errChan
}

type candidateObserver struct {
	phaseStartTime  time.Time
	ctx             context.Context
	request         types.RetrievalRequest
	eventsCallback  func(types.RetrievalEvent)
	errChan         chan error
	totalCandidates uint64
}

func (cc *candidateObserver) sendError(err error) {
	cc.eventsCallback(events.Failed(cc.request.RetrievalID, cc.phaseStartTime, types.IndexerPhase, types.RetrievalCandidate{RootCid: cc.request.Cid}, err.Error()))
	select {
	case cc.errChan <- err:
	default:
	}
}
func (cc *candidateObserver) Next(candidates []types.RetrievalCandidate) {
	// record any candidates that made it past the filter
	cc.eventsCallback(events.CandidatesFiltered(cc.request.RetrievalID, cc.phaseStartTime, cc.request.Cid, candidates))
	// add to the candidate count
	cc.totalCandidates += uint64(len(candidates))
}

func (cc *candidateObserver) Error(err error) {
	cc.sendError(err)
}

func (cc *candidateObserver) Complete() {
	// send an error if were no candidates observed
	if cc.totalCandidates == 0 {
		cc.sendError(ErrNoCandidates)
	}
}
