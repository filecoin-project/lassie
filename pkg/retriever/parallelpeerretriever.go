package retriever

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/retriever/prioritywaitqueue"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

type GetStorageProviderTimeout func(peer peer.ID) time.Duration

// TransportProtocol implements the protocol-specific portions of a parallel-
// peer retriever. It is responsible for communicating with individual peers
// and also bears responsibility for some of the peer-selection logic.
type TransportProtocol interface {
	Code() multicodec.Code
	GetMergedMetadata(cid cid.Cid, currentMetadata, newMetadata metadata.Protocol) metadata.Protocol
	Connect(ctx context.Context, retrieval *retrieval, candidate types.RetrievalCandidate) (time.Duration, error)
	Retrieve(
		ctx context.Context,
		retrieval *retrieval,
		shared *retrievalShared,
		timeout time.Duration,
		candidate types.RetrievalCandidate,
	) (*types.RetrievalStats, error)
}

var (
	_ types.CandidateRetriever = (*parallelPeerRetriever)(nil)
	_ types.CandidateRetrieval = (*retrieval)(nil)
)

// parallelPeerRetriever is an abstract utility type that implements a retrieval
// flow that retrieves from multiple peers separately but needs to manage that
// flow in parallel. Unlike a Bitswap retrieval, in which all peers are managed
// as a group and may all be collectively retrieved from, parallelPeerRetriever
// is used for protocols where a retrieval is performed directly with a single
// peer, but many peers may be prioritised for attempts.
//
// The concrete implementation of the retrieval protocol is provided by the
// TransportProtocol interface. parallelPeerRetriever manages candidates and
// the parallel+serial flow of connect+retrieve.
type parallelPeerRetriever struct {
	Protocol          TransportProtocol
	Session           Session
	Clock             clock.Clock
	QueueInitialPause time.Duration

	// --- These are *only* for testing purposes ---
	// awaitReceivedCandidates helps ensure that we receive all candidates
	awaitReceivedCandidates chan<- struct{}
	// noDirtyClose will prevent the parallelPeerRetriever from closing the
	// retrieval until all parallel attempts have completed, even with context
	// cancellation. Default behavior is to be permissive, waiting up to 100ms
	// or for context cancellation.
	noDirtyClose bool
}

// retrieval handles state on a per-retrieval (across multiple candidates) basis
type retrieval struct {
	*parallelPeerRetriever
	ctx                context.Context
	request            types.RetrievalRequest
	eventsCallback     func(types.RetrievalEvent)
	candidateMetadata  map[peer.ID]metadata.Protocol
	candidateMetdataLk sync.RWMutex
}

type retrievalResult struct {
	PeerID      peer.ID
	Stats       *types.RetrievalStats
	Event       *types.RetrievalEvent
	Err         error
	AllFinished bool
}

func (cfg *parallelPeerRetriever) Retrieve(
	ctx context.Context,
	retrievalRequest types.RetrievalRequest,
	eventsCallback func(types.RetrievalEvent),
) types.CandidateRetrieval {
	if eventsCallback == nil {
		eventsCallback = func(re types.RetrievalEvent) {}
	}
	return &retrieval{
		parallelPeerRetriever: cfg,
		ctx:                   ctx,
		request:               retrievalRequest,
		eventsCallback:        eventsCallback,
		candidateMetadata:     make(map[peer.ID]metadata.Protocol),
	}
}

func (retrieval *retrieval) RetrieveFromAsyncCandidates(asyncCandidates types.InboundAsyncCandidates) (*types.RetrievalStats, error) {
	ctx, cancelCtx := context.WithCancel(retrieval.ctx)

	pwqOpts := []prioritywaitqueue.Option[peer.ID]{prioritywaitqueue.WithClock[peer.ID](retrieval.Clock)}
	if retrieval.QueueInitialPause > 0 {
		pwqOpts = append(pwqOpts, prioritywaitqueue.WithInitialPause[peer.ID](retrieval.QueueInitialPause))
	}

	shared := newRetrievalShared(prioritywaitqueue.New(retrieval.candidateChooser, pwqOpts...))

	// start retrievals
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		for {
			active, candidates, err := retrieval.filterCandidates(ctx, asyncCandidates)
			if !active || err != nil {
				return
			}
			for _, candidate := range candidates {
				// start the retrieval with the candidate
				candidate := candidate
				waitGroup.Add(1)
				go func() {
					defer waitGroup.Done()
					retrieval.runRetrievalCandidate(ctx, shared, candidate)
				}()
			}
		}
	}()

	finishAll := make(chan struct{}, 1)
	go func() {
		waitGroup.Wait()
		shared.sendAllFinished(ctx)
		finishAll <- struct{}{}
	}()

	eventsCallback := func(evt types.RetrievalEvent) {
		switch ret := evt.(type) {
		case events.FirstByteEvent:
			retrieval.Session.RecordFirstByteTime(ret.ProviderId(), ret.Duration())
		}
		retrieval.eventsCallback(evt)
	}

	stats, err := collectResults(ctx, shared, eventsCallback)
	cancelCtx()
	// optimistically try to wait for all routines to finish
	if retrieval.noDirtyClose {
		<-finishAll
	} else {
		select {
		case <-finishAll:
		case <-retrieval.Clock.After(100 * time.Millisecond):
			logger.Errorf(
				"Possible leak: unable to successfully cancel all %s retrieval attempts for %s within 100ms",
				retrieval.Protocol.Code().String(),
				retrieval.request.Root.String(),
			)
		}
	}
	return stats, err
}

// candidateChooser selects the candidate to run next from a list of candidates.
// This is used for the PriorityWaitQueue that manages the order of candidates
// to run.
// PriorityWaitQueue should only call this when there are >1 candidates to
// choose from.
func (retrieval *retrieval) candidateChooser(peers []peer.ID) int {
	metadata := make([]metadata.Protocol, 0, len(peers))
	retrieval.candidateMetdataLk.RLock()
	for _, p := range peers {
		md := retrieval.candidateMetadata[p]
		metadata = append(metadata, md)
	}
	retrieval.candidateMetdataLk.RUnlock()

	return retrieval.Session.ChooseNextProvider(peers, metadata)
}

// filterCandidates is needed because we can receive duplicate candidates in
// a single batch or across different batches. We need to filter out duplicates
// and make sure we have the best information from candidate metadata across
// those duplicates.
func (retrieval *retrieval) filterCandidates(ctx context.Context, asyncCandidates types.InboundAsyncCandidates) (bool, []types.RetrievalCandidate, error) {
	filtered := make([]types.RetrievalCandidate, 0)
	active, candidates, err := asyncCandidates.Next(ctx)
	if !active || err != nil {
		if retrieval.awaitReceivedCandidates != nil {
			select {
			case <-retrieval.ctx.Done():
			case retrieval.awaitReceivedCandidates <- struct{}{}:
			}
		}
		return false, nil, err
	}

	retrieval.candidateMetdataLk.Lock()
	defer retrieval.candidateMetdataLk.Unlock()

	for _, candidate := range candidates {
		// update or add new candidate metadata
		currMetadata, seenCandidate := retrieval.candidateMetadata[candidate.MinerPeer.ID]
		newMetadata := candidate.Metadata.Get(multicodec.Code(retrieval.Protocol.Code()))
		candidateMetadata := retrieval.Protocol.GetMergedMetadata(retrieval.request.Root, currMetadata, newMetadata)
		retrieval.candidateMetadata[candidate.MinerPeer.ID] = candidateMetadata
		// if it's a new candidate, include it, otherwise don't start a new retrieval for it
		if !seenCandidate {
			filtered = append(filtered, candidate)
		}
	}

	return true, filtered, nil
}

// runRetrievalCandidate is a singular CID:SP retrieval, expected to be run in a goroutine
// and coordinate with other candidate retrievals to block and only attempt one
// retrieval-proper at a time.
func (retrieval *retrieval) runRetrievalCandidate(
	ctx context.Context,
	shared *retrievalShared,
	candidate types.RetrievalCandidate,
) {
	timeout := retrieval.Session.GetStorageProviderTimeout(candidate.MinerPeer.ID)

	var stats *types.RetrievalStats
	var retrievalErr error
	var done func()

	shared.sendEvent(ctx, events.StartedRetrieval(retrieval.parallelPeerRetriever.Clock.Now(), retrieval.request.RetrievalID, candidate, retrieval.Protocol.Code()))
	connectCtx := ctx
	if timeout != 0 {
		var timeoutFunc func()
		connectCtx, timeoutFunc = retrieval.parallelPeerRetriever.Clock.WithDeadline(ctx, retrieval.parallelPeerRetriever.Clock.Now().Add(timeout))
		defer timeoutFunc()
	}

	// Setup in parallel
	connectTime, err := retrieval.Protocol.Connect(connectCtx, retrieval, candidate)
	if err != nil {
		// Exclude the case where the context was cancelled by the parent, which likely means that
		// another protocol has succeeded.
		if !errors.Is(ctx.Err(), context.Canceled) {
			logger.Warnf("Failed to connect to SP %s on protocol %s: %v", candidate.MinerPeer.ID, retrieval.Protocol.Code().String(), err)
			retrievalErr = fmt.Errorf("%w: %v", ErrConnectFailed, err)
			if err := retrieval.Session.RecordFailure(retrieval.request.RetrievalID, candidate.MinerPeer.ID); err != nil {
				logger.Errorf("Error recording retrieval failure on protocol %s: %v", retrieval.Protocol.Code().String(), err)
			}
			shared.sendEvent(ctx, events.FailedRetrieval(retrieval.parallelPeerRetriever.Clock.Now(), retrieval.request.RetrievalID, candidate, retrieval.Protocol.Code(), retrievalErr.Error()))
		}
	} else {
		shared.sendEvent(ctx, events.ConnectedToProvider(retrieval.parallelPeerRetriever.Clock.Now(), retrieval.request.RetrievalID, candidate, retrieval.Protocol.Code()))

		retrieval.Session.RecordConnectTime(candidate.MinerPeer.ID, connectTime)

		// Form a queue and run retrieval in serial
		done = shared.waitQueue.Wait(candidate.MinerPeer.ID)

		if shared.canSendResult() { // move on to retrieval
			stats, retrievalErr = retrieval.Protocol.Retrieve(ctx, retrieval, shared, timeout, candidate)

			if retrievalErr != nil {
				// Exclude the case where the context was cancelled by the parent, which likely
				// means that another protocol has succeeded.
				if !errors.Is(ctx.Err(), context.Canceled) {
					msg := retrievalErr.Error()
					if errors.Is(retrievalErr, ErrRetrievalTimedOut) {
						msg = fmt.Sprintf("%s after %s", ErrRetrievalTimedOut.Error(), timeout)
					}
					shared.sendEvent(ctx, events.FailedRetrieval(retrieval.parallelPeerRetriever.Clock.Now(), retrieval.request.RetrievalID, candidate, retrieval.Protocol.Code(), msg))
					if err := retrieval.Session.RecordFailure(retrieval.request.RetrievalID, candidate.MinerPeer.ID); err != nil {
						logger.Errorf("Error recording retrieval failure for protocol %s: %v", retrieval.Protocol.Code().String(), err)
					}
				}
			} else {
				shared.sendEvent(ctx, events.Success(
					retrieval.parallelPeerRetriever.Clock.Now(),
					retrieval.request.RetrievalID,
					candidate,
					stats.Size,
					stats.Blocks,
					stats.Duration,
					retrieval.Protocol.Code(),
				))
				seconds := stats.Duration.Seconds()
				if seconds == 0 { // avoid a divide by zero
					seconds = 1
				}
				bandwidthBytesPerSecond := float64(stats.Size) / seconds
				retrieval.Session.RecordSuccess(candidate.MinerPeer.ID, uint64(bandwidthBytesPerSecond))
			}
		} // else we didn't get to retrieval because we were cancelled
	}

	if shared.canSendResult() {
		if retrievalErr != nil {
			if ctx.Err() != nil { // cancelled, don't report the error
				shared.sendResult(ctx, retrievalResult{PeerID: candidate.MinerPeer.ID})
			} else {
				// an error of some kind to report
				shared.sendResult(ctx, retrievalResult{PeerID: candidate.MinerPeer.ID, Err: retrievalErr})
			}
		} else { // success, we have stats and no errors
			shared.sendResult(ctx, retrievalResult{PeerID: candidate.MinerPeer.ID, Stats: stats})
		}
	} // else nothing to do, we were cancelled

	if done != nil {
		done() // allow prioritywaitqueue to move on to next candidate
	}
}
