package retriever

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/retriever/prioritywaitqueue"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"go.uber.org/multierr"
)

type CounterCallback func(int) error
type CandidateCallback func(types.RetrievalCandidate) error
type CandidateErrorCallback func(types.RetrievalCandidate, error)

type GetStorageProviderTimeout func(peer peer.ID) time.Duration

type GraphSyncRetriever struct {
	GetStorageProviderTimeout GetStorageProviderTimeout
	Client                    RetrievalClient
	Clock                     clock.Clock
	QueueInitialPause         time.Duration
}

func NewGraphsyncRetriever(getStorageProviderTimeout GetStorageProviderTimeout, client RetrievalClient) *GraphSyncRetriever {
	return &GraphSyncRetriever{
		GetStorageProviderTimeout: getStorageProviderTimeout,
		Client:                    client,
		Clock:                     clock.New(),
		QueueInitialPause:         2 * time.Millisecond,
	}
}

// metadataCompare compares two metadata.GraphsyncFilecoinV1s and returns true if the first is preferable to the second.
func metadataCompare(a, b metadata.GraphsyncFilecoinV1) bool {
	// prioritize verified deals over not verified deals
	if a.VerifiedDeal != b.VerifiedDeal {
		return a.VerifiedDeal
	}

	// prioritize fast retrievel over not fast retrieval
	if a.FastRetrieval && !b.FastRetrieval {
		return true
	}

	return false
}

type retrievalResult struct {
	PeerID     peer.ID
	PhaseStart time.Time
	Stats      *types.RetrievalStats
	Event      *types.RetrievalEvent
	Err        error
}

// retrieval handles state on a per-retrieval (across multiple candidates) basis
type graphsyncRetrieval struct {
	*GraphSyncRetriever
	ctx                context.Context
	request            types.RetrievalRequest
	eventsCallback     func(types.RetrievalEvent)
	candidateMetadata  map[peer.ID]metadata.GraphsyncFilecoinV1
	candidateMetdataLk sync.RWMutex
}

type connectCandidate struct {
	PeerID   peer.ID
	Duration time.Duration
}

type graphsyncCandidateRetrieval struct {
	waitQueue  prioritywaitqueue.PriorityWaitQueue[connectCandidate]
	resultChan chan retrievalResult
	finishChan chan struct{}
}

// RetrieveFromCandidates performs a retrieval for a given CID by querying the indexer, then
// attempting to query all candidates and attempting to perform a full retrieval
// from the best and fastest storage provider as the queries are received.
func (cfg *GraphSyncRetriever) Retrieve(
	ctx context.Context,
	retrievalRequest types.RetrievalRequest,
	eventsCallback func(types.RetrievalEvent),
) types.CandidateRetrieval {

	if cfg == nil {
		cfg = &GraphSyncRetriever{}
	}
	if eventsCallback == nil {
		eventsCallback = func(re types.RetrievalEvent) {}
	}

	// state local to this CID's retrieval
	return &graphsyncRetrieval{
		GraphSyncRetriever: cfg,
		ctx:                ctx,
		request:            retrievalRequest,
		eventsCallback:     eventsCallback,
		candidateMetadata:  make(map[peer.ID]metadata.GraphsyncFilecoinV1),
	}
}

// candidateCompare compares two connectCandidates and returns true if the first is
// preferable to the second. This is used for the PriorityWaitQueue that will
// prioritise execution of retrievals if two candidates are available to compare
// at the same time.
func (r *graphsyncRetrieval) candidateCompare(a, b connectCandidate) bool {
	r.candidateMetdataLk.RLock()
	defer r.candidateMetdataLk.RUnlock()

	mdA, ok := r.candidateMetadata[a.PeerID]
	if !ok {
		return false
	}

	mdB, ok := r.candidateMetadata[b.PeerID]
	if !ok {
		return true
	}

	if metadataCompare(mdA, mdB) {
		return true
	}

	return a.Duration < b.Duration
}

func (r *graphsyncRetrieval) RetrieveFromAsyncCandidates(asyncCandidates types.InboundAsyncCandidates) (*types.RetrievalStats, error) {
	ctx, cancelCtx := context.WithCancel(r.ctx)

	retrieval := &graphsyncCandidateRetrieval{
		resultChan: make(chan retrievalResult),
		finishChan: make(chan struct{}),
		waitQueue: prioritywaitqueue.New(
			r.candidateCompare,
			prioritywaitqueue.WithInitialPause[connectCandidate](r.QueueInitialPause),
			prioritywaitqueue.WithClock[connectCandidate](r.Clock),
		),
	}

	// start retrievals
	phaseStartTime := r.Clock.Now()
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		for {
			hasCandidates, candidates, err := asyncCandidates.Next(ctx)
			if !hasCandidates || err != nil {
				return
			}
			for _, candidate := range candidates {
				// Check if we already started a retrieval for this candidate
				r.candidateMetdataLk.RLock()
				currMetadata, seenCandidate := r.candidateMetadata[candidate.MinerPeer.ID]
				r.candidateMetdataLk.RUnlock()

				// Grab the current candidate's metadata, adding the piece cid to the metadata if the type assertion failed
				candidateMetadata, ok := candidate.Metadata.Get(multicodec.TransportGraphsyncFilecoinv1).(*metadata.GraphsyncFilecoinV1)
				if !ok {
					candidateMetadata = &metadata.GraphsyncFilecoinV1{PieceCID: r.request.Cid}
				}

				// Don't start a new retrieval if we've seen this candidate before,
				// but update the metadata if it's more favorable
				if seenCandidate {
					// We know the metadata is not as favorable if the type assertion failed
					// since the metadata will be the zero value of graphsync metadata
					if !ok {
						continue
					}

					if metadataCompare(*candidateMetadata, currMetadata) {
						r.candidateMetdataLk.Lock()
						r.candidateMetadata[candidate.MinerPeer.ID] = *candidateMetadata
						r.candidateMetdataLk.Unlock()
					}
					continue
				}

				// Track the candidate metadata
				r.candidateMetdataLk.Lock()
				r.candidateMetadata[candidate.MinerPeer.ID] = *candidateMetadata
				r.candidateMetdataLk.Unlock()

				// Start the retrieval with the candidate
				candidate := candidate
				waitGroup.Add(1)
				go func() {
					defer waitGroup.Done()
					runRetrievalCandidate(ctx, r.GraphSyncRetriever, r.request, r.Client, r.request.LinkSystem, retrieval, phaseStartTime, candidate)
				}()
			}
		}
	}()

	finishAll := make(chan struct{}, 1)
	go func() {
		waitGroup.Wait()
		close(retrieval.resultChan)
		finishAll <- struct{}{}
	}()

	stats, err := collectResults(ctx, retrieval, r.eventsCallback)
	cancelCtx()
	// optimistically try to wait for all routines to finish
	select {
	case <-finishAll:
	case <-time.After(100 * time.Millisecond):
		log.Warn("Unable to successfully cancel all retrieval attempts withing 100ms")
	}
	return stats, err
}

// collectResults is responsible for receiving query errors, retrieval errors
// and retrieval results and aggregating into an appropriate return of either
// a complete RetrievalStats or an bundled multi-error
func collectResults(ctx context.Context, retrieval *graphsyncCandidateRetrieval, eventsCallback func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
	var retrievalErrors error
	for {
		select {
		case result, ok := <-retrieval.resultChan:
			// have we got all responses but no success?
			if !ok {
				// we failed, and got only retrieval errors
				retrievalErrors = multierr.Append(retrievalErrors, ErrAllRetrievalsFailed)
				return nil, retrievalErrors
			}

			if result.Event != nil {
				eventsCallback(*result.Event)
				break
			}
			if result.Err != nil {
				retrievalErrors = multierr.Append(retrievalErrors, result.Err)
			}
			if result.Stats != nil {
				return result.Stats, nil
			}
		case <-ctx.Done():
			return nil, context.Canceled
		}
	}
}

// runRetrievalCandidate is a singular CID:SP retrieval, expected to be run in a goroutine
// and coordinate with other candidate retrievals to block after query phase and
// only attempt one retrieval-proper at a time.
func runRetrievalCandidate(
	ctx context.Context,
	cfg *GraphSyncRetriever,
	req types.RetrievalRequest,
	client RetrievalClient,
	linkSystem ipld.LinkSystem,
	retrieval *graphsyncCandidateRetrieval,
	phaseStartTime time.Time,
	candidate types.RetrievalCandidate,
) {

	var timeout time.Duration
	if cfg.GetStorageProviderTimeout != nil {
		timeout = cfg.GetStorageProviderTimeout(candidate.MinerPeer.ID)
	}

	var stats *types.RetrievalStats
	var retrievalErr error
	var done func()

	retrieval.sendEvent(events.Started(cfg.Clock.Now(), req.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate))
	connectCtx := ctx
	if timeout != 0 {
		var timeoutFunc func()
		connectCtx, timeoutFunc = context.WithDeadline(ctx, time.Now().Add(timeout))
		defer timeoutFunc()
	}

	err := client.Connect(connectCtx, candidate.MinerPeer)

	if err != nil {
		if ctx.Err() == nil { // not cancelled, maybe timed out though
			log.Warnf("Failed to connect to miner %s: %v", candidate.MinerPeer.ID, err)
			retrievalErr = fmt.Errorf("%w: %v", ErrConnectFailed, err)
			retrieval.sendEvent(events.Failed(cfg.Clock.Now(), req.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, retrievalErr.Error()))
		}
	} else {
		retrieval.sendEvent(events.Connected(cfg.Clock.Now(), req.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate))
		// if query is successful, then wait for priority and execute retrieval
		done = retrieval.waitQueue.Wait(connectCandidate{
			PeerID:   candidate.MinerPeer.ID,
			Duration: cfg.Clock.Now().Sub(phaseStartTime),
		})

		if retrieval.canSendResult() { // move on to retrieval phase
			var receivedFirstByte bool
			eventsCallback := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				switch event.Code {
				case datatransfer.Open:
					retrieval.sendEvent(events.Proposed(cfg.Clock.Now(), req.RetrievalID, phaseStartTime, candidate))
				case datatransfer.NewVoucherResult:
					lastVoucher := channelState.LastVoucherResult()
					resType, err := retrievaltypes.DealResponseFromNode(lastVoucher.Voucher)
					if err != nil {
						return
					}
					if resType.Status == retrievaltypes.DealStatusAccepted {
						retrieval.sendEvent(events.Accepted(cfg.Clock.Now(), req.RetrievalID, phaseStartTime, candidate))
					}
				case datatransfer.DataReceivedProgress:
					if !receivedFirstByte {
						receivedFirstByte = true
						retrieval.sendEvent(events.FirstByte(cfg.Clock.Now(), req.RetrievalID, phaseStartTime, candidate))
					}
				}
			}

			stats, retrievalErr = retrievalPhase(
				ctx,
				cfg,
				client,
				linkSystem,
				timeout,
				candidate,
				req.GetSelector(),
				eventsCallback,
			)

			if retrievalErr != nil {
				msg := retrievalErr.Error()
				if errors.Is(retrievalErr, ErrRetrievalTimedOut) {
					msg = fmt.Sprintf("timeout after %s", timeout)
				}
				retrieval.sendEvent(events.Failed(cfg.Clock.Now(), req.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, msg))
			} else {
				retrieval.sendEvent(events.Success(
					cfg.Clock.Now(),
					req.RetrievalID,
					phaseStartTime,
					candidate,
					stats.Size,
					stats.Blocks,
					stats.Duration,
					stats.TotalPayment,
					0,
				),
				)
			}
		} // else we didn't get to retrieval phase because we were cancelled
	}

	if retrieval.canSendResult() {
		// as long as collectResults is still running, we need to increment finishedCount by
		// sending a retrievalResult, so each path here sends one in some form
		if retrievalErr != nil {
			if ctx.Err() != nil { // cancelled, don't report the error
				retrieval.sendResult(retrievalResult{PhaseStart: phaseStartTime, PeerID: candidate.MinerPeer.ID})
			} else {
				// an error of some kind to report
				retrieval.sendResult(retrievalResult{PhaseStart: phaseStartTime, PeerID: candidate.MinerPeer.ID, Err: retrievalErr})
			}
		} else { // success, we have stats and no errors
			retrieval.sendResult(retrievalResult{PhaseStart: phaseStartTime, PeerID: candidate.MinerPeer.ID, Stats: stats})
		}
	} // else nothing to do, we were cancelled

	if done != nil {
		done() // allow prioritywaitqueue to move on to next candidate
	}
}

// canSendResult will indicate whether a result is likely to be accepted (true)
// or whether the retrieval is already finished (likely by a success)
func (retrieval *graphsyncCandidateRetrieval) canSendResult() bool {
	select {
	case <-retrieval.finishChan:
		return false
	default:
	}
	return true
}

// sendResult will only send a result to the parent goroutine if a retrieval has
// finished (likely by a success), otherwise it will send the result
func (retrieval *graphsyncCandidateRetrieval) sendResult(result retrievalResult) bool {
	select {
	case <-retrieval.finishChan:
		return false
	case retrieval.resultChan <- result:
		if result.Stats != nil {
			// signals to goroutines to bail, this has to be done here, rather than on
			// the receiving parent end, because immediately after this call we instruct
			// the prioritywaitqueue that we're done and another may start
			close(retrieval.finishChan)
		}
	}
	return true
}

func (retrieval *graphsyncCandidateRetrieval) sendEvent(event types.RetrievalEvent) {
	retrieval.sendResult(retrievalResult{PeerID: event.StorageProviderId(), Event: &event})
}

func retrievalPhase(
	ctx context.Context,
	cfg *GraphSyncRetriever,
	client RetrievalClient,
	linkSystem ipld.LinkSystem,
	timeout time.Duration,
	candidate types.RetrievalCandidate,
	selector ipld.Node,
	eventsCallback datatransfer.Subscriber,
) (*types.RetrievalStats, error) {

	ss := "*"
	if selector != selectorparse.CommonSelector_ExploreAllRecursively {
		byts, err := ipld.Encode(selector, dagjson.Encode)
		if err != nil {
			return nil, err
		}
		ss = string(byts)
	}

	log.Infof(
		"Attempting retrieval from miner %s for %s (with selector: [%s])",
		candidate.MinerPeer.ID,
		candidate.RootCid,
		ss,
	)

	params, err := retrievaltypes.NewParamsV1(big.Zero(), 0, 0, selector, nil, big.Zero())
	if err != nil {
		return nil, multierr.Append(multierr.Append(ErrRetrievalFailed, ErrProposalCreationFailed), err)
	}
	proposal := &retrievaltypes.DealProposal{
		PayloadCID: candidate.RootCid,
		ID:         retrievaltypes.DealID(dealIdGen.Next()),
		Params:     params,
	}

	retrieveCtx, retrieveCancel := context.WithCancel(ctx)
	defer retrieveCancel()

	var lastBytesReceived uint64
	var doneLk sync.Mutex
	var done, timedOut bool
	var lastBytesReceivedTimer, gracefulShutdownTimer *clock.Timer

	gracefulShutdownChan := make(chan struct{})

	// Start the timeout tracker only if retrieval timeout isn't 0
	if timeout != 0 {
		lastBytesReceivedTimer = cfg.Clock.AfterFunc(timeout, func() {
			doneLk.Lock()
			done = true
			timedOut = true
			doneLk.Unlock()

			gracefulShutdownChan <- struct{}{}
			gracefulShutdownTimer = cfg.Clock.AfterFunc(1*time.Minute, retrieveCancel)
		})
	}

	eventsSubscriber := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.DataReceivedProgress {
			if lastBytesReceivedTimer != nil {
				doneLk.Lock()
				if !done {
					if lastBytesReceived != channelState.Received() {
						lastBytesReceivedTimer.Reset(timeout)
						lastBytesReceived = channelState.Received()
					}
				}
				doneLk.Unlock()
			}
		}
		eventsCallback(event, channelState)
	}

	stats, err := client.RetrieveFromPeer(
		retrieveCtx,
		linkSystem,
		candidate.MinerPeer.ID,
		proposal,
		selector,
		eventsSubscriber,
		gracefulShutdownChan,
	)

	if timedOut {
		return nil, multierr.Append(ErrRetrievalFailed,
			fmt.Errorf(
				"%w after %s",
				ErrRetrievalTimedOut,
				timeout,
			),
		)
	}

	if lastBytesReceivedTimer != nil {
		lastBytesReceivedTimer.Stop()
	}
	if gracefulShutdownTimer != nil {
		gracefulShutdownTimer.Stop()
	}

	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRetrievalFailed, err)
	}
	return stats, nil
}
