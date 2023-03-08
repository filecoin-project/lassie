package retriever

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/rvagg/go-prioritywaitqueue"
	"go.uber.org/multierr"
)

type CounterCallback func(int) error
type CandidateCallback func(types.RetrievalCandidate) error
type CandidateErrorCallback func(types.RetrievalCandidate, error)

type GetStorageProviderTimeout func(peer peer.ID) time.Duration
type IsAcceptableStorageProvider func(peer peer.ID) bool
type IsAcceptableQueryResponse func(peer peer.ID, req types.RetrievalRequest, queryResponse *retrievaltypes.QueryResponse) bool

type queryCandidate struct {
	*retrievaltypes.QueryResponse
	Duration time.Duration
}

// queryCompare compares two QueryResponses and returns true if the first is
// preferable to the second. This is used for the PriorityWaitQueue that will
// prioritise execution of retrievals if two queries are available to compare
// at the same time.
var queryCompare prioritywaitqueue.ComparePriority[*queryCandidate] = func(a, b *queryCandidate) bool {
	// Always prefer unsealed to sealed, no matter what
	if a.UnsealPrice.IsZero() && !b.UnsealPrice.IsZero() {
		return true
	}

	// Select lower price, or continue if equal
	aTotalCost := totalCost(a.QueryResponse)
	bTotalCost := totalCost(b.QueryResponse)
	if !aTotalCost.Equals(bTotalCost) {
		return aTotalCost.LessThan(bTotalCost)
	}

	// Select smaller size, or continue if equal
	if a.Size != b.Size {
		return a.Size < b.Size
	}

	// Select the fastest to respond
	return a.Duration < b.Duration
}

type GraphSyncRetriever struct {
	GetStorageProviderTimeout GetStorageProviderTimeout
	IsAcceptableQueryResponse IsAcceptableQueryResponse
	Client                    RetrievalClient
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
	ctx            context.Context
	request        types.RetrievalRequest
	eventsCallback func(types.RetrievalEvent)
}

type graphsyncCandidateRetrieval struct {
	waitQueue          prioritywaitqueue.PriorityWaitQueue[*queryCandidate]
	retrievalStartTime time.Time
	resultChan         chan retrievalResult
	finishChan         chan struct{}
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
	}
}

func (r *graphsyncRetrieval) RetrieveFromAsyncCandidates(asyncCandidates types.InboundAsyncCandidates) (*types.RetrievalStats, error) {
	ctx, cancelCtx := context.WithCancel(r.ctx)

	retrieval := &graphsyncCandidateRetrieval{
		resultChan: make(chan retrievalResult),
		finishChan: make(chan struct{}),
		waitQueue:  prioritywaitqueue.New(queryCompare),
	}
	// start retrievals
	queryStartTime := time.Now()
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
				candidate := candidate
				waitGroup.Add(1)
				go func() {
					defer waitGroup.Done()
					runRetrievalCandidate(ctx, r.GraphSyncRetriever, r.request, r.Client, retrieval, queryStartTime, candidate)
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
	var queryErrors error
	var retrievalErrors error

	for {
		select {
		case result, ok := <-retrieval.resultChan:
			// have we got all responses but no success?
			if !ok {
				if retrievalErrors == nil {
					// we failed, but didn't get any retrieval errors, so must have only got query errors
					retrievalErrors = ErrAllQueriesFailed
				} else {
					// we failed, and got only retrieval errors
					retrievalErrors = multierr.Append(retrievalErrors, ErrAllRetrievalsFailed)
				}
				return nil, multierr.Append(queryErrors, retrievalErrors)
			}

			if result.Event != nil {
				eventsCallback(*result.Event)
				break
			}
			if result.Err != nil {
				if errors.Is(result.Err, ErrQueryFailed) {
					queryErrors = multierr.Append(queryErrors, result.Err)
				} else if errors.Is(result.Err, ErrRetrievalFailed) {
					retrievalErrors = multierr.Append(retrievalErrors, result.Err)
				}
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
	retrieval *graphsyncCandidateRetrieval,
	queryStartTime time.Time,
	candidate types.RetrievalCandidate,
) {
	// phaseStartTime starts off as the queryStartTime, based on the start of all queries,
	// but is updated to the retrievalStartTime when the retrieval starts. By the time we
	// are sending the results, phaseStartTime may be the retrievalStartTime, or it may
	// remain the queryStartTime if we didn't get to retrieval for this candidate.
	phaseStartTime := queryStartTime

	var timeout time.Duration
	if cfg.GetStorageProviderTimeout != nil {
		timeout = cfg.GetStorageProviderTimeout(candidate.MinerPeer.ID)
	}

	var stats *types.RetrievalStats
	var retrievalErr error
	var done func()

	retrieval.sendEvent(events.Started(req.RetrievalID, phaseStartTime, types.QueryPhase, candidate))

	// run the query phase
	onConnected := func() {
		retrieval.sendEvent(events.Connected(req.RetrievalID, phaseStartTime, types.QueryPhase, candidate))
	}
	queryResponse, queryErr := queryPhase(ctx, cfg, client, timeout, candidate, onConnected)
	if queryErr != nil {
		retrieval.sendEvent(events.Failed(req.RetrievalID, phaseStartTime, types.QueryPhase, candidate, queryErr.Error()))
	}

	// treat QueryResponseError as a failure
	if queryResponse != nil && queryResponse.Status == retrievaltypes.QueryResponseError {
		retrieval.sendEvent(events.Failed(req.RetrievalID, phaseStartTime, types.QueryPhase, candidate, queryResponse.Message))
		queryResponse = nil
	}

	if queryResponse != nil {
		retrieval.sendEvent(events.QueryAsked(req.RetrievalID, phaseStartTime, candidate, *queryResponse))
		if queryResponse.Status != retrievaltypes.QueryResponseAvailable ||
			(cfg.IsAcceptableQueryResponse != nil && !cfg.IsAcceptableQueryResponse(candidate.MinerPeer.ID, req, queryResponse)) {
			queryResponse = nil
		}
	}

	if queryResponse != nil {
		retrieval.sendEvent(events.QueryAskedFiltered(req.RetrievalID, phaseStartTime, candidate, *queryResponse))

		// if query is successful, then wait for priority and execute retrieval
		done = retrieval.waitQueue.Wait(&queryCandidate{queryResponse, time.Since(phaseStartTime)})

		if retrieval.canSendResult() { // move on to retrieval phase
			// only one goroutine is allowed to execute past here at a time, so retrieval.retrievalStartTime
			// doesn't need to be protected by a mutex, but it should mark the time the first retrieval
			// starts
			if retrieval.retrievalStartTime.IsZero() {
				retrieval.retrievalStartTime = time.Now()
			}
			phaseStartTime = retrieval.retrievalStartTime

			var receivedFirstByte bool
			eventsCallback := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				switch event.Code {
				case datatransfer.Open:
					retrieval.sendEvent(events.Proposed(req.RetrievalID, phaseStartTime, candidate))
				case datatransfer.NewVoucherResult:
					lastVoucher := channelState.LastVoucherResult()
					resType, err := retrievaltypes.DealResponseFromNode(lastVoucher.Voucher)
					if err != nil {
						return
					}
					if resType.Status == retrievaltypes.DealStatusAccepted {
						retrieval.sendEvent(events.Accepted(req.RetrievalID, phaseStartTime, candidate))
					}
				case datatransfer.DataReceivedProgress:
					if !receivedFirstByte {
						receivedFirstByte = true
						retrieval.sendEvent(events.FirstByte(req.RetrievalID, phaseStartTime, candidate))
					}
				}
			}

			retrieval.sendEvent(events.Started(req.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate))

			stats, retrievalErr = retrievalPhase(
				ctx,
				cfg,
				req.LinkSystem,
				client,
				timeout,
				candidate,
				queryResponse,
				req.GetSelector(),
				eventsCallback,
			)

			if retrievalErr != nil {
				msg := retrievalErr.Error()
				if errors.Is(retrievalErr, ErrRetrievalTimedOut) {
					msg = fmt.Sprintf("timeout after %s", timeout)
				}
				retrieval.sendEvent(events.Failed(req.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, msg))
			} else {
				retrieval.sendEvent(events.Success(
					req.RetrievalID,
					phaseStartTime,
					candidate,
					stats.Size,
					stats.Blocks,
					stats.Duration,
					stats.TotalPayment),
				)
			}
		} // else we didn't get to retrieval phase because we were cancelled
	} // else we didn't get to retrieval phase because query failed

	if retrieval.canSendResult() {
		// as long as collectResults is still running, we need to increment finishedCount by
		// sending a retrievalResult, so each path here sends one in some form
		if queryErr != nil || retrievalErr != nil {
			if ctx.Err() != nil { // cancelled, don't report the error
				retrieval.sendResult(retrievalResult{PhaseStart: phaseStartTime, PeerID: candidate.MinerPeer.ID})
			} else {
				// an error of some kind to report
				err := queryErr
				if err == nil {
					err = retrievalErr
				}
				retrieval.sendResult(retrievalResult{PhaseStart: phaseStartTime, PeerID: candidate.MinerPeer.ID, Err: err})
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

func totalCost(qres *retrievaltypes.QueryResponse) big.Int {
	return big.Add(big.Mul(qres.MinPricePerByte, big.NewIntUnsigned(qres.Size)), qres.UnsealPrice)
}

// retrieveCandidate performs the full retrieval flow for a single SP:CID
// candidate, allowing for gating between query and retrieval using a
// canProceed callback.

func queryPhase(
	ctx context.Context,
	cfg *GraphSyncRetriever,
	client RetrievalClient,
	timeout time.Duration,
	candidate types.RetrievalCandidate,
	onConnected func(),
) (*retrievaltypes.QueryResponse, error) {

	queryCtx := ctx // separate context so we can capture cancellation vs timeout

	if timeout != 0 {
		var timeoutFunc func()
		queryCtx, timeoutFunc = context.WithDeadline(ctx, time.Now().Add(timeout))
		defer timeoutFunc()
	}

	queryResponse, err := client.RetrievalQueryToPeer(queryCtx, candidate.MinerPeer, candidate.RootCid, onConnected)
	if err != nil {
		if ctx.Err() == nil { // not cancelled, maybe timed out though
			log.Warnf(
				"Failed to query miner %s for %s: %v",
				candidate.MinerPeer.ID,
				candidate.RootCid,
				err,
			)
			return nil, fmt.Errorf("%w: %v", ErrQueryFailed, err)
		}
		// don't register an error on cancel, this is normal on a successful retrieval on an alternative SP
		return nil, nil
	}

	return queryResponse, nil
}

func retrievalPhase(
	ctx context.Context,
	cfg *GraphSyncRetriever,
	linkSystem ipld.LinkSystem,
	client RetrievalClient,
	timeout time.Duration,
	candidate types.RetrievalCandidate,
	queryResponse *retrievaltypes.QueryResponse,
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

	proposal, err := RetrievalProposalForAsk(queryResponse, candidate.RootCid, nil)
	if err != nil {
		return nil, multierr.Append(multierr.Append(ErrRetrievalFailed, ErrProposalCreationFailed), err)
	}

	retrieveCtx, retrieveCancel := context.WithCancel(ctx)
	defer retrieveCancel()

	var lastBytesReceived uint64
	var doneLk sync.Mutex
	var done, timedOut bool
	var lastBytesReceivedTimer, gracefulShutdownTimer *time.Timer

	gracefulShutdownChan := make(chan struct{})

	// Start the timeout tracker only if retrieval timeout isn't 0
	if timeout != 0 {
		lastBytesReceivedTimer = time.AfterFunc(timeout, func() {
			doneLk.Lock()
			done = true
			timedOut = true
			doneLk.Unlock()

			gracefulShutdownChan <- struct{}{}
			gracefulShutdownTimer = time.AfterFunc(1*time.Minute, retrieveCancel)
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
		queryResponse.PaymentAddress,
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
