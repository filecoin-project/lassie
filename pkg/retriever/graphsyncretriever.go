package retriever

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/rvagg/go-prioritywaitqueue"
	"go.uber.org/multierr"
)

type CounterCallback func(int) error
type CandidateCallback func(types.RetrievalCandidate) error
type CandidateErrorCallback func(types.RetrievalCandidate, error)

type GetStorageProviderTimeout func(peer peer.ID) time.Duration
type IsAcceptableQueryResponse func(peer peer.ID, req types.RetrievalRequest, queryResponse *retrievalmarket.QueryResponse) bool

type queryCandidate struct {
	*retrievalmarket.QueryResponse
	Duration time.Duration
}

type retrievalResult struct {
	PeerID     peer.ID
	PhaseStart time.Time
	Stats      *types.RetrievalStats
	Event      *types.RetrievalEvent
	Err        error
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
	candidateChan chan types.RetrievalCandidate
	finishChan    chan struct{}
	resultChan    chan retrievalResult
	statsChan     chan types.RetrieveResult
	waitQueue     prioritywaitqueue.PriorityWaitQueue[*queryCandidate]

	Client                    RetrievalClient
	eventsCallback            func(types.RetrievalEvent)
	GetStorageProviderTimeout GetStorageProviderTimeout
	IsAcceptableQueryResponse IsAcceptableQueryResponse
	request                   types.RetrievalRequest
	waitGroup                 sync.WaitGroup // only used internally for testing cleanup
}

func NewGraphSyncRetriever(
	ctx context.Context,
	request types.RetrievalRequest,
	events func(types.RetrievalEvent),
	client RetrievalClient,
	getStorageProviderTimeout GetStorageProviderTimeout,
	isAcceptableQueryResponse IsAcceptableQueryResponse,
) (*GraphSyncRetriever, <-chan types.RetrieveResult) {
	if events == nil {
		events = func(types.RetrievalEvent) {}
	}

	gsr := &GraphSyncRetriever{
		candidateChan: make(chan types.RetrievalCandidate),
		finishChan:    make(chan struct{}),
		resultChan:    make(chan retrievalResult),
		statsChan:     make(chan types.RetrieveResult),
		waitQueue:     prioritywaitqueue.New(queryCompare),

		Client:                    client,
		eventsCallback:            events,
		GetStorageProviderTimeout: getStorageProviderTimeout,
		IsAcceptableQueryResponse: isAcceptableQueryResponse,
		request:                   request,
	}

	go func() {
		gsr.collectResults(ctx, 100) // BUG: we'll have to rethink how we manage the "out of candidates" state
	}()

	return gsr, gsr.statsChan
}

// I know this doesn't follow the Retrieve interface
func (gsr *GraphSyncRetriever) Retrieve(ctx context.Context, candidate types.RetrievalCandidate) {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	// start retrievals
	queryStartTime := time.Now()
	gsr.waitGroup.Add(1)
	go func() {
		gsr.runRetrievalCandidate(ctx, queryStartTime, candidate)
		gsr.waitGroup.Done()
	}()
}

// RetrieveFromCandidates performs a retrieval for a given CID by querying the indexer, then
// attempting to query all candidates and attempting to perform a full retrieval
// from the best and fastest storage provider as the queries are received.
func (gsr *GraphSyncRetriever) RetrieveFromCandidates(
	ctx context.Context,
	retrievalRequest types.RetrievalRequest,
	candidates []types.RetrievalCandidate,
	eventsCallback func(types.RetrievalEvent),
) (*types.RetrievalStats, error) {

	if gsr == nil {
		gsr = &GraphSyncRetriever{}
	}
	if eventsCallback == nil {
		eventsCallback = func(re types.RetrievalEvent) {}
	}

	// state local to this CID's retrieval
	retrieval := &retrieval{
		resultChan: make(chan retrievalResult),
		finishChan: make(chan struct{}),
		waitQueue:  prioritywaitqueue.New(queryCompare),
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	// start retrievals
	queryStartTime := time.Now()
	gsr.waitGroup.Add(len(candidates))
	for _, candidate := range candidates {
		candidate := candidate
		go func() {
			runRetrievalCandidate(ctx, gsr, retrievalRequest, gsr.Client, retrieval, queryStartTime, candidate)
			gsr.waitGroup.Done()
		}()
	}

	return collectResults(ctx, retrieval, len(candidates), eventsCallback)
}

// collectResults is responsible for receiving query errors, retrieval errors
// and retrieval results and aggregating into an appropriate return of either
// a complete RetrievalStats or an bundled multi-error
func (gsr *GraphSyncRetriever) collectResults(ctx context.Context, expectedCandidates int) {
	var finishedCount int
	var queryErrors error
	var retrievalErrors error

	for {
		select {
		case result := <-gsr.resultChan:
			if result.Event != nil {
				gsr.eventsCallback(*result.Event)
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
				gsr.statsChan <- types.RetrieveResult{result.Stats, nil}
				return
			}
			// have we got all responses but no success?
			finishedCount++
			if finishedCount >= expectedCandidates {
				if retrievalErrors == nil {
					// we failed, but didn't get any retrieval errors, so must have only got query errors
					retrievalErrors = ErrAllQueriesFailed
				} else {
					// we failed, and got only retrieval errors
					retrievalErrors = multierr.Append(retrievalErrors, ErrAllRetrievalsFailed)
				}
				gsr.statsChan <- types.RetrieveResult{nil, multierr.Append(queryErrors, retrievalErrors)}
				return
			}
		case <-ctx.Done():
			gsr.statsChan <- types.RetrieveResult{nil, context.Canceled}
			return
		}
	}
}

// runRetrievalCandidate is a singular CID:SP retrieval, expected to be run in a goroutine
// and coordinate with other candidate retrievals to block after query phase and
// only attempt one retrieval-proper at a time.
func (gsr *GraphSyncRetriever) runRetrievalCandidate(
	ctx context.Context,
	queryStartTime time.Time,
	candidate types.RetrievalCandidate,
) {
	// phaseStartTime starts off as the queryStartTime, based on the start of all queries,
	// but is updated to the retrievalStartTime when the retrieval starts. By the time we
	// are sending the results, phaseStartTime may be the retrievalStartTime, or it may
	// remain the queryStartTime if we didn't get to retrieval for this candidate.
	phaseStartTime := queryStartTime

	var timeout time.Duration
	if gsr.GetStorageProviderTimeout != nil {
		timeout = gsr.GetStorageProviderTimeout(candidate.MinerPeer.ID)
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
	if queryResponse != nil && queryResponse.Status == retrievalmarket.QueryResponseError {
		retrieval.sendEvent(events.Failed(req.RetrievalID, phaseStartTime, types.QueryPhase, candidate, queryResponse.Message))
		queryResponse = nil
	}

	if queryResponse != nil {
		retrieval.sendEvent(events.QueryAsked(req.RetrievalID, phaseStartTime, candidate, *queryResponse))
		if queryResponse.Status != retrievalmarket.QueryResponseAvailable ||
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
					resType, err := retrievalmarket.DealResponseFromNode(lastVoucher.Voucher)
					if err != nil {
						return
					}
					if resType.Status == retrievalmarket.DealStatusAccepted {
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

			stats, retrievalErr = retrievalPhase(ctx, cfg, req.LinkSystem, client, timeout, candidate, queryResponse, eventsCallback)

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
func (gsr *GraphSyncRetriever) canSendResult() bool {
	select {
	case <-gsr.finishChan:
		return false
	default:
	}
	return true
}

// sendResult will only send a result to the parent goroutine if a retrieval has
// finished (likely by a success), otherwise it will send the result
func (gsr *GraphSyncRetriever) sendResult(result retrievalResult) bool {
	select {
	case <-gsr.finishChan:
		return false
	case gsr.resultChan <- result:
		if result.Stats != nil {
			// signals to goroutines to bail, this has to be done here, rather than on
			// the receiving parent end, because immediately after this call we instruct
			// the prioritywaitqueue that we're done and another may start
			close(gsr.finishChan)
		}
	}
	return true
}

func (gsr *GraphSyncRetriever) sendEvent(event types.RetrievalEvent) {
	gsr.sendResult(retrievalResult{PeerID: event.StorageProviderId(), Event: &event})
}

// wait is used internally for testing that we do proper goroutine cleanup
func (gsr *GraphSyncRetriever) wait() {
	gsr.waitGroup.Wait()
}

func totalCost(qres *retrievalmarket.QueryResponse) big.Int {
	return big.Add(big.Mul(qres.MinPricePerByte, big.NewIntUnsigned(qres.Size)), qres.UnsealPrice)
}

// retrieveCandidate performs the full retrieval flow for a single SP:CID
// candidate, allowing for gating between query and retrieval using a
// canProceed callback.

func queryPhase(
	ctx context.Context,
	gsr *GraphSyncRetriever,
	client RetrievalClient,
	timeout time.Duration,
	candidate types.RetrievalCandidate,
	onConnected func(),
) (*retrievalmarket.QueryResponse, error) {

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
	gsr *GraphSyncRetriever,
	linkSystem ipld.LinkSystem,
	client RetrievalClient,
	timeout time.Duration,
	candidate types.RetrievalCandidate,
	queryResponse *retrievalmarket.QueryResponse,
	eventsCallback datatransfer.Subscriber,
) (*types.RetrievalStats, error) {
	log.Infof(
		"Attempting retrieval from miner %s for %s",
		candidate.MinerPeer.ID,
		candidate.RootCid,
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
