package retriever

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/rvagg/go-prioritywaitqueue"
	"go.uber.org/multierr"
)

type CounterCallback func(int) error
type CandidateCallback func(RetrievalCandidate) error
type CandidateErrorCallback func(RetrievalCandidate, error)

type GetStorageProviderTimeout func(peer peer.ID) time.Duration
type IsAcceptableStorageProvider func(peer peer.ID) bool
type IsAcceptableQueryResponse func(*retrievalmarket.QueryResponse) bool

type Instrumentation interface {
	// OnRetrievalCandidatesFound is called once after querying the indexer
	OnRetrievalCandidatesFound(foundCount int) error
	// OnRetrievalCandidatesFiltered is called once after filtering is applied to indexer candidates
	OnRetrievalCandidatesFiltered(filteredCount int) error
	// OnErrorQueryingRetrievalCandidate may be called up to once per retrieval candidate
	OnErrorQueryingRetrievalCandidate(candidate RetrievalCandidate, err error)
	// OnErrorRetrievingFromCandidate may be called up to once per retrieval candidate
	OnErrorRetrievingFromCandidate(candidate RetrievalCandidate, err error)
	// OnRetrievalQueryForCandidate may be called up to once per retrieval candidate
	OnRetrievalQueryForCandidate(candidate RetrievalCandidate, queryResponse *retrievalmarket.QueryResponse)
	// OnFilteredRetrievalQueryForCandidate may be called up to once per retrieval candidate
	OnFilteredRetrievalQueryForCandidate(candidate RetrievalCandidate, queryResponse *retrievalmarket.QueryResponse)
	// OnRetrievingFromCandidate may be called up to once per retrieval candidate
	OnRetrievingFromCandidate(candidate RetrievalCandidate)
}

// queryCompare compares two QueryResponses and returns true if the first is
// preferable to the second. This is used for the PriorityWaitQueue that will
// prioritise execution of retrievals if two queries are available to compare
// at the same time.
var queryCompare prioritywaitqueue.ComparePriority[*retrievalmarket.QueryResponse] = func(a, b *retrievalmarket.QueryResponse) bool {
	// Always prefer unsealed to sealed, no matter what
	if a.UnsealPrice.IsZero() && !b.UnsealPrice.IsZero() {
		return true
	}

	// Select lower price, or continue if equal
	aTotalCost := totalCost(a)
	bTotalCost := totalCost(b)
	if !aTotalCost.Equals(bTotalCost) {
		return aTotalCost.LessThan(bTotalCost)
	}

	// Select smaller size, or continue if equal
	if a.Size != b.Size {
		return a.Size < b.Size
	}

	return false
}

type RetrievalConfig struct {
	Instrumentation             Instrumentation
	GetStorageProviderTimeout   GetStorageProviderTimeout
	IsAcceptableStorageProvider IsAcceptableStorageProvider
	IsAcceptableQueryResponse   IsAcceptableQueryResponse

	waitGroup sync.WaitGroup // only used internally for testing cleanup
}

// wait is used internally for testing that we do proper goroutine cleanup
func (cfg *RetrievalConfig) wait() {
	cfg.waitGroup.Wait()
}

// retrieval handles state on a per-retrieval (across multiple candidates) basis
type retrieval struct {
	cid                 cid.Cid
	waitQueue           prioritywaitqueue.PriorityWaitQueue[*retrievalmarket.QueryResponse]
	resultChan          chan RetrievalResult
	finishChan          chan struct{}
	activeQueryCancel   []*context.CancelFunc
	activeQueryCancelLk sync.Mutex
}

// Retrieve performs a retrieval for a given CID by querying the indexer, then
// attempting to query all candidates and attempting to perform a full retrieval
// from the best and fastest storage provider as the queries are received.
func Retrieve(
	ctx context.Context,
	cfg *RetrievalConfig,
	indexEndpoint Endpoint,
	client RetrievalClient,
	cid cid.Cid,
) (*RetrievalStats, error) {

	if cfg == nil {
		cfg = &RetrievalConfig{}
	}

	// state local to this CID's retrieval
	retrieval := &retrieval{
		cid:               cid,
		resultChan:        make(chan RetrievalResult),
		finishChan:        make(chan struct{}),
		waitQueue:         prioritywaitqueue.New(queryCompare),
		activeQueryCancel: make([]*context.CancelFunc, 0),
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	// fetch indexer candidates for CID
	candidates, err := findCandidates(ctx, cfg, indexEndpoint, cid)
	if err != nil {
		return nil, err
	}

	// start retrievals
	cfg.waitGroup.Add(len(candidates))
	for _, candidate := range candidates {
		candidate := candidate
		go func() {
			runRetrievalCandidate(ctx, cfg, client, retrieval, candidate)
			cfg.waitGroup.Done()
		}()
	}

	return collectResults(ctx, retrieval, candidates)
}

// findCandidates calls the indexer for the given CID
func findCandidates(ctx context.Context, cfg *RetrievalConfig, indexEndpoint Endpoint, cid cid.Cid) ([]RetrievalCandidate, error) {
	candidates, err := indexEndpoint.FindCandidates(ctx, cid)
	if err != nil {
		return nil, fmt.Errorf("could not get retrieval candidates for %s: %w", cid, err)
	}

	if cfg.Instrumentation != nil {
		cfg.Instrumentation.OnRetrievalCandidatesFound(len(candidates))
	}

	if len(candidates) == 0 {
		return nil, ErrNoCandidates
	}

	acceptableCandidates := make([]RetrievalCandidate, 0)
	for _, candidate := range candidates {
		if cfg.IsAcceptableStorageProvider == nil || cfg.IsAcceptableStorageProvider(candidate.MinerPeer.ID) {
			acceptableCandidates = append(acceptableCandidates, candidate)
		}
	}

	if cfg.Instrumentation != nil {
		if err := cfg.Instrumentation.OnRetrievalCandidatesFiltered(len(acceptableCandidates)); err != nil {
			return nil, err
		}
	}

	if len(acceptableCandidates) == 0 {
		return nil, ErrNoCandidates
	}

	return acceptableCandidates, nil
}

// collectResults is responsible for receiving query errors, retrieval errors
// and retrieval results and aggregating into an appropriate return of either
// a complete RetrievalStats or an bundled multi-error
func collectResults(ctx context.Context, retrieval *retrieval, candidates []RetrievalCandidate) (*RetrievalStats, error) {
	var finishedCount int
	var queryErrors error
	var retrievalErrors error
	var stats *RetrievalStats

collectcomplete:
	for {
		select {
		case result := <-retrieval.resultChan:
			if result.Err != nil {
				if errors.Is(result.Err, ErrQueryFailed) {
					queryErrors = multierr.Append(queryErrors, result.Err)
				} else {
					retrievalErrors = multierr.Append(retrievalErrors, result.Err)
				}
			}
			if result.RetrievalStats != nil {
				stats = result.RetrievalStats
				break collectcomplete
			}
			// have we got all responses but no success?
			finishedCount++
			if finishedCount >= len(candidates) {
				break collectcomplete
			}
		case <-ctx.Done():
			return nil, context.Canceled
		}
	}

	// cancel any active queries
	retrieval.activeQueryCancelLk.Lock()
	for _, ac := range retrieval.activeQueryCancel {
		(*ac)()
	}
	retrieval.activeQueryCancelLk.Unlock()

	if stats == nil {
		if retrievalErrors == nil {
			// we failed, but didn't get any retrieval errors, so must have only got query errors
			retrievalErrors = ErrAllQueriesFailed
		} else {
			// we failed, and got only retrieval errors
			retrievalErrors = multierr.Append(retrievalErrors, ErrAllRetrievalsFailed)
		}
		return nil, multierr.Append(queryErrors, retrievalErrors)
	}
	// if we succeeded, drop all the errors that occurred along the way
	return stats, nil
}

// runRetrievalCandidate is a singular CID:SP retrieval, expected to be run in a goroutine
// and coordinate with other candidate retrievals to block after query phase and
// only attempt one retrieval-proper at a time.
func runRetrievalCandidate(ctx context.Context, cfg *RetrievalConfig, client RetrievalClient, retrieval *retrieval, candidate RetrievalCandidate) {
	var timeout time.Duration
	if cfg.GetStorageProviderTimeout != nil {
		timeout = cfg.GetStorageProviderTimeout(candidate.MinerPeer.ID)
	}

	// gating after query, done() is provided by the prioritywaitqueue to signal completion
	var done func()
	canProceed := func(queryResponse *retrievalmarket.QueryResponse) bool {
		// priorityqueue wait; gated here so that only one retrieval can happen at once
		done = retrieval.waitQueue.Wait(queryResponse)
		return retrieval.canSendResult()
	}

	// allow for localised cancellation across the multiple goroutines being coordinated
	ctx, cancelFn := context.WithCancel(ctx)
	retrieval.activeQueryCancelLk.Lock()
	retrieval.activeQueryCancel = append(retrieval.activeQueryCancel, &cancelFn)
	retrieval.activeQueryCancelLk.Unlock()

	stats, err := retrieveCandidate(ctx, cfg, client, canProceed, timeout, candidate)

	if retrieval.canSendResult() {
		if err != nil {
			if ctx.Err() != nil { // cancelled, don't report the error
				retrieval.sendResult(RetrievalResult{})
			} else {
				retrieval.sendResult(RetrievalResult{Err: err})
			}
		} else {
			retrieval.sendResult(RetrievalResult{RetrievalStats: stats})
		}
	} // else nothing to do

	if done != nil {
		done()
	}
}

// canSendResult will indicate whether a result is likely to be accepted (true)
// or whether the retrieval is already finished (likely by a success)
func (retrieval *retrieval) canSendResult() bool {
	select {
	case <-retrieval.finishChan:
		return false
	default:
	}
	return true
}

// sendResult will only send a result to the parent goroutine if a retrieval has
// finished (likely by a success), otherwise it will send the result
func (retrieval *retrieval) sendResult(result RetrievalResult) bool {
	select {
	case <-retrieval.finishChan:
		return false
	case retrieval.resultChan <- result:
		if result.RetrievalStats != nil {
			// signals to goroutines to bail, this has to be done here, rather than on
			// the receiving parent end, because immediately after this call we instruct
			// the prioritywaitqueue that we're done and another may start
			close(retrieval.finishChan)
		}
	}
	return true
}

func totalCost(qres *retrievalmarket.QueryResponse) big.Int {
	return big.Add(big.Mul(qres.MinPricePerByte, big.NewIntUnsigned(qres.Size)), qres.UnsealPrice)
}

// retrieveCandidate performs the full retrieval flow for a single SP:CID
// candidate, allowing for gating between query and retrieval using a
// canProceed callback.
func retrieveCandidate(
	ctx context.Context,
	cfg *RetrievalConfig,
	client RetrievalClient,
	canProceed IsAcceptableQueryResponse,
	timeout time.Duration,
	candidate RetrievalCandidate) (*RetrievalStats, error) {

	queryCtx := ctx // separate context so we can capture cancellation vs timeout

	if timeout != 0 {
		var timeoutFunc func()
		queryCtx, timeoutFunc = context.WithDeadline(ctx, time.Now().Add(timeout))
		defer timeoutFunc()
	}

	queryResponse, err := client.RetrievalQueryToPeer(queryCtx, candidate.MinerPeer, candidate.RootCid)
	if err != nil {
		if ctx.Err() == nil { // not cancelled, maybe timed out though
			log.Warnf(
				"Failed to query miner %s for %s: %v",
				candidate.MinerPeer.ID,
				candidate.RootCid,
				err,
			)
			if cfg.Instrumentation != nil {
				cfg.Instrumentation.OnErrorQueryingRetrievalCandidate(candidate, err)
			}
			return nil, fmt.Errorf("%w: %v", ErrQueryFailed, err)
		}
		// don't register an error on cancel, this is normal on a successful retrieval on an alternative SP
		return nil, nil
	}

	if cfg.Instrumentation != nil {
		cfg.Instrumentation.OnRetrievalQueryForCandidate(candidate, queryResponse)
	}

	if queryResponse.Status != retrievalmarket.QueryResponseAvailable ||
		(cfg.IsAcceptableQueryResponse != nil && !cfg.IsAcceptableQueryResponse(queryResponse)) {
		// bail, with no result or error
		return nil, nil
	}

	if cfg.Instrumentation != nil {
		cfg.Instrumentation.OnFilteredRetrievalQueryForCandidate(candidate, queryResponse)
	}

	if !canProceed(queryResponse) {
		return nil, nil
	}

	if cfg.Instrumentation != nil {
		cfg.Instrumentation.OnRetrievingFromCandidate(candidate)
	}

	log.Infof(
		"Attempting retrieval from miner %s for %s",
		candidate.MinerPeer.ID,
		candidate.RootCid,
	)

	proposal, err := RetrievalProposalForAsk(queryResponse, candidate.RootCid, nil)
	if err != nil {
		return nil, multierr.Append(multierr.Append(ErrRetrievalFailed, ErrProposalCreationFailed), err)
	}

	startTime := time.Now()
	retrieveCtx, retrieveCancel := context.WithCancel(ctx)
	defer retrieveCancel()

	var lastBytesReceived uint64
	var doneLk sync.Mutex
	var done, timedOut bool
	var lastBytesReceivedTimer, gracefulShutdownTimer *time.Timer

	resultChan, progressChan, gracefulShutdown := client.RetrieveContentFromPeerAsync(
		retrieveCtx,
		candidate.MinerPeer.ID,
		queryResponse.PaymentAddress,
		proposal,
	)

	// Start the timeout tracker only if retrieval timeout isn't 0
	if timeout != 0 {
		lastBytesReceivedTimer = time.AfterFunc(timeout, func() {
			doneLk.Lock()
			done = true
			doneLk.Unlock()

			gracefulShutdown()
			gracefulShutdownTimer = time.AfterFunc(1*time.Minute, retrieveCancel)
			timedOut = true
		})
	}

	var stats *RetrievalStats
waitforcomplete:
	for {
		select {
		case result := <-resultChan:
			stats = result.RetrievalStats
			err = result.Err
			break waitforcomplete
		case bytesReceived := <-progressChan:
			if lastBytesReceivedTimer != nil {
				doneLk.Lock()
				if !done {
					if lastBytesReceived != bytesReceived {
						lastBytesReceivedTimer.Reset(timeout)
						lastBytesReceived = bytesReceived
					}
				}
				doneLk.Unlock()
			}
		}
	}

	if timedOut {
		return nil, multierr.Append(ErrRetrievalFailed,
			fmt.Errorf(
				"%w: did not receive data for %s (started %s ago, stopped at %s)",
				ErrRetrievalTimedOut,
				timeout,
				time.Since(startTime),
				humanize.IBytes(lastBytesReceived),
			),
		)
	}

	if lastBytesReceivedTimer != nil {
		lastBytesReceivedTimer.Stop()
	}
	if gracefulShutdownTimer != nil {
		gracefulShutdownTimer.Stop()
	}
	doneLk.Lock()
	done = true
	doneLk.Unlock()

	if err != nil {
		if cfg.Instrumentation != nil {
			cfg.Instrumentation.OnErrorRetrievingFromCandidate(candidate, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrRetrievalFailed, err)
	}

	return stats, err
}
