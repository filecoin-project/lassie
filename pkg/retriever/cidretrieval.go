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

var errQueryCancelled = errors.New("query cancelled")

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

type CidRetrieval interface {
	RetrieveCid(context.Context, cid.Cid) (*RetrievalStats, error)
}

type runResult struct {
	RetrievalResult *RetrievalStats
	QueryError      error
	RetrievalError  error
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

var _ CidRetrieval = (*cidRetrieval)(nil)

type cidRetrieval struct {
	Ctx                         context.Context
	IndexEndpoint               Endpoint
	Client                      RetrievalClient
	Instrumentation             Instrumentation
	GetStorageProviderTimeout   GetStorageProviderTimeout
	IsAcceptableStorageProvider IsAcceptableStorageProvider
	IsAcceptableQueryResponse   IsAcceptableQueryResponse

	WaitGroup sync.WaitGroup
}

// retrievalState is used on a per-retrieval basis so we can handle multiple
// retrievals on a single cidRetrieval instance
type retrievalState struct {
	Cid                 cid.Cid
	WaitQueue           prioritywaitqueue.PriorityWaitQueue[*retrievalmarket.QueryResponse]
	ResultChan          chan runResult
	FinishChan          chan struct{}
	ActiveQueryCancel   []*func()
	ActiveQueryCancelLk sync.Mutex
}

// NewCidRetrieval creates a new CidRetrieval. A CidRetrieval instance may be
// used for multiple retrievals via the RetrieveCid call.
func NewCidRetrieval(
	indexEndpoint Endpoint,
	client RetrievalClient,
	instrumentation Instrumentation,
	getStorageProviderTimeout GetStorageProviderTimeout,
	isAcceptableStorageProvider IsAcceptableStorageProvider,
	isAcceptableQueryResponse IsAcceptableQueryResponse,
) *cidRetrieval {
	return &cidRetrieval{
		IndexEndpoint:               indexEndpoint,
		Client:                      client,
		Instrumentation:             instrumentation,
		GetStorageProviderTimeout:   getStorageProviderTimeout,
		IsAcceptableStorageProvider: isAcceptableStorageProvider,
		IsAcceptableQueryResponse:   isAcceptableQueryResponse,
	}
}

// wait is used internally for testing that we do proper goroutine cleanup
func (retrieval *cidRetrieval) wait() {
	retrieval.WaitGroup.Wait()
}

// RetrieveCid begins the retrieval process for a given CID. This call may be
// safely called across multiple goroutines
func (retrieval *cidRetrieval) RetrieveCid(ctx context.Context, cid cid.Cid) (*RetrievalStats, error) {
	// state local to this CID's retrieval
	state := &retrievalState{
		Cid:               cid,
		ResultChan:        make(chan runResult),
		FinishChan:        make(chan struct{}),
		WaitQueue:         prioritywaitqueue.New(queryCompare),
		ActiveQueryCancel: make([]*func(), 0),
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	// fetch indexer candidates for CID
	candidates, err := retrieval.findCandidates(ctx, cid)
	if err != nil {
		return nil, err
	}

	// start retrievals
	retrieval.WaitGroup.Add(len(candidates))
	for _, candidate := range candidates {
		candidate := candidate
		go func() {
			retrieval.runRetrieval(ctx, state, candidate)
			retrieval.WaitGroup.Done()
		}()
	}

	return retrieval.collectResults(ctx, state, candidates)
}

// findCandidates calls the indexer for the given CID
func (retrieval *cidRetrieval) findCandidates(ctx context.Context, cid cid.Cid) ([]RetrievalCandidate, error) {
	candidates, err := retrieval.IndexEndpoint.FindCandidates(ctx, cid)
	if err != nil {
		return nil, fmt.Errorf("could not get retrieval candidates for %s: %w", cid, err)
	}

	retrieval.Instrumentation.OnRetrievalCandidatesFound(len(candidates))

	if len(candidates) == 0 {
		return nil, ErrNoCandidates
	}

	acceptableCandidates := make([]RetrievalCandidate, 0)
	for _, candidate := range candidates {
		if retrieval.IsAcceptableStorageProvider(candidate.MinerPeer.ID) {
			acceptableCandidates = append(acceptableCandidates, candidate)
		}
	}

	if err := retrieval.Instrumentation.OnRetrievalCandidatesFiltered(len(acceptableCandidates)); err != nil {
		return nil, err
	}

	if len(acceptableCandidates) == 0 {
		return nil, ErrNoCandidates
	}

	return acceptableCandidates, nil
}

// collectResults is responsible for receiving query errors, retrieval errors
// and retrieval results and aggregating into an appropriate return of either
// a complete RetrievalStats or an bundled multi-error
func (retrieval *cidRetrieval) collectResults(ctx context.Context, state *retrievalState, candidates []RetrievalCandidate) (*RetrievalStats, error) {
	var finishedCount int
	var queryErrors error
	var retrievalErrors error
	var stats *RetrievalStats

collectcomplete:
	for {
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		case result := <-state.ResultChan:
			if result.QueryError != nil {
				queryErrors = multierr.Append(queryErrors, result.QueryError)
			}
			if result.RetrievalError != nil {
				retrievalErrors = multierr.Append(retrievalErrors, result.RetrievalError)
			}
			if result.RetrievalResult != nil {
				stats = result.RetrievalResult
				break collectcomplete
			}
			// have we got all responses but no success?
			finishedCount++
			if finishedCount >= len(candidates) {
				break collectcomplete
			}
		}
	}

	// cancel any active queries
	state.ActiveQueryCancelLk.Lock()
	for _, ac := range state.ActiveQueryCancel {
		(*ac)()
	}
	state.ActiveQueryCancelLk.Unlock()

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

// runRetrieval is a singular CID:SP retrieval, expected to be run in a goroutine
// and coordinate with other candidate retrievals to block after query phase and
// only attempt one retrieval-proper at a time.
func (retrieval *cidRetrieval) runRetrieval(ctx context.Context, state *retrievalState, candidate RetrievalCandidate) {
	queryResponse, err := queryCandidate(ctx, retrieval.GetStorageProviderTimeout, retrieval.Client, state, candidate)
	if err != nil {
		if err != errQueryCancelled {
			retrieval.Instrumentation.OnErrorQueryingRetrievalCandidate(candidate, err)
			state.sendResult(runResult{QueryError: err})
		}
		return
	}

	retrieval.Instrumentation.OnRetrievalQueryForCandidate(candidate, queryResponse)

	if queryResponse.Status != retrievalmarket.QueryResponseAvailable ||
		!retrieval.IsAcceptableQueryResponse(queryResponse) {
		// bail, with no result or error
		state.sendResult(runResult{})
		return
	}

	retrieval.Instrumentation.OnFilteredRetrievalQueryForCandidate(candidate, queryResponse)

	// priorityqueue wait; gated here so that only one retrieval can happen at once
	done := state.WaitQueue.Wait(queryResponse)
	defer done()

	if !state.canSendResult() {
		// a retrieval already successfully finished, don't proceed
		return
	}

	retrieval.Instrumentation.OnRetrievingFromCandidate(candidate)

	log.Infof(
		"Attempting retrieval from miner %s for %s",
		candidate.MinerPeer.ID,
		formatCidAndRoot(state.Cid, candidate.RootCid, false),
	)

	stats, err := retrieveFromCandidate(ctx, retrieval.GetStorageProviderTimeout, retrieval.Client, candidate, queryResponse)

	if err != nil {
		retrieval.Instrumentation.OnErrorRetrievingFromCandidate(candidate, err)
		state.sendResult(runResult{RetrievalError: err})
	} else {
		state.sendResult(runResult{RetrievalResult: stats})
	}
}

// canSendResult will indicate whether a result is likely to be accepted (true)
// or whether the retrieval is already finished (likely by a success)
func (state *retrievalState) canSendResult() bool {
	select {
	case <-state.FinishChan:
		return false
	default:
	}
	return true
}

// sendResult will only send a result to the parent goroutine if a retrieval has
// finished (likely by a success), otherwise it will send the result
func (state *retrievalState) sendResult(result runResult) bool {
	select {
	case <-state.FinishChan:
		return false
	case state.ResultChan <- result:
		if result.RetrievalResult != nil {
			// signals to goroutines to bail, this has to be done here, rather than on
			// the receiving parent end, because immediately after this call we instruct
			// the prioritywaitqueue that we're done and another may start
			close(state.FinishChan)
		}
	}
	return true
}

// queryCandidate handles the query phase for the given candidate, it will
// either return a query result or an error, where the error may be a
// errQueryCancelled which results from a bail due to a successful query from
// another candidate happening in the meantime
func queryCandidate(
	ctx context.Context,
	getStorageProviderTimeout GetStorageProviderTimeout,
	client RetrievalClient,
	state *retrievalState,
	candidate RetrievalCandidate,
) (*retrievalmarket.QueryResponse, error) {
	// allow this query to be cancelled prematurely from the parent goroutine
	var cancelled bool
	var cancelFunc func()
	ctx, cancelFunc = context.WithCancel(ctx)
	activeCancel := func() {
		cancelled = true
		cancelFunc()
	}
	state.ActiveQueryCancelLk.Lock()
	state.ActiveQueryCancel = append(state.ActiveQueryCancel, &activeCancel)
	state.ActiveQueryCancelLk.Unlock()
	defer func() {
		state.ActiveQueryCancelLk.Lock()
		for ii, fn := range state.ActiveQueryCancel {
			if fn == &activeCancel {
				state.ActiveQueryCancel = append(state.ActiveQueryCancel[:ii], state.ActiveQueryCancel[ii+1:]...)
				break
			}
		}
		state.ActiveQueryCancelLk.Unlock()
	}()

	retrievalTimeout := getStorageProviderTimeout(candidate.MinerPeer.ID)

	if retrievalTimeout != 0 {
		var timeoutFunc func()
		ctx, timeoutFunc = context.WithDeadline(ctx, time.Now().Add(retrievalTimeout))
		defer timeoutFunc()
	}

	query, err := client.RetrievalQueryToPeer(ctx, candidate.MinerPeer, candidate.RootCid)
	if err != nil {
		if !cancelled {
			log.Warnf(
				"Failed to query miner %s for %s: %v",
				candidate.MinerPeer.ID,
				formatCidAndRoot(state.Cid, candidate.RootCid, false),
				err,
			)
			return nil, err
		}
		return nil, errQueryCancelled
	}

	return query, nil
}

// retrieveFromCandidate handles the full retrieval phase for an individual
// candidate. We expect only one of these to be running at a time for a given
// CID retrieval.
func retrieveFromCandidate(
	ctx context.Context,
	getStorageProviderTimeout GetStorageProviderTimeout,
	client RetrievalClient,
	candidate RetrievalCandidate,
	queryResponse *retrievalmarket.QueryResponse,
) (*RetrievalStats, error) {

	proposal, err := RetrievalProposalForAsk(queryResponse, candidate.RootCid, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrProposalCreationFailed, err)
	}

	startTime := time.Now()
	retrieveCtx, retrieveCancel := context.WithCancel(ctx)
	defer retrieveCancel()

	var lastBytesReceived uint64
	var doneLk sync.Mutex
	var done, timedOut bool
	var lastBytesReceivedTimer, gracefulShutdownTimer *time.Timer

	retrievalTimeout := getStorageProviderTimeout(candidate.MinerPeer.ID)

	resultChan, progressChan, gracefulShutdown := client.RetrieveContentFromPeerAsync(
		retrieveCtx,
		candidate.MinerPeer.ID,
		queryResponse.PaymentAddress,
		proposal,
	)

	// Start the timeout tracker only if retrieval timeout isn't 0
	if retrievalTimeout != 0 {
		lastBytesReceivedTimer = time.AfterFunc(retrievalTimeout, func() {
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
						lastBytesReceivedTimer.Reset(retrievalTimeout)
						lastBytesReceived = bytesReceived
					}
				}
				doneLk.Unlock()
			}
		}
	}

	if timedOut {
		return nil, fmt.Errorf(
			"%w: did not receive data for %s (started %s ago, stopped at %s)",
			ErrRetrievalTimedOut,
			retrievalTimeout,
			time.Since(startTime),
			humanize.IBytes(lastBytesReceived),
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
		return stats, fmt.Errorf("%w: %v", ErrRetrievalFailed, err)
	}

	return stats, nil
}

func totalCost(qres *retrievalmarket.QueryResponse) big.Int {
	return big.Add(big.Mul(qres.MinPricePerByte, big.NewIntUnsigned(qres.Size)), qres.UnsealPrice)
}

func formatCidAndRoot(cid cid.Cid, root cid.Cid, short bool) string {
	if cid.Equals(root) {
		return formatCid(cid, short)
	} else {
		return fmt.Sprintf("%s (root %s)", formatCid(cid, short), formatCid(root, short))
	}
}

func formatCid(cid cid.Cid, short bool) string {
	str := cid.String()
	if short {
		return "..." + str[len(str)-10:]
	} else {
		return str
	}
}
