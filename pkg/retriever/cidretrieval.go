package retriever

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/retriever/prioritywaitqueue"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.uber.org/multierr"
)

type RetrievalClient interface {
	RetrievalQueryToPeer(
		ctx context.Context,
		minerPeer peer.AddrInfo,
		pcid cid.Cid,
	) (*retrievalmarket.QueryResponse, error)

	RetrieveContentFromPeerAsync(
		ctx context.Context,
		peerID peer.ID,
		minerWallet address.Address,
		proposal *retrievalmarket.DealProposal,
	) (<-chan RetrievalResult, <-chan uint64, func())
}

type CounterCallback func(int) error
type CandidateCallback func(RetrievalCandidate) error
type CandidateErrorCallback func(RetrievalCandidate, error)

type GetStorageProviderTimeout func(peer peer.ID) time.Duration
type IsAcceptableStorageProvider func(peer peer.ID) bool
type IsAcceptableQueryResponse func(*retrievalmarket.QueryResponse) bool

type Instrumentation interface {
	OnRetrievalCandidatesFound(foundCount int) error
	OnRetrievalCandidatesFiltered(filteredCount int) error
	OnErrorQueryingRetrievalCandidate(candidate RetrievalCandidate, err error)
	OnErrorRetrievingFromCandidate(candidate RetrievalCandidate, err error)
	OnRetrievalQueryForCandidate(candidate RetrievalCandidate, queryResponse *retrievalmarket.QueryResponse)
	OnFilteredRetrievalQueryForCandidate(candidate RetrievalCandidate, queryResponse *retrievalmarket.QueryResponse)
	OnRetrievingFromCandidate(candidate RetrievalCandidate)
}

type CidRetrieval interface {
	RetrieveCid(ctx context.Context) (*RetrievalStats, error)
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

var _ CidRetrieval = (*retrieval)(nil)

type retrieval struct {
	Ctx                         context.Context
	IndexEndpoint               Endpoint
	Client                      RetrievalClient
	Instrumentation             Instrumentation
	Cid                         cid.Cid
	GetStorageProviderTimeout   GetStorageProviderTimeout
	IsAcceptableStorageProvider IsAcceptableStorageProvider
	IsAcceptableQueryResponse   IsAcceptableQueryResponse

	WaitQueue prioritywaitqueue.PriorityWaitQueue[*retrievalmarket.QueryResponse]
}

// NewCidRetrieval creates a new CidRetrieval
func NewCidRetrieval(
	indexEndpoint Endpoint,
	client RetrievalClient,
	instrumentation Instrumentation,
	getStorageProviderTimeout GetStorageProviderTimeout,
	isAcceptableStorageProvider IsAcceptableStorageProvider,
	isAcceptableQueryResponse IsAcceptableQueryResponse,
	cid cid.Cid,
) *retrieval {
	ret := &retrieval{
		IndexEndpoint:               indexEndpoint,
		Client:                      client,
		Instrumentation:             instrumentation,
		Cid:                         cid,
		GetStorageProviderTimeout:   getStorageProviderTimeout,
		IsAcceptableStorageProvider: isAcceptableStorageProvider,
		IsAcceptableQueryResponse:   isAcceptableQueryResponse,
		WaitQueue:                   prioritywaitqueue.New(queryCompare),
	}
	return ret
}

func (ret *retrieval) RetrieveCid(ctx context.Context) (*RetrievalStats, error) {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	// Indexer candidates for CID
	candidates, err := ret.findCandidates(ctx)
	if err != nil {
		return nil, err
	}

	resultChan := make(chan runResult, len(candidates))
	stopChan := make(chan struct{})

	ret.startRetrievals(ctx, candidates, resultChan, stopChan)
	return ret.collectResults(resultChan, stopChan)
}

func (ret *retrieval) collectResults(resultChan chan runResult, stopChan chan struct{}) (*RetrievalStats, error) {
	var finishedCount int
	var rmerr error
	var qmerr error
	var stats *RetrievalStats
	for result := range resultChan {
		if result.QueryError != nil {
			qmerr = multierr.Append(qmerr, result.QueryError)
		}
		if result.RetrievalError != nil {
			rmerr = multierr.Append(rmerr, result.RetrievalError)
		}
		if result.RetrievalResult != nil {
			stats = result.RetrievalResult
			break
		}
		// have we got all responses but no success?
		finishedCount++
		if finishedCount >= cap(resultChan) {
			break
		}
	}
	// signals to goroutines to bail
	close(stopChan)

	if stats == nil {
		if rmerr == nil {
			// we failed, but didn't get any retrieval errors, so must have only got query errors
			rmerr = ErrAllQueriesFailed
		} else {
			// we failed, and got only retrieval errors
			rmerr = multierr.Append(rmerr, ErrAllRetrievalsFailed)
		}
	}
	return stats, multierr.Append(qmerr, rmerr)
}

// startRetrievals will begin async retrievals from the list of candidates
func (ret *retrieval) startRetrievals(ctx context.Context, candidates []RetrievalCandidate, resultChan chan runResult, stopChan chan struct{}) {
	for _, candidate := range candidates {
		// go ret.runRetrieval(ctx, candidate, resultChan)
		go func(candidate RetrievalCandidate) {
			// try-receive stop channel to exit early
			select {
			case <-stopChan:
				return
			default:
			}

			// Return the result of the retrieval to the result channel
			select {
			case <-stopChan:
				return
			case resultChan <- ret.runRetrieval(ctx, candidate):
			}
		}(candidate)
	}
}

func (ret *retrieval) findCandidates(ctx context.Context) ([]RetrievalCandidate, error) {
	candidates, err := ret.IndexEndpoint.FindCandidates(ctx, ret.Cid)
	if err != nil {
		return nil, fmt.Errorf("could not get retrieval candidates for %s: %w", ret.Cid, err)
	}

	ret.Instrumentation.OnRetrievalCandidatesFound(len(candidates))

	if len(candidates) == 0 {
		return nil, ErrNoCandidates
	}

	acceptableCandidates := make([]RetrievalCandidate, 0)
	for _, candidate := range candidates {
		if ret.IsAcceptableStorageProvider(candidate.MinerPeer.ID) {
			acceptableCandidates = append(acceptableCandidates, candidate)
		}
	}

	if err := ret.Instrumentation.OnRetrievalCandidatesFiltered(len(acceptableCandidates)); err != nil {
		return nil, err
	}

	if len(acceptableCandidates) == 0 {
		return nil, ErrNoCandidates
	}

	return acceptableCandidates, nil
}

// runRetrieval is a singular CID:SP retrieval, expected to be run in a goroutine
// and coordinate with other candidate retrievals to block after query phase and
// only attempt one retrieval-proper at a time.
func (ret *retrieval) runRetrieval(ctx context.Context, candidate RetrievalCandidate) runResult {
	queryResponse, err := ret.queryCandidate(ctx, candidate)
	if err != nil {
		ret.Instrumentation.OnErrorQueryingRetrievalCandidate(candidate, err)
		return runResult{QueryError: err}
	}

	ret.Instrumentation.OnRetrievalQueryForCandidate(candidate, queryResponse)

	if queryResponse.Status != retrievalmarket.QueryResponseAvailable ||
		!ret.IsAcceptableQueryResponse(queryResponse) {
		// bail, with no result or error
		return runResult{}
	}

	ret.Instrumentation.OnFilteredRetrievalQueryForCandidate(candidate, queryResponse)

	// priority queue wait
	done := ret.WaitQueue.Wait(queryResponse)
	defer done()

	ret.Instrumentation.OnRetrievingFromCandidate(candidate)

	log.Infof(
		"Attempting retrieval from miner %s for %s",
		candidate.MinerPeer.ID,
		formatCidAndRoot(ret.Cid, candidate.RootCid, false),
	)

	stats, err := ret.retrieveFromCandidate(ctx, candidate, queryResponse)
	if err != nil {
		ret.Instrumentation.OnErrorRetrievingFromCandidate(candidate, err)
		return runResult{RetrievalError: err}
	}

	return runResult{RetrievalResult: stats}
}

func (ret *retrieval) queryCandidate(ctx context.Context, candidate RetrievalCandidate) (*retrievalmarket.QueryResponse, error) {
	retrievalTimeout := ret.GetStorageProviderTimeout(candidate.MinerPeer.ID)

	if retrievalTimeout != 0 {
		var cancelFunc func()
		ctx, cancelFunc = context.WithDeadline(ctx, time.Now().Add(retrievalTimeout))
		defer cancelFunc()
	}

	query, err := ret.Client.RetrievalQueryToPeer(ctx, candidate.MinerPeer, candidate.RootCid)
	if err != nil {
		log.Warnf(
			"Failed to query miner %s for %s: %v",
			candidate.MinerPeer.ID,
			formatCidAndRoot(ret.Cid, candidate.RootCid, false),
			err,
		)
		return nil, err
	}

	return query, nil
}

func (ret *retrieval) retrieveFromCandidate(ctx context.Context, candidate RetrievalCandidate, queryResponse *retrievalmarket.QueryResponse) (*RetrievalStats, error) {
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

	retrievalTimeout := ret.GetStorageProviderTimeout(candidate.MinerPeer.ID)

	resultChan, progressChan, gracefulShutdown := ret.Client.RetrieveContentFromPeerAsync(
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
