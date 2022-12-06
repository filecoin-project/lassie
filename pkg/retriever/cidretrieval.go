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
type PeerIDCallback func(peer.ID) error
type PeerIDErrorCallback func(peer.ID, error)

type GetStorageProviderTimeout func(peer peer.ID) time.Duration
type IsAcceptableStorageProvider func(peer peer.ID) bool
type IsAcceptableQueryResponse func(*retrievalmarket.QueryResponse) bool

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
	Cid                         cid.Cid
	GetStorageProviderTimeout   GetStorageProviderTimeout
	IsAcceptableStorageProvider IsAcceptableStorageProvider
	IsAcceptableQueryResponse   IsAcceptableQueryResponse

	WaitQueue  prioritywaitqueue.PriorityWaitQueue[*retrievalmarket.QueryResponse]
	ResultChan chan runResult
	FinishChan chan struct{}

	onCandidatesFound              []CounterCallback
	onCandidatesFiltered           []CounterCallback
	onRetrievingFromCandidate      []PeerIDCallback
	onErrorQueryingCandidate       []PeerIDErrorCallback
	onErrorRetrievingFromCandidate []PeerIDErrorCallback
}

// NewCidRetrieval creates a new CidRetrieval
func NewCidRetrieval(
	indexEndpoint Endpoint,
	Client RetrievalClient,
	getStorageProviderTimeout GetStorageProviderTimeout,
	isAcceptableStorageProvider IsAcceptableStorageProvider,
	isAcceptableQueryResponse IsAcceptableQueryResponse,
	cid cid.Cid,
) *retrieval {
	ret := &retrieval{
		IndexEndpoint:               indexEndpoint,
		Cid:                         cid,
		GetStorageProviderTimeout:   getStorageProviderTimeout,
		IsAcceptableStorageProvider: isAcceptableStorageProvider,
		IsAcceptableQueryResponse:   isAcceptableQueryResponse,
		ResultChan:                  make(chan runResult),
		FinishChan:                  make(chan struct{}),
		WaitQueue:                   prioritywaitqueue.New(queryCompare),
	}
	return ret
}

func (ret *retrieval) OnCandidatesFound(cb CounterCallback) {
	ret.onCandidatesFound = append(ret.onCandidatesFound, cb)
}

func (ret *retrieval) OnCandidatesFiltered(cb CounterCallback) {
	ret.onCandidatesFiltered = append(ret.onCandidatesFiltered, cb)
}

func (ret *retrieval) OnRetrievingFromCandidate(cb PeerIDCallback) {
	ret.onRetrievingFromCandidate = append(ret.onRetrievingFromCandidate, cb)
}

func (ret *retrieval) OnErrorQueryingCandidate(cb PeerIDErrorCallback) {
	ret.onErrorQueryingCandidate = append(ret.onErrorQueryingCandidate, cb)
}

func (ret *retrieval) OnErrorRetrievingFromCandidate(cb PeerIDErrorCallback) {
	ret.onErrorRetrievingFromCandidate = append(ret.onErrorRetrievingFromCandidate, cb)
}

func (ret *retrieval) RetrieveCid(ctx context.Context) (*RetrievalStats, error) {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	// Indexer candidates for CID
	candidates, err := ret.findCandidates(ctx)
	if err != nil {
		return nil, err
	}

	ret.startRetrievals(ctx, candidates)
	return ret.collectResults(candidates)
}

func (ret *retrieval) collectResults(candidates []RetrievalCandidate) (*RetrievalStats, error) {
	var finishedCount int
	var merr error
	var stats *RetrievalStats
	for result := range ret.ResultChan {
		if result.QueryError != nil {
			merr = multierr.Append(merr, result.QueryError)
		}
		if result.RetrievalError != nil {
			merr = multierr.Append(merr, result.RetrievalError)
		}
		if result.RetrievalResult != nil {
			stats = result.RetrievalResult
			break
		}
		// have we got all responses but no success?
		finishedCount++
		if finishedCount >= len(candidates) {
			if merr == nil {
				merr = ErrAllQueriesFailed
			}
			break
		}
	}
	// signals to goroutines to bail
	close(ret.FinishChan)
	return stats, merr
}

// startRetrievals will begin async retrievals from the list of candidates
func (ret *retrieval) startRetrievals(ctx context.Context, candidates []RetrievalCandidate) {
	for _, candidate := range candidates {
		go ret.runRetrieval(ctx, candidate)
	}
}

func (ret *retrieval) findCandidates(ctx context.Context) ([]RetrievalCandidate, error) {
	candidates, err := ret.IndexEndpoint.FindCandidates(ctx, ret.Cid)
	if err != nil {
		return nil, fmt.Errorf("could not get retrieval candidates for %s: %w", ret.Cid, err)
	}

	for _, cb := range ret.onCandidatesFound {
		if err := cb(len(candidates)); err != nil {
			return nil, err
		}
	}

	if len(candidates) == 0 {
		return nil, ErrNoCandidates
	}

	acceptableCandidates := make([]RetrievalCandidate, 0)
	for _, candidate := range candidates {
		if ret.IsAcceptableStorageProvider(candidate.MinerPeer.ID) {
			acceptableCandidates = append(acceptableCandidates, candidate)
		}
	}

	for _, cb := range ret.onCandidatesFiltered {
		if err := cb(len(acceptableCandidates)); err != nil {
			return nil, err
		}
	}

	if len(acceptableCandidates) == 0 {
		return nil, ErrNoCandidates
	}

	return acceptableCandidates, nil
}

// canSendResult will indicate whether a result is likely to be accepted (true)
// or whether the retrieval is already finished (likely by a success)
func (ret *retrieval) canSendResult() bool {
	select {
	case <-ret.FinishChan:
		return false
	default:
	}
	return true
}

// sendResult will only send a result to the parent goroutine if a retrieval has
// finished (likely by a success), otherwise it will send the result
func (ret *retrieval) sendResult(result runResult) bool {
	select {
	case <-ret.FinishChan:
		return false
	case ret.ResultChan <- result:
	}
	return true
}

// runRetrieval is a singular CID:SP retrieval, expected to be run in a goroutine
// and coordinate with other candidate retrievals to block after query phase and
// only attempt one retrieval-proper at a time.
func (ret *retrieval) runRetrieval(ctx context.Context, candidate RetrievalCandidate) {
	queryResponse, err := ret.queryCandidate(ctx, candidate)
	if err != nil {
		for _, cb := range ret.onErrorQueryingCandidate {
			cb(candidate.MinerPeer.ID, err)
		}
		if !ret.sendResult(runResult{QueryError: err}) {
			return
		}
		return
	}

	if queryResponse.Status != retrievalmarket.QueryResponseAvailable || !ret.IsAcceptableQueryResponse(queryResponse) {
		// bail, with no result or error
		ret.sendResult(runResult{})
		return
	}

	// priority queue wait
	done := ret.WaitQueue.Wait(queryResponse)
	defer done()

	if !ret.canSendResult() {
		// retrieval already finished, don't proceed
		return
	}

	for _, cb := range ret.onRetrievingFromCandidate {
		if err := cb(candidate.MinerPeer.ID); err != nil {
			// register a result (that's not from the retrieval), and bail
			ret.sendResult(runResult{RetrievalError: err})
			return
		}
	}

	stats, err := ret.retrieveFromCandidate(ctx, candidate, queryResponse)

	if err != nil {
		for _, cb := range ret.onErrorRetrievingFromCandidate {
			cb(candidate.MinerPeer.ID, err)
		}
		ret.sendResult(runResult{RetrievalError: err})
	} else {
		ret.sendResult(runResult{RetrievalResult: stats})
	}
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

	if query.Status != retrievalmarket.QueryResponseAvailable {
		return nil, nil
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
