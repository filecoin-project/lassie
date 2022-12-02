package retrieval

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/application-research/filclient"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
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
	) (<-chan filclient.RetrievalResult, <-chan uint64, func())
}

type CounterCallback func(int) error
type PeerIDCallback func(peer.ID) error
type PeerIDErrorCallback func(peer.ID, error)

type GetStorageProviderTimeout func(peer peer.ID) time.Duration
type IsAcceptableStorageProvider func(peer peer.ID) bool
type IsAcceptableQueryResponse func(*retrievalmarket.QueryResponse) bool

type RetrievalResult struct {
	StorageProviderId peer.ID
	RootCid           cid.Cid
	Duration          time.Duration
	Size              uint64
	TotalPayment      big.Int
}

type CidRetrieval interface {
	RetrieveCid(ctx context.Context) (*RetrievalResult, error)
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

	queryResponseChan chan candidateQuery

	onCandidatesFound              []CounterCallback
	onCandidatesFiltered           []CounterCallback
	onRetrievingFromCandidate      []PeerIDCallback
	onErrorQueryingCandidate       []PeerIDErrorCallback
	onErrorRetrievingFromCandidate []PeerIDErrorCallback
}

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
		queryResponseChan:           make(chan candidateQuery),
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

func (ret *retrieval) RetrieveCid(ctx context.Context) (*RetrievalResult, error) {
	// Indexer candidates for CID
	candidates, err := ret.IndexEndpoint.FindCandidates(ctx, ret.Cid)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, ErrNoCandidates
	}
	for _, cb := range ret.onCandidatesFound {
		if err := cb(len(candidates)); err != nil {
			return nil, err
		}
	}

	// Query acceptable candidates, async
	queryCtx, queryCancel := context.WithCancel(ctx)
	defer queryCancel() // after a successful retrieval we want to bail on any still-running queries
	var queryingCandidates int
	var wg sync.WaitGroup
	for _, candidate := range candidates {
		if ret.IsAcceptableStorageProvider(candidate.MinerPeer.ID) {
			queryingCandidates++
			wg.Add(1)
			go func() {
				ret.queryCandidate(queryCtx, candidate)
				wg.Done()
			}()
		}
	}
	for _, cb := range ret.onCandidatesFiltered {
		if err := cb(len(candidates)); err != nil {
			return nil, err
		}
	}
	if queryingCandidates == 0 {
		return nil, ErrNoCandidates
	}
	go func() {
		wg.Wait()
		close(ret.queryResponseChan) // signal to retrieve() to end
	}()

	// Retrieve from successful queries in the order they are received
	stats, err := ret.retrieve(ctx)
	if stats == nil && err == nil {
		return nil, ErrAllQueriesFailed
	}
	return stats, err
}

func (ret *retrieval) queryCandidate(ctx context.Context, candidate RetrievalCandidate) {
	retrievalTimeout := ret.GetStorageProviderTimeout(candidate.MinerPeer.ID)

	if retrievalTimeout != 0 {
		var cancelFunc func()
		ctx, cancelFunc = context.WithDeadline(ctx, time.Now().Add(retrievalTimeout))
		defer cancelFunc()
	}

	query, err := ret.Client.RetrievalQueryToPeer(ctx, candidate.MinerPeer, candidate.RootCid)
	if err != nil {
		for _, cb := range ret.onErrorQueryingCandidate {
			cb(candidate.MinerPeer.ID, err)
		}
		return
	}

	if query.Status != retrievalmarket.QueryResponseAvailable || !ret.IsAcceptableQueryResponse(query) {
		return
	}

	select {
	case ret.queryResponseChan <- candidateQuery{}:
	case <-ctx.Done():
	}
}

func (ret *retrieval) retrieve(ctx context.Context) (*RetrievalResult, error) {
	var merr error
	for {
		select {
		case queryResponse, ok := <-ret.queryResponseChan: // received a query response
			if !ok {
				return nil, merr
			}
			for _, cb := range ret.onRetrievingFromCandidate {
				if err := cb(queryResponse.candidate.MinerPeer.ID); err != nil {
					return nil, err
				}
			}
			stats, err := ret.retrieveFromCandidate(ctx, queryResponse)
			if err != nil {
				for _, cb := range ret.onErrorRetrievingFromCandidate {
					cb(queryResponse.candidate.MinerPeer.ID, err)
				}
				merr = multierr.Append(merr, err)
			} else {
				// success, discard other queries and bail
				close(ret.queryResponseChan)
				result := RetrievalResult{
					StorageProviderId: stats.Peer,
					RootCid:           queryResponse.candidate.RootCid,
					Duration:          stats.Duration,
					Size:              stats.Size,
					TotalPayment:      stats.TotalPayment,
				}
				return &result, nil
			}
		case <-ctx.Done():
			return nil, context.Canceled
		}
	}
}

func (ret *retrieval) retrieveFromCandidate(ctx context.Context, query candidateQuery) (*filclient.RetrievalStats, error) {
	proposal, err := retrievehelper.RetrievalProposalForAsk(query.response, query.candidate.RootCid, nil)
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

	retrievalTimeout := ret.GetStorageProviderTimeout(query.candidate.MinerPeer.ID)

	resultChan, progressChan, gracefulShutdown := ret.Client.RetrieveContentFromPeerAsync(
		retrieveCtx,
		query.candidate.MinerPeer.ID,
		query.response.PaymentAddress,
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

	var stats *filclient.RetrievalStats
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
