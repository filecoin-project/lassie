package filecoin

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/application-research/autoretrieve/blocks"
	"github.com/application-research/autoretrieve/metrics"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.opencensus.io/stats"
)

var (
	ErrNoCandidates                = errors.New("no candidates")
	ErrRetrievalAlreadyRunning     = errors.New("retrieval already running")
	ErrHitRetrievalLimit           = errors.New("hit retrieval limit")
	ErrProposalCreationFailed      = errors.New("proposal creation failed")
	ErrRetrievalRegistrationFailed = errors.New("retrieval registration failed")
	ErrRetrievalFailed             = errors.New("retrieval failed")
	ErrAllRetrievalsFailed         = errors.New("all retrievals failed")
)

type MinerConfig struct {
	RetrievalTimeout        time.Duration
	MaxConcurrentRetrievals uint
}

// All config values should be safe to leave uninitialized
type RetrieverConfig struct {
	MinerBlacklist     map[peer.ID]bool
	MinerWhitelist     map[peer.ID]bool
	DefaultMinerConfig MinerConfig
	MinerConfigs       map[peer.ID]MinerConfig
	Metrics            metrics.Metrics
}

func (cfg *RetrieverConfig) MinerConfig(peer peer.ID) MinerConfig {
	minerCfg := cfg.DefaultMinerConfig

	if individual, ok := cfg.MinerConfigs[peer]; ok {
		if individual.MaxConcurrentRetrievals != 0 {
			minerCfg.MaxConcurrentRetrievals = individual.MaxConcurrentRetrievals
		}

		if individual.RetrievalTimeout != 0 {
			minerCfg.RetrievalTimeout = individual.RetrievalTimeout
		}
	}

	return minerCfg
}

type Retriever struct {
	// Assumed immutable during operation
	config RetrieverConfig

	endpoint  Endpoint
	filClient *filclient.FilClient

	runningRetrievals        map[cid.Cid]bool
	activeRetrievalsPerMiner map[peer.ID]uint
	runningRetrievalsLk      sync.Mutex
}

type candidateQuery struct {
	candidate RetrievalCandidate
	response  *retrievalmarket.QueryResponse
}

type RetrievalCandidate struct {
	MinerPeer peer.AddrInfo
	RootCid   cid.Cid
}

type Endpoint interface {
	FindCandidates(context.Context, cid.Cid) ([]RetrievalCandidate, error)
}

// Possible errors: ErrInitKeystoreFailed, ErrInitWalletFailed,
// ErrInitFilClientFailed
func NewRetriever(
	config RetrieverConfig,
	filClient *filclient.FilClient,
	endpoint Endpoint,
	host host.Host,
	api api.Gateway,
	datastore datastore.Batching,
	blockManager *blocks.Manager,
) (*Retriever, error) {

	if config.Metrics == nil {
		config.Metrics = metrics.NewNoop()
	}

	retriever := &Retriever{
		config:                   config,
		endpoint:                 endpoint,
		filClient:                filClient,
		runningRetrievals:        make(map[cid.Cid]bool),
		activeRetrievalsPerMiner: make(map[peer.ID]uint),
	}

	return retriever, nil
}

// Request will tell the retriever to start trying to retrieve a certain CID. If
// there are no candidates available, this function will immediately return with
// an error. If a candidate is found, retrieval will begin in the background and
// nil will be returned.
//
// Retriever itself does not provide any mechanism for determining when a block
// becomes available - that is up to the caller.
//
// Possible errors: ErrInvalidEndpointURL, ErrEndpointRequestFailed,
// ErrEndpointBodyInvalid, ErrNoCandidates
func (retriever *Retriever) Request(cid cid.Cid) error {

	requestInfo := metrics.RequestInfo{
		RequestCid: cid,
	}

	// TODO: before looking up candidates from the endpoint, we could cache
	// candidates and use that cached info. We only really have to look up an
	// up-to-date candidate list from the endpoint if we need to begin a new
	// retrieval.
	candidates, err := retriever.lookupCandidates(context.Background(), cid)
	retriever.config.Metrics.RecordGetCandidatesResult(requestInfo, metrics.GetCandidatesResult{
		Err: err,
	})
	if err != nil {
		return fmt.Errorf("could not get retrieval candidates for %s: %w", cid, err)
	}

	// If we got to this point, one or more candidates have been found and we
	// are good to go ahead with the retrieval
	go retriever.retrieveFromBestCandidate(context.Background(), cid, candidates)

	return nil
}

// Takes an unsorted list of candidates, orders them, and attempts retrievals in
// serial until one succeeds.
//
// Possible errors: ErrAllRetrievalsFailed
func (retriever *Retriever) retrieveFromBestCandidate(ctx context.Context, cid cid.Cid, candidates []RetrievalCandidate) error {
	queries := retriever.queryCandidates(ctx, cid, candidates)

	sort.Slice(queries, func(i, j int) bool {
		a := queries[i].response
		b := queries[j].response

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
	})

	// retrievalStats will be nil after the loop if none of the retrievals successfully
	// complete
	var retrievalStats *filclient.RetrievalStats
	for _, query := range queries {
		candidateInfo := metrics.CandidateInfo{
			RequestInfo: metrics.RequestInfo{RequestCid: cid},
			RootCid:     query.candidate.RootCid,
			PeerID:      query.candidate.MinerPeer.ID,
		}
		if err := retriever.tryRegisterRunningRetrieval(query.candidate.RootCid, query.candidate.MinerPeer.ID); err != nil {
			// TODO: send some info to metrics about this

			if errors.Is(err, ErrRetrievalAlreadyRunning) {
				break
			}

			continue
		}

		// TODO: Determine if we still need this log
		retriever.config.Metrics.RecordRetrieval(candidateInfo)

		stats.Record(ctx, metrics.RetrievalDealCount.M(1))
		stats.Record(ctx, metrics.RetrievalDealActiveCount.M(1))

		// Make the retrieval
		retrievalStats, err := retriever.retrieve(ctx, query)

		stats.Record(ctx, metrics.RetrievalDealActiveCount.M(-1))
		if err != nil {
			stats.Record(ctx, metrics.RetrievalDealFailCount.M(1))
		} else {
			stats.Record(ctx, metrics.RetrievalDealSuccessCount.M(1))
			stats.Record(ctx, metrics.RetrievalDealDuration.M(retrievalStats.Duration.Seconds()))
			stats.Record(ctx, metrics.RetrievalDealSize.M(int64(retrievalStats.Size)))
			stats.Record(ctx, metrics.RetrievalDealCost.M(retrievalStats.TotalPayment.Int64()))
		}

		// TODO: Determine if we still need this log
		retriever.config.Metrics.RecordRetrievalResult(candidateInfo, metrics.RetrievalResult{
			Duration:      retrievalStats.Duration,
			BytesReceived: retrievalStats.Size,
			TotalPayment:  types.FIL(retrievalStats.TotalPayment),
			Err:           err,
		})

		retriever.unregisterRunningRetrieval(query.candidate.RootCid, query.candidate.MinerPeer.ID)

		if err != nil {
			continue
		}

		break
	}

	if retrievalStats == nil {
		return ErrAllRetrievalsFailed
	}

	return nil
}

// Possible errors: ErrRetrievalRegistrationFailed, ErrProposalCreationFailed,
// ErrRetrievalFailed
func (retriever *Retriever) retrieve(ctx context.Context, query candidateQuery) (*filclient.RetrievalStats, error) {
	proposal, err := retrievehelper.RetrievalProposalForAsk(query.response, query.candidate.RootCid, nil)
	if err != nil {
		return &filclient.RetrievalStats{}, fmt.Errorf("%w: %v", ErrProposalCreationFailed, err)
	}

	startTime := time.Now()

	retrieveCtx, retrieveCancel := context.WithCancel(ctx)
	defer retrieveCancel()
	var lastBytesReceived uint64 = 0
	var doneLk sync.Mutex
	done := false
	timedOut := false
	var lastBytesReceivedTimer *time.Timer

	minerCfgs := retriever.config.MinerConfigs[query.candidate.MinerPeer.ID]

	// Start the timeout tracker only if retrieval timeout isn't 0
	if minerCfgs.RetrievalTimeout != 0 {
		lastBytesReceivedTimer = time.AfterFunc(minerCfgs.RetrievalTimeout, func() {
			doneLk.Lock()
			done = true
			doneLk.Unlock()

			retrieveCancel()
			timedOut = true
		})
	}
	stats, err := retriever.filClient.RetrieveContextFromPeerWithProgressCallback(retrieveCtx, query.candidate.MinerPeer.ID, query.response.PaymentAddress, proposal, func(bytesReceived uint64) {
		if lastBytesReceivedTimer != nil {
			doneLk.Lock()
			if !done {
				if lastBytesReceived != bytesReceived {
					if !lastBytesReceivedTimer.Stop() {
						<-lastBytesReceivedTimer.C
					}
					lastBytesReceivedTimer.Reset(minerCfgs.RetrievalTimeout)
					lastBytesReceived = bytesReceived
				}
			}
			doneLk.Unlock()
		}
	})
	if timedOut {
		err = fmt.Errorf(
			"timed out after not receiving data for %s (started %s ago, stopped at %s)",
			minerCfgs.RetrievalTimeout,
			time.Since(startTime),
			humanize.IBytes(lastBytesReceived),
		)
	}
	// TODO: temporary measure, remove when filclient properly returns data on
	// failure
	if stats == nil {
		stats = &filclient.RetrievalStats{
			Size:     lastBytesReceived,
			Duration: time.Since(startTime),
		}
	}

	if lastBytesReceivedTimer != nil {
		lastBytesReceivedTimer.Stop()
	}
	doneLk.Lock()
	done = true
	doneLk.Unlock()

	if err != nil {
		return stats, fmt.Errorf("%w: %v", ErrRetrievalFailed, err)
	}

	return stats, nil
}

// Possible errors: ErrRetrievalAlreadyRunning, ErrHitRetrievalLimit
func (retriever *Retriever) tryRegisterRunningRetrieval(cid cid.Cid, miner peer.ID) error {
	retriever.runningRetrievalsLk.Lock()
	defer retriever.runningRetrievalsLk.Unlock()

	minerConfig := retriever.config.MinerConfigs[miner]

	// If limit is enabled (non-zero) and we have already hit it, we can't
	// allow this retrieval to start
	noLimit := minerConfig.MaxConcurrentRetrievals == 0
	atLimit := retriever.activeRetrievalsPerMiner[miner] >= minerConfig.MaxConcurrentRetrievals
	if !noLimit && atLimit {
		return ErrHitRetrievalLimit
	}

	if retriever.runningRetrievals[cid] {
		return ErrRetrievalAlreadyRunning
	}

	retriever.runningRetrievals[cid] = true
	retriever.activeRetrievalsPerMiner[miner] = retriever.activeRetrievalsPerMiner[miner] + 1

	return nil
}

// Unregisters a running retrieval. No-op if no retrieval is running.
func (retriever *Retriever) unregisterRunningRetrieval(cid cid.Cid, miner peer.ID) {
	retriever.runningRetrievalsLk.Lock()
	defer retriever.runningRetrievalsLk.Unlock()

	delete(retriever.runningRetrievals, cid)
	retriever.activeRetrievalsPerMiner[miner] = retriever.activeRetrievalsPerMiner[miner] - 1
	if retriever.activeRetrievalsPerMiner[miner] == 0 {
		delete(retriever.activeRetrievalsPerMiner, miner)
	}
}

// Returns a list of miners known to have the requested block, with blacklisted
// miners filtered out.
//
// Possible errors - ErrInvalidEndpointURL, ErrEndpointRequestFailed, ErrEndpointBodyInvalid,
// ErrNoCandidates
func (retriever *Retriever) lookupCandidates(ctx context.Context, cid cid.Cid) ([]RetrievalCandidate, error) {
	unfiltered, err := retriever.endpoint.FindCandidates(ctx, cid)
	if err != nil {
		return nil, err
	}

	// Remove blacklisted miners, or non-whitelisted miners
	var res []RetrievalCandidate
	for _, candidate := range unfiltered {

		// Skip blacklist
		if retriever.config.MinerBlacklist[candidate.MinerPeer.ID] {
			continue
		}

		// Skip non-whitelist IF the whitelist isn't empty
		if len(retriever.config.MinerWhitelist) > 0 && !retriever.config.MinerWhitelist[candidate.MinerPeer.ID] {
			continue
		}

		res = append(res, candidate)
	}

	return res, nil
}

func (retriever *Retriever) queryCandidates(ctx context.Context, cid cid.Cid, candidates []RetrievalCandidate) []candidateQuery {
	var queries []candidateQuery
	var queriesLk sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(candidates))

	for i, candidate := range candidates {
		go func(i int, candidate RetrievalCandidate) {
			defer wg.Done()

			candidateInfo := metrics.CandidateInfo{
				RequestInfo: metrics.RequestInfo{RequestCid: cid},
				RootCid:     candidate.RootCid,
				PeerID:      candidate.MinerPeer.ID,
			}

			retriever.config.Metrics.RecordQuery(candidateInfo)
			query, err := retriever.filClient.RetrievalQueryToPeer(ctx, candidate.MinerPeer, candidate.RootCid)
			retriever.config.Metrics.RecordQueryResult(candidateInfo, metrics.QueryResult{
				Err: err,
			})
			if err != nil {
				return
			}

			queriesLk.Lock()
			queries = append(queries, candidateQuery{candidate: candidate, response: query})
			queriesLk.Unlock()
		}(i, candidate)
	}

	wg.Wait()

	return queries
}

func totalCost(qres *retrievalmarket.QueryResponse) big.Int {
	return big.Add(big.Mul(qres.MinPricePerByte, big.NewIntUnsigned(qres.Size)), qres.UnsealPrice)
}
