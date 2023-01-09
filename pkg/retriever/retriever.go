package retriever

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/eventpublisher"
	"github.com/filecoin-project/lassie/pkg/metrics"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opencensus.io/stats"
)

var (
	ErrNoCandidates                = errors.New("no candidates")
	ErrUnexpectedRetrieval         = errors.New("unexpected active retrieval")
	ErrHitRetrievalLimit           = errors.New("hit retrieval limit")
	ErrProposalCreationFailed      = errors.New("proposal creation failed")
	ErrRetrievalRegistrationFailed = errors.New("retrieval registration failed")
	ErrRetrievalFailed             = errors.New("retrieval failed")
	ErrAllRetrievalsFailed         = errors.New("all retrievals failed")
	ErrAllQueriesFailed            = errors.New("all queries failed")
	ErrRetrievalTimedOut           = errors.New("retrieval timed out")
)

type ErrRetrievalAlreadyRunning struct {
	c     cid.Cid
	extra string
}

func (e ErrRetrievalAlreadyRunning) Error() string {
	return fmt.Sprintf("retrieval already running for CID: %s (%s)", e.c, e.extra)
}

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
	PaidRetrievals     bool
}

func (cfg *RetrieverConfig) GetMinerConfig(peer peer.ID) MinerConfig {
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
	config           RetrieverConfig
	endpoint         Endpoint
	client           RetrievalClient
	eventManager     *EventManager
	activeRetrievals *ActiveRetrievalsManager
	minerMonitor     *minerMonitor
	confirm          func(cid.Cid) (bool, error)
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

type BlockConfirmer func(c cid.Cid) (bool, error)

func NewRetriever(
	ctx context.Context,
	config RetrieverConfig,
	client RetrievalClient,
	endpoint Endpoint,
	confirmer BlockConfirmer,
) (*Retriever, error) {
	retriever := &Retriever{
		config:           config,
		endpoint:         endpoint,
		client:           client,
		eventManager:     NewEventManager(ctx),
		activeRetrievals: NewActiveRetrievalsManager(),
		minerMonitor: newMinerMonitor(minerMonitorConfig{
			maxFailuresBeforeSuspend: 5,
			suspensionDuration:       time.Minute,
			failureHistoryDuration:   time.Second * 15,
		}),
		confirm: confirmer,
	}

	retriever.client.SubscribeToRetrievalEvents(retriever)

	return retriever, nil
}

// RegisterListener registers a listener to receive all events fired during the
// process of making a retrieval, including the process of querying available
// storage providers to find compatible ones to attempt retrieval from.
func (retriever *Retriever) RegisterListener(listener RetrievalEventListener) func() {
	return retriever.eventManager.RegisterListener(listener)
}

func (retriever *Retriever) Request(cid cid.Cid) error {
	ctx := context.Background()

	getStorageProviderTimeout := func(storageProviderId peer.ID) time.Duration {
		return retriever.config.GetMinerConfig(storageProviderId).RetrievalTimeout
	}

	isAcceptableStorageProvider := func(storageProviderId peer.ID) bool {
		// Skip blacklist
		if retriever.config.MinerBlacklist[storageProviderId] {
			return false
		}

		// Skip non-whitelist IF the whitelist isn't empty
		if len(retriever.config.MinerWhitelist) > 0 && !retriever.config.MinerWhitelist[storageProviderId] {
			return false
		}

		// Skip suspended SPs from the minerMonitor
		if retriever.minerMonitor.suspended(storageProviderId) {
			return false
		}

		// Skip if we are currently at our maximum concurrent retrievals for this SP
		// since we likely won't be able to retrieve from them at the moment even if
		// query is successful
		minerConfig := retriever.config.GetMinerConfig(storageProviderId)
		if minerConfig.MaxConcurrentRetrievals > 0 &&
			retriever.activeRetrievals.GetActiveRetrievalCountFor(storageProviderId) >= minerConfig.MaxConcurrentRetrievals {
			return false
		}

		return true
	}

	isAcceptableQueryResponse := func(queryResponse *retrievalmarket.QueryResponse) bool {
		// filter out paid retrievals if necessary
		return retriever.config.PaidRetrievals || totalCost(queryResponse).Equals(big.Zero())
	}

	// setup
	retrieval := NewCidRetrieval(
		retriever.endpoint,
		retriever.filClient,
		getStorageProviderTimeout,
		isAcceptableStorageProvider,
		isAcceptableQueryResponse,
		cid,
	)

	var retrievalId uuid.UUID // TODO: use this
	var candidateCount int
	var failedCount int64

	retrieval.OnCandidatesFound(func(foundCount int) error {
		if foundCount > 0 {
			stats.Record(ctx, metrics.RequestWithIndexerCandidatesCount.M(1))
		}
		stats.Record(ctx, metrics.IndexerCandidatesPerRequestCount.M(int64(foundCount)))
		return nil
	})
	retrieval.OnCandidatesFiltered(func(filteredCount int) error {
		candidateCount = filteredCount
		var err error
		retrievalId, err = retriever.activeRetrievals.New(cid, filteredCount, 1)
		if err != nil {
			return err
		}
		stats.Record(ctx, metrics.RequestWithIndexerCandidatesFilteredCount.M(1))
		return nil
	})
	retrieval.OnErrorQueryingCandidate(func(storageProviderId peer.ID, err error) {
		retriever.minerMonitor.recordFailure(storageProviderId)
	})
	retrieval.OnErrorRetrievingFromCandidate(func(storageProviderId peer.ID, err error) {
		// TODO: if timeout
		if errors.Is(err, ErrRetrievalTimedOut) {
			retriever.OnRetrievalEvent(NewRetrievalEventFailure(
				RetrievalPhase,
				query.candidate.RootCid, // TODO: get this
				storageProviderId,
				address.Undef,
				fmt.Sprintf("timeout after %s", getStorageProviderTimeout(storageProviderId)),
			))
		}
		atomic.AddInt64(&failedCount, 1)
		retriever.minerMonitor.recordFailure(storageProviderId)
	})
	retrieval.OnRetrievingFromCandidate(func(i peer.ID) error {
		// TODO: something with retrievalId
		return nil
	})

	// retrieve
	retrievalStats, err := retrieval.RetrieveCid(ctx)
	if err != nil {
		stats.Record(ctx, metrics.FailedRetrievalsPerRequestCount.M(int64(candidateCount)))
		return err
	}

	log.Infof(
		"Successfully retrieved from miner %s for %s\n"+
			"\tDuration: %s\n"+
			"\tBytes Received: %s\n"+
			"\tTotal Payment: %s",
		retrievalStats.StorageProviderId,
		formatCidAndRoot(cid, retrievalStats.RootCid, false),
		retrievalStats.Duration,
		humanize.IBytes(retrievalStats.Size),
		types.FIL(retrievalStats.TotalPayment),
	)

	stats.Record(ctx, metrics.RetrievalDealSuccessCount.M(1))
	stats.Record(ctx, metrics.RetrievalDealDuration.M(retrievalStats.Duration.Seconds()))
	stats.Record(ctx, metrics.RetrievalDealSize.M(int64(retrievalStats.Size)))
	stats.Record(ctx, metrics.RetrievalDealCost.M(retrievalStats.TotalPayment.Int64()))
	stats.Record(ctx, metrics.FailedRetrievalsPerRequestCount.M(atomic.LoadInt64(&failedCount)))

	return nil
}

// TODO: remove most of the stuff below here -----------------------------------

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
func (retriever *Retriever) RequestOld(cid cid.Cid) error {
	// TODO: before looking up candidates from the endpoint, we could cache
	// candidates and use that cached info. We only really have to look up an
	// up-to-date candidate list from the endpoint if we need to begin a new
	// retrieval.
	ctx := context.Background()

	candidates, err := retriever.lookupCandidates(ctx, cid)
	if err != nil {
		return fmt.Errorf("could not get retrieval candidates for %s: %w", cid, err)
	}

	if len(candidates) == 0 {
		return ErrNoCandidates
	}

	// when we want to include the indexer "phase", this should move to the top of
	// Retrieve(), but for now we can avoid unnecessary new+cleanup for negative
	// indexer calls.
	// We register that we have len(candidates) candidates to query, so that when
	// we receive success or failures from that many we know the phase is
	// completed. We also pre-emptively suggest we're expecting at least one
	// retrieval to occur. But this number will be updated with a proper count
	// before we start any retrievals, but at this point we want to avoid a
	// (unlikely) race condition where we get all expected success/failures for
	// queries and trigger a clean-up before we have a chance to set the correct
	// count
	retrievalId, err := retriever.activeRetrievals.New(cid, len(candidates), 1)
	if err != nil {
		return err
	}

	stats.Record(ctx, metrics.RequestWithIndexerCandidatesFilteredCount.M(1))

	// If we got to this point, one or more candidates have been found and we
	// are good to go ahead with the retrieval
	go retriever.retrieveFromBestCandidate(ctx, retrievalId, cid, candidates)
	return nil
}

// Takes an unsorted list of candidates, orders them, and attempts retrievals in
// serial until one succeeds.
//
// Possible errors: ErrAllRetrievalsFailed
func (retriever *Retriever) retrieveFromBestCandidate(ctx context.Context, retrievalId uuid.UUID, retrievalCid cid.Cid, candidates []RetrievalCandidate) error {
	queries := retriever.queryCandidates(ctx, retrievalId, retrievalCid, candidates)

	if len(queries) > 0 {
		stats.Record(ctx, metrics.RequestWithSuccessfulQueriesCount.M(1))
	}
	stats.Record(ctx, metrics.SuccessfulQueriesPerRequestCount.M(int64(len(queries))))

	if !retriever.config.PaidRetrievals {
		// filter out paid retrievals
		qt := make([]candidateQuery, 0)
		zero := big.Zero()
		for _, q := range queries {
			if totalCost(q.response).Equals(zero) {
				qt = append(qt, q)
			}
		}
		queries = qt
	}

	// register that we have this many candidates to retrieve from, so that when we
	// receive success or failures from that many we know the phase is completed,
	// if zero at this point then clean-up will occur
	retriever.activeRetrievals.SetRetrievalCandidateCount(retrievalCid, len(queries))

	if len(queries) == 0 {
		return nil
	}

	stats.Record(ctx, metrics.RequestWithSuccessfulQueriesFilteredCount.M(1))
	stats.Record(ctx, metrics.SuccessfulQueriesPerRequestFilteredCount.M(int64(len(queries))))

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
	var retrievalStats *RetrievalStats
	for i, query := range queries {
		minerConfig := retriever.config.GetMinerConfig(query.candidate.MinerPeer.ID)
		if err := retriever.activeRetrievals.SetRetrievalCandidate(
			retrievalCid,
			query.candidate.RootCid,
			query.candidate.MinerPeer.ID,
			minerConfig.MaxConcurrentRetrievals,
		); err != nil {
			continue // likely an ErrHitRetrievalLimit, move on to next candidate
		}

		log.Infof(
			"Attempting retrieval from miner %s for %s",
			query.candidate.MinerPeer.ID,
			formatCidAndRoot(retrievalCid, query.candidate.RootCid, false),
		)

		stats.Record(ctx, metrics.RetrievalRequestCount.M(1))
		stats.Record(ctx, metrics.RetrievalDealActiveCount.M(1))

		// Make the retrieval
		retrievalStats, err := retriever.retrieve(ctx, query)

		stats.Record(ctx, metrics.RetrievalDealActiveCount.M(-1))
		if err != nil {
			log.Warnf(
				"Failed to retrieve from miner %s for %s: %v",
				query.candidate.MinerPeer.ID,
				formatCidAndRoot(retrievalCid, query.candidate.RootCid, false),
				err,
			)
			stats.Record(ctx, metrics.RetrievalDealFailCount.M(1))

			continue
		} else {
			log.Infof(
				"Successfully retrieved from miner %s for %s\n"+
					"\tDuration: %s\n"+
					"\tBytes Received: %s\n"+
					"\tTotal Payment: %s",
				query.candidate.MinerPeer.ID,
				formatCidAndRoot(retrievalCid, query.candidate.RootCid, false),
				retrievalStats.Duration,
				humanize.IBytes(retrievalStats.Size),
				types.FIL(retrievalStats.TotalPayment),
			)

			stats.Record(ctx, metrics.RetrievalDealSuccessCount.M(1))
			stats.Record(ctx, metrics.RetrievalDealDuration.M(retrievalStats.Duration.Seconds()))
			stats.Record(ctx, metrics.RetrievalDealSize.M(int64(retrievalStats.Size)))
			stats.Record(ctx, metrics.RetrievalDealCost.M(retrievalStats.TotalPayment.Int64()))
			stats.Record(ctx, metrics.FailedRetrievalsPerRequestCount.M(int64(i)))

			break
		}
	}

	// There were no successful queries
	if retrievalStats == nil {
		stats.Record(ctx, metrics.FailedRetrievalsPerRequestCount.M(int64(len(queries))))
		return ErrAllRetrievalsFailed
	}

	return nil
}

// Possible errors: ErrRetrievalRegistrationFailed, ErrProposalCreationFailed,
// ErrRetrievalFailed
func (retriever *Retriever) retrieve(ctx context.Context, query candidateQuery) (*RetrievalStats, error) {
	proposal, err := RetrievalProposalForAsk(query.response, query.candidate.RootCid, nil)
	if err != nil {
		err = fmt.Errorf("%w: %v", ErrProposalCreationFailed, err)
		// since we're prematurely ending the retrieval due to error, we need to
		// simulate a failure event that would otherwise come from filclient so we
		// can properly report it and perform clean-up
		retriever.OnRetrievalEvent(eventpublisher.NewRetrievalEventFailure(
			eventpublisher.RetrievalPhase,
			query.candidate.RootCid,
			query.candidate.MinerPeer.ID,
			address.Undef,
			err.Error()),
		)
		return &RetrievalStats{}, err
	}

	startTime := time.Now()
	retrieveCtx, retrieveCancel := context.WithCancel(ctx)
	defer retrieveCancel()

	var lastBytesReceived uint64 = 0
	var doneLk sync.Mutex
	done := false
	timedOut := false
	var lastBytesReceivedTimer *time.Timer
	var gracefulShutdownTimer *time.Timer

	minerCfgs := retriever.config.GetMinerConfig(query.candidate.MinerPeer.ID)

	resultChan, progressChan, gracefulShutdown := retriever.client.RetrieveContentFromPeerAsync(
		retrieveCtx,
		query.candidate.MinerPeer.ID,
		query.response.PaymentAddress,
		proposal,
	)

	// Start the timeout tracker only if retrieval timeout isn't 0
	if minerCfgs.RetrievalTimeout != 0 {
		lastBytesReceivedTimer = time.AfterFunc(minerCfgs.RetrievalTimeout, func() {
			doneLk.Lock()
			done = true
			doneLk.Unlock()

			// since we're prematurely ending the retrieval due to timeout, we need to
			// simulate a failure event that would otherwise come from filclient so we
			// can properly report it and perform clean-up
			retriever.OnRetrievalEvent(eventpublisher.NewRetrievalEventFailure(
				eventpublisher.RetrievalPhase,
				query.candidate.RootCid,
				query.candidate.MinerPeer.ID,
				address.Undef,
				fmt.Sprintf("timeout after %s", minerCfgs.RetrievalTimeout),
			))
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
						lastBytesReceivedTimer.Reset(minerCfgs.RetrievalTimeout)
						lastBytesReceived = bytesReceived
					}
				}
				doneLk.Unlock()
			}
		}
	}

	if timedOut {
		err = fmt.Errorf(
			"timed out after not receiving data for %s (started %s ago, stopped at %s)",
			minerCfgs.RetrievalTimeout,
			time.Since(startTime),
			humanize.IBytes(lastBytesReceived),
		)
	}
	if err != nil {
		retriever.minerMonitor.recordFailure(query.candidate.MinerPeer.ID)
	}
	// TODO: temporary measure, remove when filclient properly returns data on
	// failure
	if stats == nil {
		stats = &RetrievalStats{
			Size:     lastBytesReceived,
			Duration: time.Since(startTime),
		}
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

// Returns a list of SPs known to have the requested block, with blacklisted
// SPs filtered out.
//
// Possible errors - ErrInvalidEndpointURL, ErrEndpointRequestFailed, ErrEndpointBodyInvalid
func (retriever *Retriever) lookupCandidates(ctx context.Context, cid cid.Cid) ([]RetrievalCandidate, error) {
	unfiltered, err := retriever.endpoint.FindCandidates(ctx, cid)
	if len(unfiltered) > 0 {
		stats.Record(ctx, metrics.RequestWithIndexerCandidatesCount.M(1))
	}
	stats.Record(ctx, metrics.IndexerCandidatesPerRequestCount.M(int64(len(unfiltered))))

	if err != nil {
		return nil, err
	}

	// Remove blacklisted SPs, or non-whitelisted SPs
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

		// Skip suspended SPs from the minerMonitor
		if retriever.minerMonitor.suspended(candidate.MinerPeer.ID) {
			continue
		}

		// Skip if we are currently at our maximum concurrent retrievals for this SP
		// since we likely won't be able to retrieve from them at the moment even if
		// query is successful
		minerConfig := retriever.config.GetMinerConfig(candidate.MinerPeer.ID)
		if minerConfig.MaxConcurrentRetrievals > 0 &&
			retriever.activeRetrievals.GetActiveRetrievalCountFor(candidate.MinerPeer.ID) >= minerConfig.MaxConcurrentRetrievals {
			continue
		}

		res = append(res, candidate)
	}

	return res, nil
}

func (retriever *Retriever) queryCandidates(ctx context.Context, retrievalId uuid.UUID, cid cid.Cid, candidates []RetrievalCandidate) []candidateQuery {
	var queries []candidateQuery
	var queriesLk sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(candidates))

	for i, candidate := range candidates {
		go func(i int, candidate RetrievalCandidate) {
			defer wg.Done()

			queryCtx := ctx
			minerCfgs := retriever.config.GetMinerConfig(candidate.MinerPeer.ID)
			if minerCfgs.RetrievalTimeout != 0 {
				var cancelFunc func()
				queryCtx, cancelFunc = context.WithDeadline(queryCtx, time.Now().Add(minerCfgs.RetrievalTimeout))
				defer cancelFunc()
			}

			query, err := retriever.client.RetrievalQueryToPeer(queryCtx, candidate.MinerPeer, candidate.RootCid)
			if err != nil {
				log.Warnf(
					"Failed to query miner %s for %s: %v",
					candidate.MinerPeer.ID,
					formatCidAndRoot(cid, candidate.RootCid, false),
					err,
				)
				retriever.minerMonitor.recordFailure(candidate.MinerPeer.ID)
				return
			}

			if query.Status != retrievalmarket.QueryResponseAvailable {
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

// Implement RetrievalSubscriber
func (retriever *Retriever) OnRetrievalEvent(event eventpublisher.RetrievalEvent) {
	logEvent(event)

	retrievalId, retrievalCid, phaseStartTime, has := retriever.activeRetrievals.GetStatusFor(event.PayloadCid(), event.Phase())

	if !has {
		log.Errorf("Received event [%s] for unexpected retrieval: payload-cid=%s, storage-provider-id=%s", event.Code(), event.PayloadCid(), event.StorageProviderId())
		return
	}
	ctx := context.Background()

	switch ret := event.(type) {
	case eventpublisher.RetrievalEventFailure:

		msg := ret.ErrorMessage()

		if event.Phase() == eventpublisher.QueryPhase {
			var matched bool
			for substr, metric := range metrics.QueryErrorMetricMatches {
				if strings.Contains(msg, substr) {
					stats.Record(ctx, metric.M(1))
					matched = true
					break
				}
			}
			if !matched {
				stats.Record(ctx, metrics.QueryErrorOtherCount.M(1))
			}
			retriever.activeRetrievals.QueryCandidatedFinished(retrievalCid)
			retriever.eventManager.FireQueryFailure(
				retrievalId,
				event.PayloadCid(),
				phaseStartTime,
				event.StorageProviderId(),
				msg,
			)
		} else {

			var matched bool
			for substr, metric := range metrics.ErrorMetricMatches {
				if strings.Contains(msg, substr) {
					stats.Record(ctx, metric.M(1))
					matched = true
					break
				}
			}
			if !matched {
				stats.Record(ctx, metrics.RetrievalErrorOtherCount.M(1))
			}

			retriever.activeRetrievals.RetrievalCandidatedFinished(retrievalCid, false)
			retriever.eventManager.FireRetrievalFailure(
				retrievalId,
				event.PayloadCid(),
				phaseStartTime,
				event.StorageProviderId(),
				msg,
			)
		}
	case eventpublisher.RetrievalEventQueryAsk: // query-ask success

		retriever.activeRetrievals.QueryCandidatedFinished(retrievalCid)
		retriever.eventManager.FireQuerySuccess(
			retrievalId,
			event.PayloadCid(),
			phaseStartTime,
			event.StorageProviderId(),
			ret.QueryResponse(),
		)
		if ret.QueryResponse().Status == retrievalmarket.QueryResponseError {
			var matched bool
			for substr, metric := range metrics.QueryResponseMetricMatches {
				if strings.Contains(ret.QueryResponse().Message, substr) {
					stats.Record(ctx, metric.M(1))
					matched = true
					break
				}
			}
			if !matched {
				stats.Record(ctx, metrics.QueryErrorOtherCount.M(1))
			}
		}
	case eventpublisher.RetrievalEventSuccess:
		confirmed, err := retriever.confirm(event.PayloadCid())
		if err != nil {
			log.Errorf("Error while confirming block [%s] for retrieval [%s]: %w", event.PayloadCid(), retrievalId, err)
		}
		retriever.activeRetrievals.RetrievalCandidatedFinished(retrievalCid, true)
		retriever.eventManager.FireRetrievalSuccess(
			retrievalId,
			event.PayloadCid(),
			phaseStartTime,
			event.StorageProviderId(),
			ret.ReceivedSize(),
			ret.ReceivedCids(),
			confirmed,
		)
	default:
		if event.Phase() == eventpublisher.QueryPhase {
			retriever.eventManager.FireQueryProgress(
				retrievalId,
				event.PayloadCid(),
				phaseStartTime,
				event.StorageProviderId(),
				event.Code(),
			)
		} else {
			retriever.eventManager.FireRetrievalProgress(
				retrievalId,
				event.PayloadCid(),
				phaseStartTime,
				event.StorageProviderId(),
				event.Code(),
			)
		}
	}
}

func logEvent(event eventpublisher.RetrievalEvent) {
	kv := make([]interface{}, 0)
	logadd := func(kva ...interface{}) {
		if len(kva)%2 != 0 {
			panic("bad number of key/value arguments")
		}
		for i := 0; i < len(kva); i += 2 {
			key, ok := kva[i].(string)
			if !ok {
				panic("expected string key")
			}
			kv = append(kv, key, kva[i+1])
		}
	}
	logadd("code", event.Code(),
		"phase", event.Phase(),
		"payloadCid", event.PayloadCid(),
		"storageProviderId", event.StorageProviderId(),
		"storageProviderAddr", event.StorageProviderAddr())
	switch tevent := event.(type) {
	case eventpublisher.RetrievalEventQueryAsk:
		logadd("queryResponse:Status", tevent.QueryResponse().Status,
			"queryResponse:PieceCIDFound", tevent.QueryResponse().PieceCIDFound,
			"queryResponse:Size", tevent.QueryResponse().Size,
			"queryResponse:PaymentAddress", tevent.QueryResponse().PaymentAddress,
			"queryResponse:MinPricePerByte", tevent.QueryResponse().MinPricePerByte,
			"queryResponse:MaxPaymentInterval", tevent.QueryResponse().MaxPaymentInterval,
			"queryResponse:MaxPaymentIntervalIncrease", tevent.QueryResponse().MaxPaymentIntervalIncrease,
			"queryResponse:Message", tevent.QueryResponse().Message,
			"queryResponse:UnsealPrice", tevent.QueryResponse().UnsealPrice)
	case eventpublisher.RetrievalEventFailure:
		logadd("errorMessage", tevent.ErrorMessage())
	case eventpublisher.RetrievalEventSuccess:
		logadd("receivedSize", tevent.ReceivedSize())
	}
	log.Debugw("retrieval-event", kv...)
}
