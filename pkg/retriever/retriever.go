package retriever

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/metrics"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opencensus.io/stats"
)

var (
	ErrRetrieverNotStarted         = errors.New("retriever not started")
	ErrDealProposalFailed          = errors.New("deal proposal failed")
	ErrNoCandidates                = errors.New("no candidates")
	ErrUnexpectedRetrieval         = errors.New("unexpected active retrieval")
	ErrHitRetrievalLimit           = errors.New("hit retrieval limit")
	ErrProposalCreationFailed      = errors.New("proposal creation failed")
	ErrRetrievalRegistrationFailed = errors.New("retrieval registration failed")
	ErrRetrievalFailed             = errors.New("retrieval failed")
	ErrAllRetrievalsFailed         = errors.New("all retrievals failed")
	ErrQueryFailed                 = errors.New("query failed")
	ErrAllQueriesFailed            = errors.New("all queries failed")
	ErrRetrievalTimedOut           = errors.New("retrieval timed out")
	ErrRetrievalAlreadyRunning     = errors.New("retrieval already running for CID")
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
	PaidRetrievals     bool
}

func (cfg *RetrieverConfig) getMinerConfig(peer peer.ID) MinerConfig {
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
	config       RetrieverConfig
	executor     types.Retriever
	eventManager *events.EventManager
	spTracker    *spTracker
}

type CandidateFinder interface {
	FindCandidates(context.Context, cid.Cid) ([]types.RetrievalCandidate, error)
	FindCandidatesAsync(context.Context, cid.Cid) (<-chan types.FindCandidatesResult, error)
}

type eventStats struct {
	failedCount        int64
	queryCount         int64
	filteredQueryCount int64
}

func NewRetriever(
	ctx context.Context,
	config RetrieverConfig,
	client RetrievalClient,
	candidateFinder CandidateFinder,
) (*Retriever, error) {
	retriever := &Retriever{
		config:       config,
		eventManager: events.NewEventManager(ctx),
		spTracker:    newSpTracker(nil),
	}
	executor := &Executor{
		GetStorageProviderTimeout:   retriever.getStorageProviderTimeout,
		IsAcceptableStorageProvider: retriever.isAcceptableStorageProvider,
		IsAcceptableQueryResponse:   retriever.isAcceptableQueryResponse,
		Client:                      client,
	}
	retrievalCandidateFinder := NewRetrievalCandidateFinder(candidateFinder, retriever.isAcceptableStorageProvider)
	retriever.executor = types.WithCandidates(retrievalCandidateFinder.FindCandidates, executor.RetrieveFromCandidates)

	return retriever, nil
}

// Start will start the retriever events system
func (retriever *Retriever) Start() {
	retriever.eventManager.Start()
}

// Stop will stop the retriever events system and return a channel that will be
// closed when shutdown has completed
func (retriever *Retriever) Stop() chan struct{} {
	return retriever.eventManager.Stop()
}

// RegisterSubscriber registers a subscriber to receive all events fired during the
// process of making a retrieval, including the process of querying available
// storage providers to find compatible ones to attempt retrieval from.
func (retriever *Retriever) RegisterSubscriber(subscriber types.RetrievalEventSubscriber) func() {
	return retriever.eventManager.RegisterSubscriber(subscriber)
}

func (retriever *Retriever) getStorageProviderTimeout(storageProviderId peer.ID) time.Duration {
	return retriever.config.getMinerConfig(storageProviderId).RetrievalTimeout
}

// isAcceptableStorageProvider checks whether the storage provider in question
// is acceptable as a retrieval candidate. It checks the blacklists and
// whitelists, the miner monitor for failures and whether we are already at
// concurrency limit for this SP.
func (retriever *Retriever) isAcceptableStorageProvider(storageProviderId peer.ID) bool {
	// Skip blacklist
	if retriever.config.MinerBlacklist[storageProviderId] {
		return false
	}

	// Skip non-whitelist IF the whitelist isn't empty
	if len(retriever.config.MinerWhitelist) > 0 && !retriever.config.MinerWhitelist[storageProviderId] {
		return false
	}

	// Skip suspended SPs from the minerMonitor
	if retriever.spTracker.IsSuspended(storageProviderId) {
		return false
	}

	// Skip if we are currently at our maximum concurrent retrievals for this SP
	// since we likely won't be able to retrieve from them at the moment even if
	// query is successful
	minerConfig := retriever.config.getMinerConfig(storageProviderId)
	if minerConfig.MaxConcurrentRetrievals > 0 &&
		retriever.spTracker.GetConcurrency(storageProviderId) >= minerConfig.MaxConcurrentRetrievals {
		return false
	}

	return true
}

// isAcceptableQueryResponse determines whether a queryResponse is acceptable
// according to the current configuration. For now this is just checking whether
// PaidRetrievals is set and not accepting paid retrievals if so.
func (retriever *Retriever) isAcceptableQueryResponse(peer peer.ID, req types.RetrievalRequest, queryResponse *retrievalmarket.QueryResponse) bool {
	// filter out paid retrievals if necessary

	acceptable := retriever.config.PaidRetrievals || big.Add(big.Mul(queryResponse.MinPricePerByte, big.NewIntUnsigned(queryResponse.Size)), queryResponse.UnsealPrice).Equals(big.Zero())
	if !acceptable {
		log.Debugf("skipping query response from %s for %s: paid retrieval not allowed", peer, req.Cid)
		retriever.spTracker.RemoveStorageProviderFromRetrieval(peer, req.RetrievalID)
	}
	return acceptable
}

// Retrieve attempts to retrieve the given CID using the configured
// CandidateFinder to find storage providers that should have the CID.
func (retriever *Retriever) Retrieve(
	ctx context.Context,
	request types.RetrievalRequest,
	eventsCB func(types.RetrievalEvent),
) (*types.RetrievalStats, error) {

	if !retriever.eventManager.IsStarted() {
		return nil, ErrRetrieverNotStarted
	}
	if !retriever.spTracker.RegisterRetrieval(request.RetrievalID, request.Cid) {
		return nil, fmt.Errorf("%w: %s", ErrRetrievalAlreadyRunning, request.Cid)
	}
	defer func() {
		if err := retriever.spTracker.EndRetrieval(request.RetrievalID); err != nil {
			log.Errorf("failed to end retrieval tracking for %s: %s", request.Cid, err.Error())
		}
	}()

	// setup the event handler to track progress
	eventStats := &eventStats{}
	onRetrievalEvent := makeOnRetrievalEvent(ctx,
		retriever.eventManager,
		retriever.spTracker,
		request.Cid,
		request.RetrievalID,
		eventStats,
		eventsCB,
	)

	// retrieve, note that we could get a successful retrieval
	// (retrievalStats!=nil) _and_ also an error return because there may be
	// multiple failures along the way, if we got a retrieval then we'll pretend
	// to our caller that there was no error
	retrievalStats, err := retriever.executor(
		ctx,
		request,
		onRetrievalEvent,
	)
	if err != nil && retrievalStats == nil {
		return nil, err
	}

	// success
	log.Infof(
		"Successfully retrieved from miner %s for %s\n"+
			"\tDuration: %s\n"+
			"\tBytes Received: %s\n"+
			"\tTotal Payment: %s",
		retrievalStats.StorageProviderId,
		request.Cid,
		retrievalStats.Duration,
		humanize.IBytes(retrievalStats.Size),
		types.FIL(retrievalStats.TotalPayment),
	)

	stats.Record(ctx, metrics.RetrievalDealActiveCount.M(-1))
	stats.Record(ctx, metrics.RetrievalDealSuccessCount.M(1))
	stats.Record(ctx, metrics.RetrievalDealDuration.M(retrievalStats.Duration.Seconds()))
	stats.Record(ctx, metrics.RetrievalDealSize.M(int64(retrievalStats.Size)))
	stats.Record(ctx, metrics.RetrievalDealCost.M(retrievalStats.TotalPayment.Int64()))
	stats.Record(ctx, metrics.FailedRetrievalsPerRequestCount.M(eventStats.failedCount))
	stats.Record(ctx, metrics.SuccessfulQueriesPerRequestCount.M(eventStats.queryCount))
	stats.Record(ctx, metrics.SuccessfulQueriesPerRequestFilteredCount.M(eventStats.filteredQueryCount))

	return retrievalStats, nil
}

// Implement RetrievalSubscriber
func makeOnRetrievalEvent(
	ctx context.Context,
	eventManager *events.EventManager,
	spTracker *spTracker,
	retrievalCid cid.Cid,
	retrievalId types.RetrievalID,
	eventStats *eventStats,
	eventsCb func(event types.RetrievalEvent),
) func(event types.RetrievalEvent) {
	// this callback is only called in the main retrieval goroutine so is safe to
	// modify local values (eventStats) without synchronization
	return func(event types.RetrievalEvent) {
		logEvent(event)

		switch ret := event.(type) {
		case events.RetrievalEventCandidatesFound:
			handleCandidatesFoundEvent(ret)
		case events.RetrievalEventCandidatesFiltered:
			handleCandidatesFilteredEvent(retrievalId, spTracker, retrievalCid, ret)
		case events.RetrievalEventStarted:
			handleStartedEvent(ret)
		case events.RetrievalEventFailed:
			handleFailureEvent(ctx, spTracker, retrievalId, eventStats, ret)
		case events.RetrievalEventQueryAsked: // query-ask success
			handleQueryAskEvent(ctx, eventStats, ret)
		case events.RetrievalEventQueryAskedFiltered:
			handleQueryAskFilteredEvent(eventStats)
		}

		eventManager.DispatchEvent(event)
		if eventsCb != nil {
			eventsCb(event)
		}
	}
}

func handleQueryAskFilteredEvent(eventStats *eventStats) {
	eventStats.filteredQueryCount++
	if eventStats.filteredQueryCount == 1 {
		stats.Record(context.Background(), metrics.RequestWithSuccessfulQueriesFilteredCount.M(1))
	}
}

func handleQueryAskEvent(
	ctx context.Context,
	eventStats *eventStats,
	event events.RetrievalEventQueryAsked,
) {
	eventStats.queryCount++
	if eventStats.queryCount == 1 {
		stats.Record(context.Background(), metrics.RequestWithSuccessfulQueriesCount.M(1))
	}

	if event.QueryResponse().Status == retrievalmarket.QueryResponseError {
		var matched bool
		for substr, metric := range metrics.QueryResponseMetricMatches {
			if strings.Contains(event.QueryResponse().Message, substr) {
				stats.Record(ctx, metric.M(1))
				matched = true
				break
			}
		}
		if !matched {
			stats.Record(ctx, metrics.QueryErrorOtherCount.M(1))
		}
	}
}

// handleFailureEvent is called when a query _or_ retrieval fails
func handleFailureEvent(
	ctx context.Context,
	spTracker *spTracker,
	retrievalId types.RetrievalID,
	eventStats *eventStats,
	event events.RetrievalEventFailed,
) {
	if event.Phase() != types.IndexerPhase { // indexer failures don't have a storageProviderId
		spTracker.RecordFailure(event.StorageProviderId(), retrievalId)
	}

	msg := event.ErrorMessage()

	switch event.Phase() {
	case types.QueryPhase:
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
	case types.RetrievalPhase:
		eventStats.failedCount++
		log.Warnf(
			"Failed to retrieve from miner %s for %s: %s",
			event.StorageProviderId(),
			event.PayloadCid(),
			event.ErrorMessage(),
		)
		stats.Record(context.Background(), metrics.RetrievalDealFailCount.M(1))
		stats.Record(context.Background(), metrics.RetrievalDealActiveCount.M(-1))

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
	}
}

func handleStartedEvent(event events.RetrievalEventStarted) {
	if event.Phase() == types.RetrievalPhase {
		stats.Record(context.Background(), metrics.RetrievalRequestCount.M(1))
		stats.Record(context.Background(), metrics.RetrievalDealActiveCount.M(1))
	}
}

func handleCandidatesFilteredEvent(
	retrievalId types.RetrievalID,
	spTracker *spTracker,
	retrievalCid cid.Cid,
	event events.RetrievalEventCandidatesFiltered,
) {
	if len(event.Candidates()) > 0 {
		stats.Record(context.Background(), metrics.RequestWithIndexerCandidatesFilteredCount.M(1))
		ids := make([]peer.ID, 0)
		for _, c := range event.Candidates() {
			ids = append(ids, c.MinerPeer.ID)
		}
		if err := spTracker.AddToRetrieval(retrievalId, ids); err != nil {
			log.Errorf("failed to add storage providers to tracked retrieval for %s: %s", retrievalCid, err.Error())
		}
	}
}

func handleCandidatesFoundEvent(event events.RetrievalEventCandidatesFound) {
	if len(event.Candidates()) > 0 {
		stats.Record(context.Background(), metrics.RequestWithIndexerCandidatesCount.M(1))
	}
	stats.Record(context.Background(), metrics.IndexerCandidatesPerRequestCount.M(int64(len(event.Candidates()))))
}

func logEvent(event types.RetrievalEvent) {
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
		"storageProviderId", event.StorageProviderId())
	switch tevent := event.(type) {
	case events.EventWithQueryResponse:
		logadd("queryResponse:Status", tevent.QueryResponse().Status,
			"queryResponse:PieceCIDFound", tevent.QueryResponse().PieceCIDFound,
			"queryResponse:Size", tevent.QueryResponse().Size,
			"queryResponse:PaymentAddress", tevent.QueryResponse().PaymentAddress,
			"queryResponse:MinPricePerByte", tevent.QueryResponse().MinPricePerByte,
			"queryResponse:MaxPaymentInterval", tevent.QueryResponse().MaxPaymentInterval,
			"queryResponse:MaxPaymentIntervalIncrease", tevent.QueryResponse().MaxPaymentIntervalIncrease,
			"queryResponse:Message", tevent.QueryResponse().Message,
			"queryResponse:UnsealPrice", tevent.QueryResponse().UnsealPrice)
	case events.EventWithCandidates:
		var cands = strings.Builder{}
		for _, c := range tevent.Candidates() {
			cands.WriteString(c.MinerPeer.ID.String())
		}
		logadd("candidates", cands.String())
	case events.RetrievalEventFailed:
		logadd("errorMessage", tevent.ErrorMessage())
	case events.RetrievalEventSuccess:
		logadd("receivedSize", tevent.ReceivedSize())
	}
	log.Debugw("retrieval-event", kv...)
}
