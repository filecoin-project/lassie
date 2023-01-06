package retriever

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/eventpublisher"
	"github.com/filecoin-project/lassie/pkg/metrics"
	"github.com/filecoin-project/lassie/pkg/types"
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
	ErrQueryFailed                 = errors.New("query failed")
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

type retrievalInstrumentation struct {
	retriever           *Retriever
	cid                 cid.Cid
	startingRetrievalCb func(candidateCount int) error
	queryCount          int64
	filteredQueryCount  int64
	failedCount         int64
}

func (ri *retrievalInstrumentation) OnRetrievalCandidatesFound(foundCount int) error {
	if foundCount > 0 {
		stats.Record(context.Background(), metrics.RequestWithIndexerCandidatesCount.M(1))
	}
	stats.Record(context.Background(), metrics.IndexerCandidatesPerRequestCount.M(int64(foundCount)))
	return nil
}

func (ri *retrievalInstrumentation) OnRetrievalCandidatesFiltered(filteredCount int) error {
	if filteredCount == 0 {
		return nil
	}
	stats.Record(context.Background(), metrics.RequestWithIndexerCandidatesFilteredCount.M(1))
	return ri.startingRetrievalCb(filteredCount)
}

func (ri *retrievalInstrumentation) OnErrorQueryingRetrievalCandidate(candidate RetrievalCandidate, err error) {
	ri.retriever.minerMonitor.recordFailure(candidate.MinerPeer.ID)
}

func (ri *retrievalInstrumentation) OnErrorRetrievingFromCandidate(candidate RetrievalCandidate, err error) {
	// need to simulate error events locally because they don't arise from the client
	if errors.Is(err, ErrRetrievalTimedOut) {
		ri.retriever.OnRetrievalEvent(eventpublisher.NewRetrievalEventFailure(
			eventpublisher.RetrievalPhase,
			candidate.RootCid,
			candidate.MinerPeer.ID,
			address.Undef,
			fmt.Sprintf("timeout after %s", ri.retriever.getStorageProviderTimeout(candidate.MinerPeer.ID)),
		))
	} else if errors.Is(err, ErrProposalCreationFailed) {
		ri.retriever.OnRetrievalEvent(eventpublisher.NewRetrievalEventFailure(
			eventpublisher.RetrievalPhase,
			candidate.RootCid,
			candidate.MinerPeer.ID,
			address.Undef,
			err.Error()),
		)
	}
	atomic.AddInt64(&ri.failedCount, 1)
	log.Warnf(
		"Failed to retrieve from miner %s for %s: %v",
		candidate.MinerPeer.ID,
		ri.cid,
		err,
	)
	stats.Record(context.Background(), metrics.RetrievalDealFailCount.M(1))
	stats.Record(context.Background(), metrics.RetrievalDealActiveCount.M(-1))
	ri.retriever.minerMonitor.recordFailure(candidate.MinerPeer.ID)
}

func (ri *retrievalInstrumentation) OnRetrievalQueryForCandidate(candidate RetrievalCandidate, queryResponse *retrievalmarket.QueryResponse) {
	qc := atomic.AddInt64(&ri.queryCount, 1)
	if qc == 1 {
		stats.Record(context.Background(), metrics.RequestWithSuccessfulQueriesCount.M(1))
	}
}

func (ri *retrievalInstrumentation) OnFilteredRetrievalQueryForCandidate(candidate RetrievalCandidate, queryResponse *retrievalmarket.QueryResponse) {
	fqc := atomic.AddInt64(&ri.filteredQueryCount, 1)
	if fqc == 1 {
		stats.Record(context.Background(), metrics.RequestWithSuccessfulQueriesFilteredCount.M(1))
	}
	// register that we have this many candidates to retrieve from, so that when we
	// receive success or failures from that many we know the phase is completed,
	// if zero at this point then clean-up will occur
	ri.retriever.activeRetrievals.SetRetrievalCandidateCount(candidate.RootCid, int(fqc))
}

func (ri *retrievalInstrumentation) OnRetrievingFromCandidate(candidate RetrievalCandidate) {
	stats.Record(context.Background(), metrics.RetrievalRequestCount.M(1))
	stats.Record(context.Background(), metrics.RetrievalDealActiveCount.M(1))
}

func (retriever *Retriever) getStorageProviderTimeout(storageProviderId peer.ID) time.Duration {
	return retriever.config.GetMinerConfig(storageProviderId).RetrievalTimeout
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

// isAcceptableQueryResponse determines whether a queryResponse is acceptable
// according to the current configuration. For now this is just checking whether
// PaidRetrievals is set and not accepting paid retrievals if so.
func (retriever *Retriever) isAcceptableQueryResponse(queryResponse *retrievalmarket.QueryResponse) bool {
	// filter out paid retrievals if necessary
	return retriever.config.PaidRetrievals || totalCost(queryResponse).Equals(big.Zero())
}

func (retriever *Retriever) Request(cid cid.Cid) error {
	ctx := context.Background()

	// instrumentation receives events primarily responsible for metrics reporting
	// but we also use it for activeRetrievals management while we still need to
	// deal with that here
	instrumentation := &retrievalInstrumentation{
		retriever: retriever,
		cid:       cid,
		startingRetrievalCb: func(filteredCount int) error {
			// We register that we have len(candidates) candidates to query, so that when
			// we receive success or failures from that many we know the phase is
			// completed. We also pre-emptively suggest we're expecting at least one
			// retrieval to occur. But this number will be updated as queries come in,
			// but at this point we want to avoid a (unlikely) race condition where we
			// get all expected success/failures for queries and trigger a clean-up
			// before we have a chance to set the correct count
			if _, err := retriever.activeRetrievals.New(cid, filteredCount, 1); err != nil {
				return err
			}
			return nil
		},
	}

	config := &RetrievalConfig{
		Instrumentation:             instrumentation,
		GetStorageProviderTimeout:   retriever.getStorageProviderTimeout,
		IsAcceptableStorageProvider: retriever.isAcceptableStorageProvider,
		IsAcceptableQueryResponse:   retriever.isAcceptableQueryResponse,
	}

	// retrieve, note that we could get a successful retrieval
	// (retrievalStats!=nil) _and_ also an error return because there may be
	// multiple failures along the way, if we got a retrieval then we'll pretend
	// to our caller that there was no error
	retrievalStats, err := Retrieve(ctx, config, retriever.endpoint, retriever.client, cid)
	if err != nil {
		if errors.Is(err, ErrAllQueriesFailed) {
			// tell the ActiveRetrievalsManager not to expect any retrievals for this
			// CID, so it can be closed out
			retriever.activeRetrievals.SetRetrievalCandidateCount(cid, 0)
		}
		if retrievalStats == nil {
			return err
		}
	}

	// success
	log.Infof(
		"Successfully retrieved from miner %s for %s\n"+
			"\tDuration: %s\n"+
			"\tBytes Received: %s\n"+
			"\tTotal Payment: %s",
		retrievalStats.StorageProviderId,
		cid,
		retrievalStats.Duration,
		humanize.IBytes(retrievalStats.Size),
		types.FIL(retrievalStats.TotalPayment),
	)

	stats.Record(ctx, metrics.RetrievalDealActiveCount.M(-1))
	stats.Record(ctx, metrics.RetrievalDealSuccessCount.M(1))
	stats.Record(ctx, metrics.RetrievalDealDuration.M(retrievalStats.Duration.Seconds()))
	stats.Record(ctx, metrics.RetrievalDealSize.M(int64(retrievalStats.Size)))
	stats.Record(ctx, metrics.RetrievalDealCost.M(retrievalStats.TotalPayment.Int64()))
	stats.Record(ctx, metrics.FailedRetrievalsPerRequestCount.M(atomic.LoadInt64(&instrumentation.failedCount)))
	stats.Record(ctx, metrics.SuccessfulQueriesPerRequestCount.M(atomic.LoadInt64(&instrumentation.queryCount)))
	stats.Record(ctx, metrics.SuccessfulQueriesPerRequestFilteredCount.M(atomic.LoadInt64(&instrumentation.filteredQueryCount)))

	return nil
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
