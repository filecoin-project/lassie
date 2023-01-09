package retriever

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
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
	candidateFinder  CandidateFinder
	client           RetrievalClient
	eventManager     *EventManager
	activeRetrievals *ActiveRetrievalsManager
	minerMonitor     *minerMonitor
	confirm          func(cid.Cid) (bool, error)
}

type CandidateFinder interface {
	FindCandidates(context.Context, cid.Cid) ([]types.RetrievalCandidate, error)
}

type BlockConfirmer func(c cid.Cid) (bool, error)

func NewRetriever(
	ctx context.Context,
	config RetrieverConfig,
	client RetrievalClient,
	candidateFinder CandidateFinder,
	confirmer BlockConfirmer,
) (*Retriever, error) {
	retriever := &Retriever{
		config:           config,
		candidateFinder:  candidateFinder,
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

	return retriever, nil
}

// RegisterListener registers a listener to receive all events fired during the
// process of making a retrieval, including the process of querying available
// storage providers to find compatible ones to attempt retrieval from.
func (retriever *Retriever) RegisterListener(listener RetrievalEventListener) func() {
	return retriever.eventManager.RegisterListener(listener)
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

// Retrieve attempts to retrieve the given CID using the configured
// CandidateFinder to find storage providers that should have the CID.
func (retriever *Retriever) Retrieve(ctx context.Context, cid cid.Cid) (*RetrievalStats, error) {
	config := &RetrievalConfig{
		GetStorageProviderTimeout:   retriever.getStorageProviderTimeout,
		IsAcceptableStorageProvider: retriever.isAcceptableStorageProvider,
		IsAcceptableQueryResponse:   retriever.isAcceptableQueryResponse,
	}

	var failedCount int64
	var queryCount int64
	var filteredQueryCount int64

	// retrieve, note that we could get a successful retrieval
	// (retrievalStats!=nil) _and_ also an error return because there may be
	// multiple failures along the way, if we got a retrieval then we'll pretend
	// to our caller that there was no error
	retrievalStats, err := RetrieveFromCandidates(ctx, config, retriever.candidateFinder, retriever.client, cid, func(event eventpublisher.RetrievalEvent) {
		// handle special-case retrieval failure event (could be >1 of these per Retrieve() call)
		switch ret := event.(type) {
		case eventpublisher.RetrievalEventCandidatesFound:
			if len(ret.Candidates()) > 0 {
				stats.Record(context.Background(), metrics.RequestWithIndexerCandidatesCount.M(1))
			}
			stats.Record(context.Background(), metrics.IndexerCandidatesPerRequestCount.M(int64(len(ret.Candidates()))))
		case eventpublisher.RetrievalEventCandidatesFiltered:
			if len(ret.Candidates()) > 0 {
				stats.Record(context.Background(), metrics.RequestWithIndexerCandidatesFilteredCount.M(1))
				// We register that we have len(candidates) candidates to query, so that when
				// we receive success or failures from that many we know the phase is
				// completed. We also pre-emptively suggest we're expecting at least one
				// retrieval to occur. But this number will be updated as queries come in,
				// but at this point we want to avoid a (unlikely) race condition where we
				// get all expected success/failures for queries and trigger a clean-up
				// before we have a chance to set the correct count
				retriever.activeRetrievals.New(event.PayloadCid(), len(ret.Candidates()), 1) // TODO: handle err
			}
		case eventpublisher.RetrievalEventQueryAsk:
			qc := atomic.AddInt64(&queryCount, 1)
			if qc == 1 {
				stats.Record(context.Background(), metrics.RequestWithSuccessfulQueriesCount.M(1))
			}
		case eventpublisher.RetrievalEventQueryAskFiltered:
			fqc := atomic.AddInt64(&filteredQueryCount, 1)
			if fqc == 1 {
				stats.Record(context.Background(), metrics.RequestWithSuccessfulQueriesFilteredCount.M(1))
			}
			// register that we have this many candidates to retrieve from, so that when we
			// receive success or failures from that many we know the phase is completed,
			// if zero at this point then clean-up will occur
			retriever.activeRetrievals.SetRetrievalCandidateCount(ret.PayloadCid(), int(fqc))
		case eventpublisher.RetrievalEventFailure:
			if ret.Phase() == eventpublisher.RetrievalPhase {
				atomic.AddInt64(&failedCount, 1)
				log.Warnf(
					"Failed to retrieve from miner %s for %s: %s",
					event.StorageProviderId(),
					event.PayloadCid(),
					ret.ErrorMessage(),
				)
				stats.Record(context.Background(), metrics.RetrievalDealFailCount.M(1))
				stats.Record(context.Background(), metrics.RetrievalDealActiveCount.M(-1))
			}
			retriever.minerMonitor.recordFailure(ret.StorageProviderId())
		case eventpublisher.RetrievalEventStarted:
			if ret.Phase() == eventpublisher.RetrievalPhase {
				stats.Record(context.Background(), metrics.RetrievalRequestCount.M(1))
				stats.Record(context.Background(), metrics.RetrievalDealActiveCount.M(1))
			}
		}

		retriever.onRetrievalEvent(event)
	})
	if err != nil {
		if errors.Is(err, ErrAllQueriesFailed) {
			// tell the ActiveRetrievalsManager not to expect any retrievals for this
			// CID, so it can be closed out
			retriever.activeRetrievals.SetRetrievalCandidateCount(cid, 0)
		}
		if retrievalStats == nil {
			return nil, err
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
	stats.Record(ctx, metrics.FailedRetrievalsPerRequestCount.M(atomic.LoadInt64(&failedCount)))
	stats.Record(ctx, metrics.SuccessfulQueriesPerRequestCount.M(atomic.LoadInt64(&queryCount)))
	stats.Record(ctx, metrics.SuccessfulQueriesPerRequestFilteredCount.M(atomic.LoadInt64(&filteredQueryCount)))

	return retrievalStats, nil
}

// Implement RetrievalSubscriber
func (retriever *Retriever) onRetrievalEvent(event eventpublisher.RetrievalEvent) {
	logEvent(event)

	if event.Code() == eventpublisher.CandidatesFoundCode {
		// activeRetrievals not set up for this yet
		return
	}

	retrievalId, retrievalCid, phaseStartTime, has := retriever.activeRetrievals.GetStatusFor(event.PayloadCid(), event.Phase())

	if !has {
		log.Errorf("Received event [%s / %t] for unexpected retrieval: payload-cid=%s, storage-provider-id=%s", event.Code(), event.PayloadCid(), event.StorageProviderId())
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
		} else if event.Phase() == eventpublisher.RetrievalPhase {
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
		} else if event.Phase() == eventpublisher.RetrievalPhase {
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
		"storageProviderId", event.StorageProviderId())
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
