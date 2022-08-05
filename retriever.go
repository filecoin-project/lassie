package filecoin

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/application-research/autoretrieve/metrics"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/rep"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.opencensus.io/stats"
)

var (
	ErrNoCandidates                = errors.New("no candidates")
	ErrRetrievalAlreadyRunning     = errors.New("retrieval already running")
	ErrUnexpectedRetrieval         = errors.New("unexpected active retrieval")
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
	config           RetrieverConfig
	endpoint         Endpoint
	filClient        *filclient.FilClient
	eventManager     *EventManager
	activeRetrievals *ActiveRetrievalsManager
	minerMonitor     *minerMonitor
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
	ctx context.Context,
	config RetrieverConfig,
	filClient *filclient.FilClient,
	endpoint Endpoint,
) (*Retriever, error) {
	retriever := &Retriever{
		config:           config,
		endpoint:         endpoint,
		filClient:        filClient,
		eventManager:     NewEventManager(ctx),
		activeRetrievals: NewActiveRetrievalsManager(),
		minerMonitor: newMinerMonitor(minerMonitorConfig{
			maxFailuresBeforeSuspend: 5,
			suspensionDuration:       time.Minute,
			failureHistoryDuration:   time.Second * 15,
		}),
	}

	retriever.filClient.SubscribeToRetrievalEvents(retriever)

	return retriever, nil
}

// RegisterListener registers a listener to receive all events fired during the
// process of making a retrieval, including the process of querying available
// storage providers to find compatible ones to attempt retrieval from.
func (retriever *Retriever) RegisterListener(listener RetrievalEventListener) func() {
	return retriever.eventManager.RegisterListener(listener)
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
	// TODO: before looking up candidates from the endpoint, we could cache
	// candidates and use that cached info. We only really have to look up an
	// up-to-date candidate list from the endpoint if we need to begin a new
	// retrieval.
	candidates, err := retriever.lookupCandidates(context.Background(), cid)
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

	// If we got to this point, one or more candidates have been found and we
	// are good to go ahead with the retrieval
	go retriever.retrieveFromBestCandidate(context.Background(), retrievalId, cid, candidates)
	return nil
}

// Takes an unsorted list of candidates, orders them, and attempts retrievals in
// serial until one succeeds.
//
// Possible errors: ErrAllRetrievalsFailed
func (retriever *Retriever) retrieveFromBestCandidate(ctx context.Context, retrievalId uuid.UUID, retrievalCid cid.Cid, candidates []RetrievalCandidate) error {
	queries := retriever.queryCandidates(ctx, retrievalId, retrievalCid, candidates)
	// register that we have this many candidates to retrieve from, so that when we
	// receive success or failures from that many we know the phase is completed,
	// if zero at this point then clean-up will occur
	retriever.activeRetrievals.SetRetrievalCandidateCount(retrievalCid, len(queries))
	if len(queries) == 0 {
		return nil
	}

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
		minerConfig := retriever.config.MinerConfig(query.candidate.MinerPeer.ID)
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
		}

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

			// since we're prematurely ending the retrieval due to timeout, we need to
			// simulate a failure event that would otherwise come from filclient so we
			// can properly report it and perform clean-up
			retriever.OnRetrievalEvent(rep.NewRetrievalEventFailure(
				rep.RetrievalPhase,
				query.candidate.RootCid,
				query.candidate.MinerPeer.ID,
				address.Undef,
				fmt.Sprintf("timeout after %s", minerCfgs.RetrievalTimeout),
			))
			retrieveCancel()
			timedOut = true
		})
	}
	stats, err := retriever.filClient.RetrieveContentFromPeerWithProgressCallback(retrieveCtx, query.candidate.MinerPeer.ID, query.response.PaymentAddress, proposal, func(bytesReceived uint64) {
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
	})
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

// Returns a list of miners known to have the requested block, with blacklisted
// miners filtered out.
//
// Possible errors - ErrInvalidEndpointURL, ErrEndpointRequestFailed, ErrEndpointBodyInvalid
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

		// Skip suspended miners from the miner monitor
		if retriever.minerMonitor.suspended(candidate.MinerPeer.ID) {
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

			minerCfgs := retriever.config.MinerConfigs[candidate.MinerPeer.ID]
			if minerCfgs.RetrievalTimeout != 0 {
				var cancelFunc func()
				ctx, cancelFunc = context.WithDeadline(ctx, time.Now().Add(minerCfgs.RetrievalTimeout))
				defer cancelFunc()
			}

			query, err := retriever.filClient.RetrievalQueryToPeer(ctx, candidate.MinerPeer, candidate.RootCid)
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

// Implement rep.RetrievalSubscriber
func (retriever *Retriever) OnRetrievalEvent(event rep.RetrievalEvent) {
	logEvent(event)

	retrievalId, retrievalCid, phaseStartTime, has := retriever.activeRetrievals.GetStatusFor(event.PayloadCid(), event.Phase())

	if !has {
		log.Errorf("Received event [%s] for unexpected retrieval: payload-cid=%s, storage-provider-id=%s", event.Code, event.PayloadCid(), event.StorageProviderId())
		return
	}

	switch ret := event.(type) {
	case rep.RetrievalEventFailure:
		if event.Phase() == rep.QueryPhase {
			retriever.activeRetrievals.QueryCandidatedFinished(retrievalCid)
			retriever.eventManager.FireQueryFailure(
				retrievalId,
				event.PayloadCid(),
				phaseStartTime,
				event.StorageProviderId(),
				ret.ErrorMessage(),
			)
		} else {
			retriever.activeRetrievals.RetrievalCandidatedFinished(retrievalCid, false)
			retriever.eventManager.FireRetrievalFailure(
				retrievalId,
				event.PayloadCid(),
				phaseStartTime,
				event.StorageProviderId(),
				ret.ErrorMessage(),
			)
		}
	case rep.RetrievalEventQueryAsk: // query-ask success
		retriever.activeRetrievals.QueryCandidatedFinished(retrievalCid)
		retriever.eventManager.FireQuerySuccess(
			retrievalId,
			event.PayloadCid(),
			phaseStartTime,
			event.StorageProviderId(),
			ret.QueryResponse(),
		)
	case rep.RetrievalEventSuccess:
		retriever.activeRetrievals.RetrievalCandidatedFinished(retrievalCid, true)
		retriever.eventManager.FireRetrievalSuccess(
			retrievalId,
			event.PayloadCid(),
			phaseStartTime,
			event.StorageProviderId(),
			ret.ReceivedSize(),
			ret.ReceivedCids(),
		)
	default:
		if event.Phase() == rep.QueryPhase {
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

func logEvent(event rep.RetrievalEvent) {
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
	case rep.RetrievalEventQueryAsk:
		logadd("queryResponse:Status", tevent.QueryResponse().Status,
			"queryResponse:PieceCIDFound", tevent.QueryResponse().PieceCIDFound,
			"queryResponse:Size", tevent.QueryResponse().Size,
			"queryResponse:PaymentAddress", tevent.QueryResponse().PaymentAddress,
			"queryResponse:MinPricePerByte", tevent.QueryResponse().MinPricePerByte,
			"queryResponse:MaxPaymentInterval", tevent.QueryResponse().MaxPaymentInterval,
			"queryResponse:MaxPaymentIntervalIncrease", tevent.QueryResponse().MaxPaymentIntervalIncrease,
			"queryResponse:Message", tevent.QueryResponse().Message,
			"queryResponse:UnsealPrice", tevent.QueryResponse().UnsealPrice)
	case rep.RetrievalEventFailure:
		logadd("errorMessage", tevent.ErrorMessage())
	case rep.RetrievalEventSuccess:
		logadd("receivedSize", tevent.ReceivedSize())
	}
	log.Debugw("retrieval-event", kv...)
}
