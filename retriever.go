package filecoin

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/application-research/autoretrieve/filecoin/eventrecorder"
	"github.com/application-research/autoretrieve/metrics"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/rep"
	"github.com/application-research/filclient/retrievehelper"
	"github.com/dustin/go-humanize"
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

type activeRetrieval struct {
	retrievalId       uuid.UUID
	storageProviderId peer.ID
}

type Retriever struct {
	// Assumed immutable during operation
	config RetrieverConfig

	endpoint      Endpoint
	filClient     *filclient.FilClient
	eventRecorder *eventrecorder.EventRecorder

	activeRetrievals         map[cid.Cid]activeRetrieval
	activeRetrievalsLk       sync.Mutex
	activeRetrievalsPerMiner map[peer.ID]uint

	minerMonitor *minerMonitor
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
	eventRecorder *eventrecorder.EventRecorder,
) (*Retriever, error) {
	retriever := &Retriever{
		config:                   config,
		endpoint:                 endpoint,
		filClient:                filClient,
		eventRecorder:            eventRecorder,
		activeRetrievals:         make(map[cid.Cid]activeRetrieval),
		activeRetrievalsPerMiner: make(map[peer.ID]uint),
		minerMonitor: newMinerMonitor(minerMonitorConfig{
			maxFailuresBeforeSuspend: 5,
			suspensionDuration:       time.Minute,
			failureHistoryDuration:   time.Second * 15,
		}),
	}

	retriever.filClient.SubscribeToRetrievalEvents(retriever)

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
		retrievalId, err := retriever.tryRegisterActiveRetrieval(query.candidate.RootCid, query.candidate.MinerPeer.ID)
		if err != nil {
			// TODO: send some info to metrics about this

			if errors.Is(err, ErrRetrievalAlreadyRunning) {
				break
			}

			continue
		}

		log.Infof(
			"Attempting retrieval from miner %s for %s",
			query.candidate.MinerPeer.ID,
			formatCidAndRoot(cid, query.candidate.RootCid, false),
		)

		stats.Record(ctx, metrics.RetrievalRequestCount.M(1))
		stats.Record(ctx, metrics.RetrievalDealActiveCount.M(1))

		startTime := time.Now()

		if retriever.eventRecorder != nil {
			if err := retriever.eventRecorder.RecordStart(
				retrievalId,
				cid,
				query.candidate.RootCid,
				query.candidate.MinerPeer.ID,
				startTime,
			); err != nil {
				log.Errorf("Failed to post event to recorder:", err)
			}
		}

		// Make the retrieval
		retrievalStats, err := retriever.retrieve(ctx, query)

		stats.Record(ctx, metrics.RetrievalDealActiveCount.M(-1))
		if err != nil {
			log.Errorf(
				"Failed to retrieve from miner %s for %s: %v",
				query.candidate.MinerPeer.ID,
				formatCidAndRoot(cid, query.candidate.RootCid, false),
				err,
			)
			stats.Record(ctx, metrics.RetrievalDealFailCount.M(1))
			if retriever.eventRecorder != nil {
				if err := retriever.eventRecorder.RecordFailure(
					retrievalId,
					cid,
					query.candidate.MinerPeer.ID,
					err,
				); err != nil {
					log.Errorf("Failed to post event to recorder:", err)
				}
			}
		} else {
			log.Infof(
				"Successfully retrieved from miner %s for %s\n"+
					"\tDuration: %s\n"+
					"\tBytes Received: %s\n"+
					"\tTotal Payment: %s",
				query.candidate.MinerPeer.ID,
				formatCidAndRoot(cid, query.candidate.RootCid, false),
				retrievalStats.Duration,
				humanize.IBytes(retrievalStats.Size),
				types.FIL(retrievalStats.TotalPayment),
			)

			stats.Record(ctx, metrics.RetrievalDealSuccessCount.M(1))
			stats.Record(ctx, metrics.RetrievalDealDuration.M(retrievalStats.Duration.Seconds()))
			stats.Record(ctx, metrics.RetrievalDealSize.M(int64(retrievalStats.Size)))
			stats.Record(ctx, metrics.RetrievalDealCost.M(retrievalStats.TotalPayment.Int64()))

			if err := retriever.eventRecorder.RecordSuccess(
				retrievalId,
				cid,
				query.candidate.MinerPeer.ID,
				retrievalStats,
			); err != nil {
				log.Errorf("Failed to post event to recorder:", err)
			}
		}

		retriever.unregisterActiveRetrieval(query.candidate.RootCid, query.candidate.MinerPeer.ID)

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

// Possible errors: ErrRetrievalAlreadyRunning, ErrHitRetrievalLimit
func (retriever *Retriever) tryRegisterActiveRetrieval(cid cid.Cid, storageProviderId peer.ID) (uuid.UUID, error) {
	retriever.activeRetrievalsLk.Lock()
	defer retriever.activeRetrievalsLk.Unlock()

	if _, ok := retriever.activeRetrievals[cid]; ok {
		return uuid.UUID{}, ErrRetrievalAlreadyRunning
	}

	minerConfig := retriever.config.MinerConfig(storageProviderId)

	// If limit is enabled (non-zero) and we have already hit it, we can't
	// allow this retrieval to start
	noLimit := minerConfig.MaxConcurrentRetrievals == 0
	atLimit := retriever.activeRetrievalsPerMiner[storageProviderId] >= minerConfig.MaxConcurrentRetrievals
	if !noLimit && atLimit {
		return uuid.UUID{}, ErrHitRetrievalLimit
	}

	retrievalId, err := uuid.NewRandom()
	if err != nil {
		return uuid.UUID{}, err
	}

	retriever.activeRetrievals[cid] = activeRetrieval{retrievalId, storageProviderId}
	retriever.activeRetrievalsPerMiner[storageProviderId] += 1

	fmt.Printf("active retrievals: %d (at limit: %v, no limit: %v)\n", retriever.activeRetrievalsPerMiner[storageProviderId], atLimit, noLimit)

	return retrievalId, nil
}

// Unregisters an active retrieval. No-op if no retrieval is running.
func (retriever *Retriever) unregisterActiveRetrieval(cid cid.Cid, storageProviderId peer.ID) {
	retriever.activeRetrievalsLk.Lock()
	defer retriever.activeRetrievalsLk.Unlock()

	delete(retriever.activeRetrievals, cid)
	retriever.activeRetrievalsPerMiner[storageProviderId] = retriever.activeRetrievalsPerMiner[storageProviderId] - 1
	if retriever.activeRetrievalsPerMiner[storageProviderId] == 0 {
		delete(retriever.activeRetrievalsPerMiner, storageProviderId)
	}
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

func (retriever *Retriever) queryCandidates(ctx context.Context, cid cid.Cid, candidates []RetrievalCandidate) []candidateQuery {
	var queries []candidateQuery
	var queriesLk sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(candidates))

	for i, candidate := range candidates {
		go func(i int, candidate RetrievalCandidate) {
			defer wg.Done()

			query, err := retriever.filClient.RetrievalQueryToPeer(ctx, candidate.MinerPeer, candidate.RootCid)
			if err != nil {
				log.Errorf(
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
func (retriever *Retriever) OnRetrievalEvent(event rep.RetrievalEvent, state rep.RetrievalState) {
	log.Debugw("retrieval-event",
		"code", event.Code,
		"status", event.Status,
		"storage-provider-id", state.StorageProviderID,
		"storage-provider-address", state.StorageProviderAddr,
		"client-address", state.ClientID,
		"payload-cid", state.PayloadCid,
		"piece-cid", state.PieceCid,
		"finished-time", state.FinishedTime,
	)

	retriever.activeRetrievalsLk.Lock()
	defer retriever.activeRetrievalsLk.Unlock()

	retrieval, ok := retriever.activeRetrievals[state.PayloadCid]
	if !ok {
		log.Errorf("Received event for unexpected retrieval, no such payload-cid: payload-cid=%v, storage-provider-id=%v", state.PayloadCid, state.StorageProviderID)
		return
	}
	if retrieval.storageProviderId != state.StorageProviderID {
		log.Errorf("Received event for unexpected retrieval, storage-provider-id does not match: payload-cid=%v, storage-provider-id=%v (expected %v)", state.PayloadCid, state.StorageProviderID, retrieval.storageProviderId)
		return
	}

	// retrieval-event {"code": "success", "status": "data transfer for retrieval complete", "storage-provider-id": "12D3KooWBwUERBhJPtZ7hg5N3q1DesvJ67xx9RLdSaStBz9Y6Ny8", "storage-provider-address": "<empty>", "client-address": "", "payload-cid": "bafkreierk262ngztwz2apnmxrpe5hh2wn4ncj3fvf7xylrhhwtqomv53j4", "piece-cid": "<nil>", "finished-time": "2022-07-15T04:35:56.916Z"}

	if retriever.eventRecorder != nil {
		switch event.Code {
		// ignore the success and failure events and handle that in retrieveFromBestCandidate()
		// where we have more information
		case rep.RetrievalEventSuccess:
		case rep.RetrievalEventFailure:
		default:
			if err := retriever.eventRecorder.RecordProgress(
				retrieval.retrievalId,
				state.PayloadCid,
				state.StorageProviderID,
				event.Code,
			); err != nil {
				log.Errorf("Failed to post event to recorder:", err)
			}
		}
	}
}

func (retriever *Retriever) RetrievalSubscriberId() interface{} {
	return "autoretrieve"
}
