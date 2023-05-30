package retriever

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/metrics"
	"github.com/filecoin-project/lassie/pkg/retriever/combinators"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
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
	ErrConnectFailed               = errors.New("unable to connect to provider")
	ErrAllQueriesFailed            = errors.New("all queries failed")
	ErrRetrievalTimedOut           = errors.New("retrieval timed out")
	ErrRetrievalAlreadyRunning     = errors.New("retrieval already running for CID")
)

type Session interface {
	GetStorageProviderTimeout(storageProviderId peer.ID) time.Duration
	FilterIndexerCandidate(candidate types.RetrievalCandidate) (bool, types.RetrievalCandidate)

	RegisterRetrieval(retrievalId types.RetrievalID, cid cid.Cid, selector datamodel.Node) bool
	AddToRetrieval(retrievalId types.RetrievalID, storageProviderIds []peer.ID) error
	EndRetrieval(retrievalId types.RetrievalID) error

	RecordConnectTime(storageProviderId peer.ID, connectTime time.Duration)
	RecordFirstByteTime(storageProviderId peer.ID, firstByteTime time.Duration)
	RecordFailure(retrievalId types.RetrievalID, storageProviderId peer.ID) error
	RecordSuccess(storageProviderId peer.ID, bandwidthBytesPerSecond uint64)

	ChooseNextProvider(peers []peer.ID, metadata []metadata.Protocol) int
}

type Retriever struct {
	// Assumed immutable during operation
	executor     types.Retriever
	eventManager *events.EventManager
	session      Session
	clock        clock.Clock
	protocols    []multicodec.Code
}

type CandidateFinder interface {
	FindCandidates(context.Context, cid.Cid) ([]types.RetrievalCandidate, error)
	FindCandidatesAsync(context.Context, cid.Cid, func(types.RetrievalCandidate)) error
}

type eventStats struct {
	failedCount        int64
	queryCount         int64
	filteredQueryCount int64
}

func NewRetriever(
	ctx context.Context,
	session Session,
	candidateFinder CandidateFinder,
	protocolRetrievers map[multicodec.Code]types.CandidateRetriever,
) (*Retriever, error) {
	return NewRetrieverWithClock(ctx, session, candidateFinder, protocolRetrievers, clock.New())
}

func NewRetrieverWithClock(
	ctx context.Context,
	session Session,
	candidateFinder CandidateFinder,
	protocolRetrievers map[multicodec.Code]types.CandidateRetriever,
	clock clock.Clock,
) (*Retriever, error) {
	retriever := &Retriever{
		eventManager: events.NewEventManager(ctx),
		session:      session,
		clock:        clock,
	}
	retriever.protocols = []multicodec.Code{}
	for protocol := range protocolRetrievers {
		retriever.protocols = append(retriever.protocols, protocol)
	}
	retriever.executor = combinators.RetrieverWithCandidateFinder{
		CandidateFinder: NewAssignableCandidateFinderWithClock(candidateFinder, session.FilterIndexerCandidate, clock),
		CandidateRetriever: combinators.SplitRetriever[multicodec.Code]{
			AsyncCandidateSplitter: combinators.NewAsyncCandidateSplitter(retriever.protocols, NewProtocolSplitter),
			CandidateRetrievers:    protocolRetrievers,
			CoordinationKind:       types.RaceCoordination,
		},
	}

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

// Retrieve attempts to retrieve the given CID using the configured
// CandidateFinder to find storage providers that should have the CID.
func (retriever *Retriever) Retrieve(
	ctx context.Context,
	request types.RetrievalRequest,
	eventsCB func(types.RetrievalEvent),
) (*types.RetrievalStats, error) {
	startTime := retriever.clock.Now()

	ctx = types.RegisterRetrievalIDToContext(ctx, request.RetrievalID)
	if !retriever.eventManager.IsStarted() {
		return nil, ErrRetrieverNotStarted
	}
	if !retriever.session.RegisterRetrieval(request.RetrievalID, request.Cid, request.GetSelector()) {
		return nil, fmt.Errorf("%w: %s", ErrRetrievalAlreadyRunning, request.Cid)
	}
	defer func() {
		if err := retriever.session.EndRetrieval(request.RetrievalID); err != nil {
			logger.Errorf("failed to end retrieval tracking for %s: %s", request.Cid, err.Error())
		}
	}()

	// setup the event handler to track progress
	eventStats := &eventStats{}
	onRetrievalEvent := makeOnRetrievalEvent(ctx,
		retriever.eventManager,
		retriever.session,
		request.Cid,
		request.RetrievalID,
		eventStats,
		eventsCB,
	)

	// Emit a Started event denoting that the entire fetch phase has started
	onRetrievalEvent(events.Started(retriever.clock.Now(), request.RetrievalID, startTime, types.FetchPhase, types.RetrievalCandidate{RootCid: request.Cid}, request.GetSupportedProtocols(retriever.protocols)...))

	// retrieve, note that we could get a successful retrieval
	// (retrievalStats!=nil) _and_ also an error return because there may be
	// multiple failures along the way, if we got a retrieval then we'll pretend
	// to our caller that there was no error
	retrievalStats, err := retriever.executor.Retrieve(
		ctx,
		request,
		onRetrievalEvent,
	)

	// Emit a Finished event denoting that the entire fetch phase has finished
	onRetrievalEvent(events.Finished(retriever.clock.Now(), request.RetrievalID, startTime, types.RetrievalCandidate{RootCid: request.Cid}))

	if err != nil && retrievalStats == nil {
		return nil, err
	}

	// success
	logger.Infof(
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
	session Session,
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
			handleCandidatesFilteredEvent(retrievalId, session, retrievalCid, ret)
		case events.RetrievalEventStarted:
			handleStartedEvent(ret)
		case events.RetrievalEventFailed:
			handleFailureEvent(ctx, session, retrievalId, eventStats, ret)
		}
		eventManager.DispatchEvent(event)
		if eventsCb != nil {
			eventsCb(event)
		}
	}
}

// handleFailureEvent is called when a query _or_ retrieval fails
func handleFailureEvent(
	ctx context.Context,
	session Session,
	retrievalId types.RetrievalID,
	eventStats *eventStats,
	event events.RetrievalEventFailed,
) {
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
		logger.Warnf(
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
	session Session,
	retrievalCid cid.Cid,
	event events.RetrievalEventCandidatesFiltered,
) {
	if len(event.Candidates()) > 0 {
		stats.Record(context.Background(), metrics.RequestWithIndexerCandidatesFilteredCount.M(1))
		ids := make([]peer.ID, 0)
		for _, c := range event.Candidates() {
			ids = append(ids, c.MinerPeer.ID)
		}
		if err := session.AddToRetrieval(retrievalId, ids); err != nil {
			logger.Errorf("failed to add storage providers to tracked retrieval for %s: %s", retrievalCid, err.Error())
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
	case events.EventWithCandidates:
		var cands = strings.Builder{}
		for i, c := range tevent.Candidates() {
			cands.WriteString(c.MinerPeer.ID.String())
			if i < len(tevent.Candidates())-1 {
				cands.WriteString(", ")
			}
		}
		logadd("candidates", cands.String())
	case events.RetrievalEventFailed:
		logadd("errorMessage", tevent.ErrorMessage())
	case events.RetrievalEventSuccess:
		logadd("receivedSize", tevent.ReceivedSize())
	}
	logger.Debugw("retrieval-event", kv...)
}
