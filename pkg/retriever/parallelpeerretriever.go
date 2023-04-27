package retriever

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/retriever/prioritywaitqueue"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"go.uber.org/multierr"
)

type CounterCallback func(int) error
type CandidateCallback func(types.RetrievalCandidate) error
type CandidateErrorCallback func(types.RetrievalCandidate, error)

type GetStorageProviderTimeout func(peer peer.ID) time.Duration

type GraphsyncClient interface {
	Connect(ctx context.Context, peerAddr peer.AddrInfo) error
	RetrieveFromPeer(
		ctx context.Context,
		linkSystem ipld.LinkSystem,
		peerID peer.ID,
		proposal *retrievaltypes.DealProposal,
		selector ipld.Node,
		maxLinks uint64,
		eventsCallback datatransfer.Subscriber,
		gracefulShutdownRequested <-chan struct{},
	) (*types.RetrievalStats, error)
}

type TransportProtocol multicodec.Code

const (
	ProtocolGraphsync = TransportProtocol(multicodec.TransportGraphsyncFilecoinv1)
	ProtocolHttp      = TransportProtocol(multicodec.TransportIpfsGatewayHttp)
)

type parallelPeerRetriever struct {
	Protocol                  TransportProtocol
	GetStorageProviderTimeout GetStorageProviderTimeout
	Clock                     clock.Clock
	QueueInitialPause         time.Duration
	GraphsyncClient           GraphsyncClient
	HttpClient                *http.Client
}

func NewGraphsyncRetriever(getStorageProviderTimeout GetStorageProviderTimeout, client GraphsyncClient) types.CandidateRetriever {
	return &parallelPeerRetriever{
		Protocol:                  ProtocolGraphsync,
		GetStorageProviderTimeout: getStorageProviderTimeout,
		Clock:                     clock.New(),
		QueueInitialPause:         2 * time.Millisecond,
		GraphsyncClient:           client,
	}
}

func NewHttpRetriever(getStorageProviderTimeout GetStorageProviderTimeout, client *http.Client) types.CandidateRetriever {
	return &parallelPeerRetriever{
		Protocol:                  ProtocolGraphsync,
		GetStorageProviderTimeout: getStorageProviderTimeout,
		Clock:                     clock.New(),
		QueueInitialPause:         2 * time.Millisecond,
		HttpClient:                client,
	}
}

// graphsyncMetadataCompare compares two metadata.GraphsyncFilecoinV1s and
// returns true if the first is preferable to the second.
func graphsyncMetadataCompare(a, b *metadata.GraphsyncFilecoinV1, defaultValue bool) bool {
	// prioritize verified deals over not verified deals
	if a.VerifiedDeal != b.VerifiedDeal {
		return a.VerifiedDeal
	}

	// prioritize fast retrievel over not fast retrieval
	if a.FastRetrieval != b.FastRetrieval {
		return a.FastRetrieval
	}

	return defaultValue
}

// retrieval handles state on a per-retrieval (across multiple candidates) basis
type retrieval struct {
	*parallelPeerRetriever
	ctx                context.Context
	request            types.RetrievalRequest
	eventsCallback     func(types.RetrievalEvent)
	candidateMetadata  map[peer.ID]metadata.Protocol
	candidateMetdataLk sync.RWMutex
}

type connectCandidate struct {
	PeerID   peer.ID
	Duration time.Duration
}

type retrievalResult struct {
	PeerID     peer.ID
	PhaseStart time.Time
	Stats      *types.RetrievalStats
	Event      *types.RetrievalEvent
	Err        error
}

type candidateRetrieval struct {
	waitQueue  prioritywaitqueue.PriorityWaitQueue[connectCandidate]
	resultChan chan retrievalResult
	finishChan chan struct{}
}

// RetrieveFromCandidates performs a retrieval for a given CID by querying the indexer, then
// attempting to query all candidates and attempting to perform a full retrieval
// from the best and fastest storage provider as the queries are received.
func (cfg *parallelPeerRetriever) Retrieve(
	ctx context.Context,
	retrievalRequest types.RetrievalRequest,
	eventsCallback func(types.RetrievalEvent),
) types.CandidateRetrieval {

	if cfg == nil {
		cfg = &parallelPeerRetriever{}
	}
	if eventsCallback == nil {
		eventsCallback = func(re types.RetrievalEvent) {}
	}

	// state local to this CID's retrieval
	return &retrieval{
		parallelPeerRetriever: cfg,
		ctx:                   ctx,
		request:               retrievalRequest,
		eventsCallback:        eventsCallback,
		candidateMetadata:     make(map[peer.ID]metadata.Protocol),
	}
}

// candidateCompare compares two connectCandidates and returns true if the first is
// preferable to the second. This is used for the PriorityWaitQueue that will
// prioritise execution of retrievals if two candidates are available to compare
// at the same time.
func (r *retrieval) candidateCompare(a, b connectCandidate) bool {
	r.candidateMetdataLk.RLock()
	defer r.candidateMetdataLk.RUnlock()

	mdA, ok := r.candidateMetadata[a.PeerID]
	if !ok {
		return false
	}

	mdB, ok := r.candidateMetadata[b.PeerID]
	if !ok {
		return true
	}

	return graphsyncMetadataCompare(mdA.(*metadata.GraphsyncFilecoinV1), mdB.(*metadata.GraphsyncFilecoinV1), a.Duration < b.Duration)
}

func (r *retrieval) RetrieveFromAsyncCandidates(asyncCandidates types.InboundAsyncCandidates) (*types.RetrievalStats, error) {
	ctx, cancelCtx := context.WithCancel(r.ctx)

	retrieval := &candidateRetrieval{
		resultChan: make(chan retrievalResult),
		finishChan: make(chan struct{}),
		waitQueue: prioritywaitqueue.New(
			r.candidateCompare,
			prioritywaitqueue.WithInitialPause[connectCandidate](r.QueueInitialPause),
			prioritywaitqueue.WithClock[connectCandidate](r.Clock),
		),
	}

	// start retrievals
	phaseStartTime := r.Clock.Now()
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		for {
			active, candidates, err := r.filterCandidates(ctx, asyncCandidates)
			if !active || err != nil {
				return
			}
			for _, candidate := range candidates {
				// Start the retrieval with the candidate
				candidate := candidate
				waitGroup.Add(1)
				go func() {
					defer waitGroup.Done()
					r.runRetrievalCandidate(ctx, retrieval, phaseStartTime, candidate)
				}()
			}
		}
	}()

	finishAll := make(chan struct{}, 1)
	go func() {
		waitGroup.Wait()
		close(retrieval.resultChan)
		finishAll <- struct{}{}
	}()

	stats, err := collectResults(ctx, retrieval, r.eventsCallback)
	cancelCtx()
	// optimistically try to wait for all routines to finish
	select {
	case <-finishAll:
	case <-time.After(100 * time.Millisecond):
		log.Warn("Unable to successfully cancel all retrieval attempts withing 100ms")
	}
	return stats, err
}

func (r *retrieval) filterCandidates(ctx context.Context, asyncCandidates types.InboundAsyncCandidates) (bool, []types.RetrievalCandidate, error) {
	filtered := make([]types.RetrievalCandidate, 0)
	active, candidates, err := asyncCandidates.Next(ctx)
	if !active || err != nil {
		return false, nil, err
	}

	for _, candidate := range candidates {
		// Grab the current candidate's metadata, adding the piece cid to the metadata if the type assertion failed
		candidateMetadata := candidate.Metadata.Get(multicodec.Code(r.Protocol))
		var hasMetadata bool
		switch r.Protocol {
		case ProtocolGraphsync:
			_, hasMetadata = candidateMetadata.(*metadata.GraphsyncFilecoinV1)
			if !hasMetadata {
				candidateMetadata = &metadata.GraphsyncFilecoinV1{PieceCID: r.request.Cid}
			}
		case ProtocolHttp:
			_, ok := candidateMetadata.(*metadata.IpfsGatewayHttp)
			if !ok {
				candidateMetadata = &metadata.IpfsGatewayHttp{}
			}
		default:
			panic(fmt.Sprintf("unexpected protocol: %v", r.Protocol))
		}

		// Check if we already started a retrieval for this candidate
		r.candidateMetdataLk.RLock()
		currMetadata, seenCandidate := r.candidateMetadata[candidate.MinerPeer.ID]
		r.candidateMetdataLk.RUnlock()

		// Don't start a new retrieval if we've seen this candidate before,
		// but update the metadata if it's more favorable
		if r.Protocol == ProtocolGraphsync && seenCandidate {
			// We know the metadata is not as favorable if the type assertion failed
			// since the metadata will be the zero value of graphsync metadata
			if !hasMetadata {
				continue
			}

			if graphsyncMetadataCompare(candidateMetadata.(*metadata.GraphsyncFilecoinV1), currMetadata.(*metadata.GraphsyncFilecoinV1), false) {
				r.candidateMetdataLk.Lock()
				r.candidateMetadata[candidate.MinerPeer.ID] = candidateMetadata
				r.candidateMetdataLk.Unlock()
			}
			continue
		}

		// Track the candidate metadata
		r.candidateMetdataLk.Lock()
		r.candidateMetadata[candidate.MinerPeer.ID] = candidateMetadata
		r.candidateMetdataLk.Unlock()

		filtered = append(filtered, candidate)
	}

	return true, filtered, nil
}

// collectResults is responsible for receiving query errors, retrieval errors
// and retrieval results and aggregating into an appropriate return of either
// a complete RetrievalStats or an bundled multi-error
func collectResults(ctx context.Context, retrieval *candidateRetrieval, eventsCallback func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
	var retrievalErrors error
	for {
		select {
		case result, ok := <-retrieval.resultChan:
			// have we got all responses but no success?
			if !ok {
				// we failed, and got only retrieval errors
				retrievalErrors = multierr.Append(retrievalErrors, ErrAllRetrievalsFailed)
				return nil, retrievalErrors
			}

			if result.Event != nil {
				eventsCallback(*result.Event)
				break
			}
			if result.Err != nil {
				retrievalErrors = multierr.Append(retrievalErrors, result.Err)
			}
			if result.Stats != nil {
				return result.Stats, nil
			}
		case <-ctx.Done():
			return nil, context.Canceled
		}
	}
}

// runRetrievalCandidate is a singular CID:SP retrieval, expected to be run in a goroutine
// and coordinate with other candidate retrievals to block after query phase and
// only attempt one retrieval-proper at a time.
func (r *retrieval) runRetrievalCandidate(
	ctx context.Context,
	retrieval *candidateRetrieval,
	phaseStartTime time.Time,
	candidate types.RetrievalCandidate,
) {

	var timeout time.Duration
	if r.parallelPeerRetriever.GetStorageProviderTimeout != nil {
		timeout = r.parallelPeerRetriever.GetStorageProviderTimeout(candidate.MinerPeer.ID)
	}

	var stats *types.RetrievalStats
	var retrievalErr error
	var done func()

	retrieval.sendEvent(events.Started(r.parallelPeerRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate))
	connectCtx := ctx
	if timeout != 0 {
		var timeoutFunc func()
		connectCtx, timeoutFunc = r.parallelPeerRetriever.Clock.WithDeadline(ctx, r.parallelPeerRetriever.Clock.Now().Add(timeout))
		defer timeoutFunc()
	}

	// Setup
	var err error
	var httpResp *http.Response
	switch r.Protocol {
	case ProtocolGraphsync:
		err = r.GraphsyncClient.Connect(connectCtx, candidate.MinerPeer)
	case ProtocolHttp:
		// TODO: consider whether we want to be starting the requests in parallel here
		// and just deferring body read for the serial portion below, we may be causing
		// upstream providers to do more work than necessary if we give up on them
		// because we get a complete response from another candidate.
		var httpReq *http.Request
		httpReq, err = makeRequest(ctx, r.request, candidate)
		if err == nil {
			httpResp, err = r.HttpClient.Do(httpReq)
		}
	default:
		panic(fmt.Sprintf("unexpected protocol: %v", r.Protocol))
	}
	if err != nil {
		if ctx.Err() == nil { // not cancelled, maybe timed out though
			log.Warnf("Failed to connect to SP %s: %v", candidate.MinerPeer.ID, err)
			retrievalErr = fmt.Errorf("%w: %v", ErrConnectFailed, err)
			retrieval.sendEvent(events.Failed(r.parallelPeerRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, retrievalErr.Error()))
		}
	} else {
		retrieval.sendEvent(events.Connected(r.parallelPeerRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate))
		// if query is successful, then wait for priority and execute retrieval
		done = retrieval.waitQueue.Wait(connectCandidate{
			PeerID:   candidate.MinerPeer.ID,
			Duration: r.parallelPeerRetriever.Clock.Now().Sub(phaseStartTime),
		})

		if retrieval.canSendResult() { // move on to retrieval phase
			switch r.Protocol {
			case ProtocolGraphsync:
				eventsCallback := retrieval.makeEventsCallback(r.parallelPeerRetriever.Clock, r.request.RetrievalID, phaseStartTime, candidate)
				stats, retrievalErr = r.retrievalPhase(
					ctx,
					timeout,
					candidate,
					eventsCallback,
				)
			case ProtocolHttp:
				defer httpResp.Body.Close()
				stats, retrievalErr = readBody(candidate.RootCid, candidate.MinerPeer.ID, httpResp.Body, r.request.LinkSystem.StorageWriteOpener)
			default:
				panic(fmt.Sprintf("unexpected protocol: %v", r.Protocol))
			}

			if retrievalErr != nil {
				msg := retrievalErr.Error()
				if errors.Is(retrievalErr, ErrRetrievalTimedOut) {
					msg = fmt.Sprintf("timeout after %s", timeout)
				}
				retrieval.sendEvent(events.Failed(r.parallelPeerRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, msg))
			} else {
				retrieval.sendEvent(events.Success(
					r.parallelPeerRetriever.Clock.Now(),
					r.request.RetrievalID,
					phaseStartTime,
					candidate,
					stats.Size,
					stats.Blocks,
					stats.Duration,
					stats.TotalPayment,
					0,
				),
				)
			}
		} // else we didn't get to retrieval phase because we were cancelled
	}

	if retrieval.canSendResult() {
		if retrievalErr != nil {
			if ctx.Err() != nil { // cancelled, don't report the error
				retrieval.sendResult(retrievalResult{PhaseStart: phaseStartTime, PeerID: candidate.MinerPeer.ID})
			} else {
				// an error of some kind to report
				retrieval.sendResult(retrievalResult{PhaseStart: phaseStartTime, PeerID: candidate.MinerPeer.ID, Err: retrievalErr})
			}
		} else { // success, we have stats and no errors
			retrieval.sendResult(retrievalResult{PhaseStart: phaseStartTime, PeerID: candidate.MinerPeer.ID, Stats: stats})
		}
	} // else nothing to do, we were cancelled

	if done != nil {
		done() // allow prioritywaitqueue to move on to next candidate
	}
}

func (retrieval *candidateRetrieval) makeEventsCallback(
	clock clock.Clock,
	retrievalId types.RetrievalID,
	phaseStartTime time.Time,
	candidate types.RetrievalCandidate) datatransfer.Subscriber {

	var receivedFirstByte bool
	return func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		switch event.Code {
		case datatransfer.Open:
			retrieval.sendEvent(events.Proposed(clock.Now(), retrievalId, phaseStartTime, candidate))
		case datatransfer.NewVoucherResult:
			lastVoucher := channelState.LastVoucherResult()
			resType, err := retrievaltypes.DealResponseFromNode(lastVoucher.Voucher)
			if err != nil {
				return
			}
			if resType.Status == retrievaltypes.DealStatusAccepted {
				retrieval.sendEvent(events.Accepted(clock.Now(), retrievalId, phaseStartTime, candidate))
			}
		case datatransfer.DataReceivedProgress:
			if !receivedFirstByte {
				receivedFirstByte = true
				retrieval.sendEvent(events.FirstByte(clock.Now(), retrievalId, phaseStartTime, candidate))
			}
		}
	}
}

// canSendResult will indicate whether a result is likely to be accepted (true)
// or whether the retrieval is already finished (likely by a success)
func (retrieval *candidateRetrieval) canSendResult() bool {
	select {
	case <-retrieval.finishChan:
		return false
	default:
	}
	return true
}

// sendResult will only send a result to the parent goroutine if a retrieval has
// finished (likely by a success), otherwise it will send the result
func (retrieval *candidateRetrieval) sendResult(result retrievalResult) bool {
	select {
	case <-retrieval.finishChan:
		return false
	case retrieval.resultChan <- result:
		if result.Stats != nil {
			// signals to goroutines to bail, this has to be done here, rather than on
			// the receiving parent end, because immediately after this call we instruct
			// the prioritywaitqueue that we're done and another may start
			close(retrieval.finishChan)
		}
	}
	return true
}

func (retrieval *candidateRetrieval) sendEvent(event types.RetrievalEvent) {
	retrieval.sendResult(retrievalResult{PeerID: event.StorageProviderId(), Event: &event})
}

func (r *retrieval) retrievalPhase(
	ctx context.Context,
	timeout time.Duration,
	candidate types.RetrievalCandidate,
	eventsCallback datatransfer.Subscriber,
) (*types.RetrievalStats, error) {

	ss := "*"
	selector := r.request.GetSelector()
	if !ipld.DeepEqual(selector, selectorparse.CommonSelector_ExploreAllRecursively) {
		byts, err := ipld.Encode(selector, dagjson.Encode)
		if err != nil {
			return nil, err
		}
		ss = string(byts)
	}

	log.Infof(
		"Attempting retrieval from SP %s for %s (with selector: [%s])",
		candidate.MinerPeer.ID,
		candidate.RootCid,
		ss,
	)

	params, err := retrievaltypes.NewParamsV1(big.Zero(), 0, 0, selector, nil, big.Zero())
	if err != nil {
		return nil, multierr.Append(multierr.Append(ErrRetrievalFailed, ErrProposalCreationFailed), err)
	}
	proposal := &retrievaltypes.DealProposal{
		PayloadCID: candidate.RootCid,
		ID:         retrievaltypes.DealID(dealIdGen.Next()),
		Params:     params,
	}

	retrieveCtx, retrieveCancel := context.WithCancel(ctx)
	defer retrieveCancel()

	var lastBytesReceived uint64
	var doneLk sync.Mutex
	var done, timedOut bool
	var lastBytesReceivedTimer, gracefulShutdownTimer *clock.Timer

	gracefulShutdownChan := make(chan struct{})

	// Start the timeout tracker only if retrieval timeout isn't 0
	if timeout != 0 {
		lastBytesReceivedTimer = r.parallelPeerRetriever.Clock.AfterFunc(timeout, func() {
			doneLk.Lock()
			done = true
			timedOut = true
			doneLk.Unlock()

			gracefulShutdownChan <- struct{}{}
			gracefulShutdownTimer = r.parallelPeerRetriever.Clock.AfterFunc(1*time.Minute, retrieveCancel)
		})
	}

	eventsSubscriber := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.DataReceivedProgress {
			if lastBytesReceivedTimer != nil {
				doneLk.Lock()
				if !done {
					if lastBytesReceived != channelState.Received() {
						lastBytesReceivedTimer.Reset(timeout)
						lastBytesReceived = channelState.Received()
					}
				}
				doneLk.Unlock()
			}
		}
		eventsCallback(event, channelState)
	}

	stats, err := r.GraphsyncClient.RetrieveFromPeer(
		retrieveCtx,
		r.request.LinkSystem,
		candidate.MinerPeer.ID,
		proposal,
		selector,
		uint64(r.request.MaxBlocks),
		eventsSubscriber,
		gracefulShutdownChan,
	)

	if timedOut {
		return nil, multierr.Append(ErrRetrievalFailed,
			fmt.Errorf(
				"%w after %s",
				ErrRetrievalTimedOut,
				timeout,
			),
		)
	}

	if lastBytesReceivedTimer != nil {
		lastBytesReceivedTimer.Stop()
	}
	if gracefulShutdownTimer != nil {
		gracefulShutdownTimer.Stop()
	}

	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRetrievalFailed, err)
	}
	return stats, nil
}
