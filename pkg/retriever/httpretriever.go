package retriever

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/maurl"
	"go.uber.org/multierr"
)

type HTTPRetriever struct {
	Client http.Client
	Clock  clock.Clock
}

func NewHTTPRetriever(getStorageProviderTimeout GetStorageProviderTimeout, client http.Client) *HTTPRetriever {
	return &HTTPRetriever{
		Client: client,
	}
}

type httpRetrieval struct {
	*HTTPRetriever
	ctx            context.Context
	request        types.RetrievalRequest
	eventChan      chan retrievalResult
	eventsCallback func(types.RetrievalEvent)
}

func (cfg *HTTPRetriever) Retrieve(
	ctx context.Context,
	retrievalRequest types.RetrievalRequest,
	eventsCallback func(types.RetrievalEvent),
) types.CandidateRetrieval {

	if cfg == nil {
		cfg = &HTTPRetriever{}
	}
	if eventsCallback == nil {
		eventsCallback = func(re types.RetrievalEvent) {}
	}

	// state local to this CID's retrieval
	return &httpRetrieval{
		HTTPRetriever:  cfg,
		ctx:            ctx,
		request:        retrievalRequest,
		eventChan:      make(chan retrievalResult),
		eventsCallback: eventsCallback,
	}
}

func (r *httpRetrieval) RetrieveFromAsyncCandidates(asyncCandidates types.InboundAsyncCandidates) (*types.RetrievalStats, error) {
	ctx, cancelCtx := context.WithCancel(r.ctx)

	// start retrievals
	phaseStartTime := r.Clock.Now()
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		for {
			hasCandidates, candidates, err := asyncCandidates.Next(ctx)
			if !hasCandidates || err != nil {
				return
			}
			for _, candidate := range candidates {
				// Start the retrieval with the candidate
				waitGroup.Add(1)
				localCandidate := candidate
				go func() {
					defer waitGroup.Done()
					r.doHTTPDownload(ctx, phaseStartTime, localCandidate)
				}()
			}
		}
	}()

	finishAll := make(chan struct{}, 1)
	go func() {
		waitGroup.Wait()
		close(r.eventChan)
		finishAll <- struct{}{}
	}()

	stats, err := r.collectHTTPResults(ctx)
	cancelCtx()
	// optimistically try to wait for all routines to finish
	select {
	case <-finishAll:
	case <-time.After(100 * time.Millisecond):
		log.Warn("Unable to successfully cancel all retrieval attempts withing 100ms")
	}
	return stats, err
}

func (r *httpRetrieval) collectHTTPResults(ctx context.Context) (*types.RetrievalStats, error) {
	var retrievalErrors error
	for {
		select {
		case result, ok := <-r.eventChan:
			// have we got all responses but no success?
			if !ok {
				// we failed, and got only retrieval errors
				retrievalErrors = multierr.Append(retrievalErrors, ErrAllRetrievalsFailed)
				return nil, retrievalErrors
			}

			if result.Event != nil {
				r.eventsCallback(*result.Event)
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

func (r *httpRetrieval) doHTTPDownload(
	ctx context.Context,
	phaseStartTime time.Time,
	candidate types.RetrievalCandidate,
) {
	var stats *types.RetrievalStats
	var retrievalErr error

	r.sendEvent(events.Started(r.HTTPRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate))

	var candidateURL *url.URL
	var err error
	for _, addr := range candidate.MinerPeer.Addrs {
		candidateURL, err = maurl.ToURL(addr)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Warnf("Couldn't construct a url for miner %s: %v", candidate.MinerPeer.ID, err)
		retrievalErr = fmt.Errorf("%w: %v", ErrConnectFailed, err)
		r.sendEvent(events.Failed(r.HTTPRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, retrievalErr.Error()))
		return
	}

	reqURL := fmt.Sprintf("%s/ipfs/%s%s", candidateURL, r.request.Cid, r.request.Path)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		log.Warnf("Couldn't construct a http request %s: %v", candidate.MinerPeer.ID, err)
		retrievalErr = fmt.Errorf("%w: %v", ErrConnectFailed, err)
		r.sendEvent(events.Failed(r.HTTPRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, retrievalErr.Error()))
		return
	}
	if r.request.Scope == types.CarScopeBlock {
		req.Header.Add("Accept", "application/vnd.ipld.block")
	} else {
		req.Header.Add("Accept", "application/vnd.ipld.car")
	}

	resp, err := r.Client.Do(req)

	if err != nil {
		if ctx.Err() == nil { // not cancelled, maybe timed out though
			log.Warnf("Failed to connect to http %s: %v", candidate.MinerPeer.ID, err)
			retrievalErr = fmt.Errorf("%w: %v", ErrConnectFailed, err)
			r.sendEvent(events.Failed(r.HTTPRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, retrievalErr.Error()))
		}
	} else {
		r.sendEvent(events.Connected(r.HTTPRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate))

		if r.canSendResult() { // move on to retrieval phase
			r.sendEvent(events.FirstByte(r.HTTPRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, candidate))
		}
	}
	defer resp.Body.Close()

	cbr, err := car.NewBlockReader(resp.Body)
	if err != nil {
		r.sendEvent(events.Failed(r.HTTPRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, retrievalErr.Error()))
		return
	}

	for {
		blk, err := cbr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			r.sendEvent(events.Failed(r.HTTPRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, retrievalErr.Error()))
			return
		}
		w, d, err := r.request.LinkSystem.StorageWriteOpener(ipld.LinkContext{})
		if err != nil {
			r.sendEvent(events.Failed(r.HTTPRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, retrievalErr.Error()))
			return
		}
		_, err = w.Write(blk.RawData())
		if err != nil {
			r.sendEvent(events.Failed(r.HTTPRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, retrievalErr.Error()))
			return
		}
		err = d(cidlink.Link{Cid: blk.Cid()})
		if err != nil {
			r.sendEvent(events.Failed(r.HTTPRetriever.Clock.Now(), r.request.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, retrievalErr.Error()))
			return
		}
	}

	r.sendEvent(events.Success(
		r.HTTPRetriever.Clock.Now(),
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

	if r.canSendResult() {
		r.sendResult(retrievalResult{PhaseStart: phaseStartTime, PeerID: candidate.MinerPeer.ID, Stats: stats})
	}
}

// sendResult will only send a result to the parent goroutine if a retrieval has
// finished (likely by a success), otherwise it will send the result
func (retrieval *httpRetrieval) sendResult(result retrievalResult) bool {
	select {
	case <-retrieval.ctx.Done():
		return false
	case retrieval.eventChan <- result:
	}
	return true
}

func (retrieval *httpRetrieval) sendEvent(event types.RetrievalEvent) {
	retrieval.sendResult(retrievalResult{PeerID: event.StorageProviderId(), Event: &event})
}

func (retrieval *httpRetrieval) canSendResult() bool {
	return retrieval.ctx.Err() != nil
}
