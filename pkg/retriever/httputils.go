package retriever

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	ErrHttpSelectorRequest = errors.New("HTTP retrieval for an explicit selector request")
	ErrNoHttpForPeer       = errors.New("no HTTP url for peer")
	ErrBadPathForRequest   = errors.New("bad path for request")
	//ErrHttpConstruction    = errors.New("failed to construct a HTTP request")
)

/*
type HTTPRetriever struct {
	Client *http.Client
	Clock  clock.Clock
}

func NewHTTPRetriever(getStorageProviderTimeout GetStorageProviderTimeout, client *http.Client) *HTTPRetriever {
	return &HTTPRetriever{
		Client: client,
		Clock:  clock.New(),
	}
}

type httpRetrieval struct {
	*HTTPRetriever
	ctx            context.Context
	request        types.RetrievalRequest
	resultChan     chan retrievalResult
	finishChan     chan struct{}
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
		ctx:            ctx,
		request:        retrievalRequest,
		resultChan:     make(chan retrievalResult),
		finishChan:     make(chan struct{}),
		eventsCallback: eventsCallback,
	}
}

func (retrieval *httpRetrieval) RetrieveFromAsyncCandidates(asyncCandidates types.InboundAsyncCandidates) (*types.RetrievalStats, error) {
	if retrieval.request.Selector != nil { // explicit selector, can't handle these here
		return nil, ErrHttpSelectorRequest
	}

	ctx, cancelCtx := context.WithCancel(retrieval.ctx)

	// start retrievals
	phaseStartTime := retrieval.Clock.Now()
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
					runHttpRetrievalCandidate(
						ctx,
						retrieval.HTTPRetriever,
						retrieval.request,
						nil, // TODO: nope: retrieval.HTTPRetriever.Client,
						retrieval.request.LinkSystem,
						retrieval, //TODO:Nope
						phaseStartTime,
						localCandidate,
					)
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

	stats, err := retrieval.collectHTTPResults(ctx)
	fmt.Println("cancelling context")
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
		case result, ok := <-r.resultChan:
			// have we got all responses but no success?
			if !ok {
				// we failed, and got only retrieval errors
				fmt.Println("closed resultChan")
				retrievalErrors = multierr.Append(retrievalErrors, ErrAllRetrievalsFailed)
				return nil, retrievalErrors
			}

			fmt.Println("result.Event", result.Event)
			if result.Event != nil {
				r.eventsCallback(*result.Event)
				break
			}
			if result.Err != nil {
				fmt.Println("result.Err", result.Err)
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

// runHttpRetrievalCandidate is a singular CID:SP retrieval, expected to be run in a goroutine
// and coordinate with other candidate retrievals to block after query phase and
// only attempt one retrieval-proper at a time.
func runHttpRetrievalCandidate(
	ctx context.Context,
	cfg *HTTPRetriever,
	retrievalRequest types.RetrievalRequest,
	client GraphsyncClient,
	linkSystem ipld.LinkSystem,
	retrieval *httpRetrieval,
	phaseStartTime time.Time,
	candidate types.RetrievalCandidate,
) {
	var stats *types.RetrievalStats
	var retrievalErr error

	retrieval.sendEvent(events.Started(cfg.Clock.Now(), retrievalRequest.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate))

	req, err := makeRequest(ctx, retrievalRequest, candidate)
	if err != nil {
		retrievalErr = err
		retrieval.sendEvent(events.Failed(cfg.Clock.Now(), retrievalRequest.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, retrievalErr.Error()))
	} else {
		resp, err := cfg.Client.Do(req)
		if err != nil {
			if ctx.Err() == nil { // not cancelled, maybe timed out though
				log.Warnf("Failed to connect to http %s: %v", candidate.MinerPeer.ID, err)
				retrievalErr = fmt.Errorf("%w: %v", ErrConnectFailed, err)
				retrieval.sendEvent(events.Failed(cfg.Clock.Now(), retrievalRequest.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, retrievalErr.Error()))
			}
		} else {
			retrieval.sendEvent(events.Connected(cfg.Clock.Now(), retrievalRequest.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate))

			// TODO: move this properly to fb
			if retrieval.canSendResult() { // move on to retrieval phase
				retrieval.sendEvent(events.FirstByte(cfg.Clock.Now(), retrievalRequest.RetrievalID, phaseStartTime, candidate))
			}

			defer resp.Body.Close()

			if _, err := readBody(candidate.RootCid, candidate.MinerPeer.ID, resp.Body, retrievalRequest.LinkSystem.StorageWriteOpener); err != nil {
				retrievalErr = err
				retrieval.sendEvent(events.Failed(cfg.Clock.Now(), retrievalRequest.RetrievalID, phaseStartTime, types.RetrievalPhase, candidate, retrievalErr.Error()))
			} else {
				retrieval.sendEvent(events.Success(
					cfg.Clock.Now(),
					retrievalRequest.RetrievalID,
					phaseStartTime,
					candidate,
					stats.Size,
					stats.Blocks,
					stats.Duration,
					stats.TotalPayment,
					0,
				))
			}
		}
	}

	fmt.Println("retrievalErr", retrievalErr, "stats", stats, "ctx.Err()", ctx.Err())
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
	} else {
		fmt.Println("can't send result")
	} // else nothing to do, we were cancelled
}
*/

func makeRequest(ctx context.Context, request types.RetrievalRequest, candidate types.RetrievalCandidate) (*http.Request, error) {
	candidateURL, err := candidate.ToURL()
	fmt.Println("candidateURL", candidateURL, "err", err, "candidate", candidate)
	if err != nil {
		log.Warnf("Couldn't construct a url for miner %s: %v", candidate.MinerPeer.ID, err)
		return nil, fmt.Errorf("%w: %v", ErrNoHttpForPeer, err)
	}

	path, err := request.GetUrlPath()
	if err != nil {
		log.Warnf("Couldn't construct a url path for request: %v", err)
		return nil, fmt.Errorf("%w: %v", ErrBadPathForRequest, err)
	}

	reqURL := fmt.Sprintf("%s/ipfs/%s%s", candidateURL, request.Cid, path)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		log.Warnf("Couldn't construct a http request %s: %v", candidate.MinerPeer.ID, err)
		return nil, fmt.Errorf("%w for peer %s: %v", ErrBadPathForRequest, candidate.MinerPeer.ID, err)
	}
	req.Header.Add("Accept", request.Scope.AcceptHeader())

	return req, nil
}

func readBody(rootCid cid.Cid, peerId peer.ID, body io.ReadCloser, writer linking.BlockWriteOpener) (*types.RetrievalStats, error) {
	startTime := time.Now() // TODO: consider whether this should be at connection time rather than body read time
	cr := &countingReader{Reader: body}
	cbr, err := car.NewBlockReader(cr)
	if err != nil {
		return nil, err
	}
	ttfb := time.Since(startTime)

	var blockCount uint64
	for {
		blk, err := cbr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		w, d, err := writer(ipld.LinkContext{})
		if err != nil {
			return nil, err
		}
		_, err = w.Write(blk.RawData())
		if err != nil {
			return nil, err
		}
		err = d(cidlink.Link{Cid: blk.Cid()})
		if err != nil {
			return nil, err
		}
		blockCount++
	}

	duration := time.Since(startTime)
	speed := uint64(float64(cr.total) / duration.Seconds())

	return &types.RetrievalStats{
		RootCid:           rootCid,
		StorageProviderId: peerId,
		Size:              cr.total,
		Blocks:            blockCount,
		Duration:          duration,
		AverageSpeed:      speed,
		TotalPayment:      big.Zero(),
		NumPayments:       0,
		AskPrice:          big.Zero(),
		TimeToFirstByte:   ttfb,
	}, nil
}

type countingReader struct {
	io.Reader
	total uint64
}

func (cr *countingReader) Read(p []byte) (n int, err error) {
	n, err = cr.Reader.Read(p)
	cr.total += uint64(n)
	return
}

/*
// sendResult will only send a result to the parent goroutine if a retrieval has
// finished (likely by a success), otherwise it will send the result
func (retrieval *httpRetrieval) sendResult(result retrievalResult) bool {
	select {
	case <-retrieval.finishChan:
		return false
	case retrieval.resultChan <- result:
		if result.Stats != nil {
			close(retrieval.finishChan)
		}
	}
	return true
}

func (retrieval *httpRetrieval) sendEvent(event types.RetrievalEvent) {
	retrieval.sendResult(retrievalResult{PeerID: event.StorageProviderId(), Event: &event})
}

// canSendResult will indicate whether a result is likely to be accepted (true)
// or whether the retrieval is already finished (likely by a success)
func (retrieval *httpRetrieval) canSendResult() bool {
	select {
	case <-retrieval.finishChan:
		return false
	default:
	}
	return true
}
*/
