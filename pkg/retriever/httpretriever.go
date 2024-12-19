package retriever

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/build"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	trustlesshttp "github.com/ipld/go-trustless-utils/http"
	"github.com/ipld/go-trustless-utils/traversal"
	"github.com/ipni/go-libipni/metadata"
	"github.com/multiformats/go-multicodec"
)

var (
	ErrHttpSelectorRequest = errors.New("HTTP retrieval for an explicit selector request")
	ErrNoHttpForPeer       = errors.New("no HTTP url for peer")
	ErrBadPathForRequest   = errors.New("bad path for request")
)

type ErrHttpRequestFailure struct {
	Code int
}

func (e ErrHttpRequestFailure) Error() string {
	return fmt.Sprintf("HTTP request failed, remote response code: %d", e.Code)
}

// Connect() is currently a noop, so this simply allows parallel goroutines to
// queue and the scoring logic to select one to start.
const HttpDefaultInitialWait time.Duration = 2 * time.Millisecond

var _ TransportProtocol = &ProtocolHttp{}

type ProtocolHttp struct {
	Client *http.Client
	Clock  clock.Clock
}

// NewHttpRetriever makes a new CandidateRetriever for verified CAR HTTP
// retrievals (transport-ipfs-gateway-http).
func NewHttpRetriever(session Session, client *http.Client) types.CandidateRetriever {
	return NewHttpRetrieverWithDeps(session, client, clock.New(), nil, HttpDefaultInitialWait, false)
}

func NewHttpRetrieverWithDeps(
	session Session,
	client *http.Client,
	clock clock.Clock,
	awaitReceivedCandidates chan<- struct{},
	initialPause time.Duration,
	noDirtyClose bool,
) types.CandidateRetriever {
	return &parallelPeerRetriever{
		Protocol: &ProtocolHttp{
			Client: client,
			Clock:  clock,
		},
		Session:                 session,
		Clock:                   clock,
		QueueInitialPause:       initialPause,
		awaitReceivedCandidates: awaitReceivedCandidates,
		noDirtyClose:            noDirtyClose,
	}
}

func (ph ProtocolHttp) Code() multicodec.Code {
	return multicodec.TransportIpfsGatewayHttp
}

func (ph ProtocolHttp) GetMergedMetadata(cid cid.Cid, currentMetadata, newMetadata metadata.Protocol) metadata.Protocol {
	return &metadata.IpfsGatewayHttp{}
}

func (ph *ProtocolHttp) Connect(ctx context.Context, retrieval *retrieval, candidate types.RetrievalCandidate) (time.Duration, error) {
	// We could begin the request here by moving ph.beginRequest() to this function.
	// That would result in parallel connections to candidates as they are received,
	// then serial reading of bodies.
	// If/when we need to share connection state between a Connect() and Retrieve()
	// call, we'll need a shared state that we can pass - either return a Context
	// here that we pick up in Retrieve, or have something on `retrieval` that can
	// be keyed by `candidate` to do this; or similar. ProtocolHttp is not
	// per-connection, it's per-protocol, and `retrieval` is not per-candidate
	// either, it's per-retrieval.
	return 0, nil
}

func (ph *ProtocolHttp) Retrieve(
	ctx context.Context,
	retrieval *retrieval,
	shared *retrievalShared,
	timeout time.Duration,
	candidate types.RetrievalCandidate,
) (*types.RetrievalStats, error) {
	// Connect and read body in one flow, we can move ph.beginRequest() to Connect()
	// to parallelise connections if we have confidence in not wasting server time
	// by requesting but not reading bodies (or delayed reading which may result in
	// timeouts).

	retrievalStart := ph.Clock.Now()

	resp, err := ph.beginRequest(ctx, retrieval.request, candidate)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, ErrHttpRequestFailure{Code: resp.StatusCode}
	}

	var expectDuplicates = trustlesshttp.DefaultIncludeDupes
	if contentType, valid := trustlesshttp.ParseContentType(resp.Header.Get("Content-Type")); valid {
		expectDuplicates = contentType.Duplicates
	} // else be permissive and just expect duplicates (DefaultIncludeDupes)

	var ttfb time.Duration
	rdr := newTimeToFirstByteReader(resp.Body, func() {
		ttfb = retrieval.Clock.Since(retrievalStart)
		shared.sendEvent(ctx, events.FirstByte(retrieval.Clock.Now(), retrieval.request.RetrievalID, candidate, ttfb, multicodec.TransportIpfsGatewayHttp))
	})
	cfg := traversal.Config{
		Root:               retrieval.request.Root,
		Selector:           retrieval.request.GetSelector(),
		ExpectDuplicatesIn: expectDuplicates,
		// write out the same as we get in  so we're not causing waste here,
		// dealing with the actual output duplicates requirements can be done
		// in a parent
		WriteDuplicatesOut: expectDuplicates,
		MaxBlocks:          retrieval.request.MaxBlocks,
		OnBlockIn: func(read uint64) {
			shared.sendEvent(ctx, events.BlockReceived(retrieval.Clock.Now(), retrieval.request.RetrievalID, candidate, multicodec.TransportIpfsGatewayHttp, read))
		},
	}

	traversalResult, err := cfg.VerifyCar(ctx, rdr, retrieval.request.LinkSystem)
	if err != nil {
		return nil, err
	}

	duration := retrieval.Clock.Since(retrievalStart)
	speed := uint64(float64(traversalResult.BytesIn) / duration.Seconds())

	return &types.RetrievalStats{
		RootCid:           candidate.RootCid,
		StorageProviderId: candidate.MinerPeer.ID,
		Size:              traversalResult.BytesIn,
		Blocks:            traversalResult.BlocksIn,
		Duration:          duration,
		AverageSpeed:      speed,
		TotalPayment:      big.Zero(),
		NumPayments:       0,
		AskPrice:          big.Zero(),
		TimeToFirstByte:   ttfb,
	}, nil
}

func (ph *ProtocolHttp) beginRequest(ctx context.Context, request types.RetrievalRequest, candidate types.RetrievalCandidate) (resp *http.Response, err error) {
	var req *http.Request
	req, err = makeRequest(ctx, request, candidate)
	logger.Debugf("HTTP request: %s", req.URL.String())
	if err == nil {
		resp, err = ph.Client.Do(req)
	}
	return resp, err
}

func makeRequest(ctx context.Context, request types.RetrievalRequest, candidate types.RetrievalCandidate) (*http.Request, error) {
	candidateURL, err := candidate.ToURL()
	if err != nil {
		logger.Warnf("Couldn't construct a url for miner %s: %v", candidate.MinerPeer.ID, err)
		return nil, fmt.Errorf("%w: %v", ErrNoHttpForPeer, err)
	}

	path, err := request.Request.UrlPath()
	if err != nil {
		logger.Warnf("Couldn't construct a url path for request: %v", err)
		return nil, fmt.Errorf("%w: %v", ErrBadPathForRequest, err)
	}

	reqURL := fmt.Sprintf("%s/ipfs/%s%s", candidateURL, request.Root, path)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		logger.Warnf("Couldn't construct a http request %s: %v", candidate.MinerPeer.ID, err)
		return nil, fmt.Errorf("%w for peer %s: %v", ErrBadPathForRequest, candidate.MinerPeer.ID, err)
	}
	req.Header.Add("Accept", trustlesshttp.DefaultContentType().String()) // prefer duplicates
	req.Header.Add("X-Request-Id", request.RetrievalID.String())
	req.Header.Add("User-Agent", build.UserAgent)

	return req, nil
}

var _ io.Reader = (*timeToFirstByteReader)(nil)

type timeToFirstByteReader struct {
	r     io.Reader
	first bool
	cb    func()
}

func newTimeToFirstByteReader(r io.Reader, cb func()) *timeToFirstByteReader {
	return &timeToFirstByteReader{
		r:  r,
		cb: cb,
	}
}

func (t *timeToFirstByteReader) Read(p []byte) (int, error) {
	if !t.first {
		t.first = true
		defer t.cb()
	}
	return t.r.Read(p)
}
