package retriever

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/metadata"
	"github.com/multiformats/go-multicodec"
)

var (
	ErrHttpSelectorRequest = errors.New("HTTP retrieval for an explicit selector request")
	ErrNoHttpForPeer       = errors.New("no HTTP url for peer")
	ErrBadPathForRequest   = errors.New("bad path for request")
)

var _ TransportProtocol = &ProtocolHttp{}

type ProtocolHttp struct {
	Client *http.Client

	req  *http.Request
	resp *http.Response
}

// NewHttpRetriever makes a new CandidateRetriever for verified CAR HTTP
// retrievals (transport-ipfs-gateway-http).
func NewHttpRetriever(getStorageProviderTimeout GetStorageProviderTimeout, client *http.Client) types.CandidateRetriever {
	return NewHttpRetrieverWithDeps(getStorageProviderTimeout, client, clock.New(), nil)
}

func NewHttpRetrieverWithDeps(getStorageProviderTimeout GetStorageProviderTimeout, client *http.Client, clock clock.Clock, awaitReceivedCandidates chan<- struct{}) types.CandidateRetriever {
	return &parallelPeerRetriever{
		Protocol: &ProtocolHttp{
			Client: client,
		},
		GetStorageProviderTimeout: getStorageProviderTimeout,
		Clock:                     clock,
		awaitReceivedCandidates:   awaitReceivedCandidates,
	}
}

func (ph ProtocolHttp) Code() multicodec.Code {
	return multicodec.TransportIpfsGatewayHttp
}

func (ph ProtocolHttp) GetMergedMetadata(cid cid.Cid, currentMetadata, newMetadata metadata.Protocol) metadata.Protocol {
	return &metadata.IpfsGatewayHttp{}
}

func (ph ProtocolHttp) CompareCandidates(a, b connectCandidate, mda, mdb metadata.Protocol) bool {
	// we only have duration .. currently
	return a.Duration < b.Duration
}

func (ph *ProtocolHttp) Connect(ctx context.Context, retrieval *retrieval, candidate types.RetrievalCandidate) error {
	// We could begin the request here by moving ph.beginRequest() to this function.
	// That would result in parallel connections to candidates as they are received,
	// then serial reading of bodies.
	return nil
}

func (ph *ProtocolHttp) Retrieve(
	ctx context.Context,
	retrieval *retrieval,
	session *retrievalSession,
	phaseStartTime time.Time,
	timeout time.Duration,
	candidate types.RetrievalCandidate,
) (*types.RetrievalStats, error) {

	// Connect and read body in one flow, we can move ph.beginRequest() to Connect()
	// to parallelise connections if we have confidence in not wasting server time
	// by requesting but not reading bodies (or delayed reading which may result in
	// timeouts).
	if err := ph.beginRequest(ctx, retrieval.request, candidate); err != nil {
		return nil, err
	}

	defer ph.resp.Body.Close()

	var blockBytes uint64
	cbr, err := car.NewBlockReader(ph.resp.Body)
	if err != nil {
		return nil, err
	}
	ttfb := retrieval.Clock.Since(phaseStartTime)
	session.sendEvent(events.FirstByte(retrieval.Clock.Now(), retrieval.request.RetrievalID, phaseStartTime, candidate))

	var blockCount uint64
	for {
		blk, err := cbr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		w, d, err := retrieval.request.LinkSystem.StorageWriteOpener(ipld.LinkContext{})
		if err != nil {
			return nil, err
		}
		_, err = w.Write(blk.RawData())
		if err != nil {
			return nil, err
		}
		blockBytes += uint64(len(blk.RawData()))
		err = d(cidlink.Link{Cid: blk.Cid()})
		if err != nil {
			return nil, err
		}
		blockCount++
	}

	duration := retrieval.Clock.Since(phaseStartTime)
	speed := uint64(float64(blockBytes) / duration.Seconds())

	return &types.RetrievalStats{
		RootCid:           candidate.RootCid,
		StorageProviderId: candidate.MinerPeer.ID,
		Size:              blockBytes,
		Blocks:            blockCount,
		Duration:          duration,
		AverageSpeed:      speed,
		TotalPayment:      big.Zero(),
		NumPayments:       0,
		AskPrice:          big.Zero(),
		TimeToFirstByte:   ttfb,
	}, nil
}

func (ph *ProtocolHttp) beginRequest(ctx context.Context, request types.RetrievalRequest, candidate types.RetrievalCandidate) error {
	var err error
	ph.req, err = makeRequest(ctx, request, candidate)
	if err == nil {
		ph.resp, err = ph.Client.Do(ph.req)
	}
	return err
}

func makeRequest(ctx context.Context, request types.RetrievalRequest, candidate types.RetrievalCandidate) (*http.Request, error) {
	candidateURL, err := candidate.ToURL()
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
