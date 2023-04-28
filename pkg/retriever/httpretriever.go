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
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
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
	return &parallelPeerRetriever{
		Protocol: &ProtocolHttp{
			Client: client,
		},
		GetStorageProviderTimeout: getStorageProviderTimeout,
		Clock:                     clock.New(),
		QueueInitialPause:         2 * time.Millisecond,
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
	// we currently start a full HTTP request during "connect" phase, which means
	// we'll hit all candidates in parallel before proceeding to read the body
	// of ones we choose, one by one, until we get success. This may not be
	// optimal for servers that have to queue with a body, and may also result in
	// timeouts for body reading when we fail on one and move to another.
	// This may all need to move into the Retrieve() call and Connect() be a noop.
	var err error
	ph.req, err = makeRequest(ctx, retrieval.request, candidate)
	if err == nil {
		ph.resp, err = ph.Client.Do(ph.req)
	}
	return err
}

func (ph *ProtocolHttp) Retrieve(
	ctx context.Context,
	retrieval *retrieval,
	session *retrievalSession,
	phaseStartTime time.Time,
	timeout time.Duration,
	candidate types.RetrievalCandidate,
) (*types.RetrievalStats, error) {

	defer ph.resp.Body.Close()
	return readBody(candidate.RootCid, candidate.MinerPeer.ID, ph.resp.Body, retrieval.request.LinkSystem.StorageWriteOpener)
}

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
