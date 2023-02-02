package indexerlookup

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"path"

	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-log/v2"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

var (
	_ retriever.CandidateFinder = (*IndexerCandidateFinder)(nil)

	logger = log.Logger("indexerlookup")
)

type IndexerCandidateFinder struct {
	*options
}

func NewCandidateFinder(o ...Option) (*IndexerCandidateFinder, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	return &IndexerCandidateFinder{
		options: opts,
	}, nil
}

func (idxf *IndexerCandidateFinder) sendJsonRequest(req *http.Request) (*model.FindResponse, error) {
	req.Header.Set("Accept", "application/json")
	resp, err := idxf.httpClient.Do(req)
	if err != nil {
		logger.Debugw("Failed to perform json lookup", "err", err)
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			logger.Debugw("Failed to read response JSON response body", "err", err)
			return nil, err
		}
		return model.UnmarshalFindResponse(b)
	case http.StatusNotFound:
		return &model.FindResponse{}, nil
	default:
		return nil, fmt.Errorf("batch find query failed: %s", http.StatusText(resp.StatusCode))
	}
}

func (idxf *IndexerCandidateFinder) FindCandidates(ctx context.Context, cid cid.Cid) ([]types.RetrievalCandidate, error) {
	req, err := idxf.newFindHttpRequest(ctx, cid)
	if err != nil {
		return nil, err
	}
	parsedResp, err := idxf.sendJsonRequest(req)
	if err != nil {
		return nil, err
	}
	// turn parsedResp into records.
	var matches []types.RetrievalCandidate

	indices := rand.Perm(len(parsedResp.MultihashResults))
	for _, i := range indices {
		multihashResult := parsedResp.MultihashResults[i]
		if !bytes.Equal(cid.Hash(), multihashResult.Multihash) {
			continue
		}
		for _, val := range multihashResult.ProviderResults {
			// filter out any results that aren't filecoin graphsync
			if hasTransportGraphsyncFilecoinv1(val) {
				matches = append(matches, types.RetrievalCandidate{
					RootCid:   cid,
					MinerPeer: val.Provider,
				})
			}
		}
	}
	return matches, nil
}

func hasTransportGraphsyncFilecoinv1(pr model.ProviderResult) bool {
	if len(pr.Metadata) == 0 {
		return false
	}
	// Metadata may contain more than one protocol, sorted by ascending order of their protocol ID.
	// Therefore, decode the metadata as metadata.Metadata, then check if it supports Graphsync.
	// See: https://github.com/ipni/specs/blob/main/IPNI.md#metadata
	dtm := metadata.Default.New()
	if err := dtm.UnmarshalBinary(pr.Metadata); err != nil {
		logger.Debugw("Failed to unmarshal metadata", "err", err)
		return false
	}
	return dtm.Get(multicodec.TransportGraphsyncFilecoinv1) != nil
}

func (idxf *IndexerCandidateFinder) FindCandidatesAsync(ctx context.Context, c cid.Cid) (<-chan types.FindCandidatesResult, error) {
	req, err := idxf.newFindHttpRequest(ctx, c)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/x-ndjson")
	resp, err := idxf.httpClient.Do(req)
	if err != nil {
		logger.Debugw("Failed to perform streaming lookup", "err", err)
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		defer resp.Body.Close()
		return idxf.decodeProviderResultStream(ctx, c, resp.Body)
	case http.StatusNotFound:
		return nil, nil
	default:
		return nil, fmt.Errorf("batch find query failed: %v", http.StatusText(resp.StatusCode))
	}
}

func (idxf *IndexerCandidateFinder) newFindHttpRequest(ctx context.Context, c cid.Cid) (*http.Request, error) {
	endpoint := idxf.findByMultihashEndpoint(c.Hash())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	if idxf.httpUserAgent != "" {
		req.Header.Set("User-Agent", idxf.httpUserAgent)
	}
	return req, nil
}

func (idxf *IndexerCandidateFinder) decodeProviderResultStream(ctx context.Context, c cid.Cid, from io.Reader) (<-chan types.FindCandidatesResult, error) {
	rch := make(chan types.FindCandidatesResult, idxf.asyncResultsChanBuffer)
	go func() {
		defer close(rch)
		scanner := bufio.NewScanner(from)
		for {
			var r types.FindCandidatesResult
			select {
			case <-ctx.Done():
				r.Err = ctx.Err()
				rch <- r
				return
			default:
				if scanner.Scan() {
					line := scanner.Bytes()
					if len(line) == 0 {
						continue
					}
					var pr model.ProviderResult
					if r.Err = json.Unmarshal(line, &pr); r.Err != nil {
						rch <- r
						return
					}
					r.Candidate.MinerPeer = pr.Provider
					r.Candidate.RootCid = c
					rch <- r
				} else if r.Err = scanner.Err(); r.Err != nil {
					rch <- r
					return
				}
			}
		}
	}()
	return rch, nil
}

func (idxf *IndexerCandidateFinder) findByMultihashEndpoint(mh multihash.Multihash) string {
	// TODO: Replace with URL.JoinPath once minimum go version in CI is updated to 1.19; like this:
	//       return idxf.httpEndpoint.JoinPath("multihash", mh.B58String()).String()
	return idxf.httpEndpoint.String() + path.Join("/multihash", mh.B58String())
}
