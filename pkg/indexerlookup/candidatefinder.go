package indexerlookup

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"path"

	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/metadata"
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
	logger.Debugw("sending outgoing request", "url", req.URL, "accept", req.Header.Get("Accept"))
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
			// skip results without decodable metadata
			if md, err := decodeMetadata(val); err == nil {
				candidate := types.RetrievalCandidate{
					RootCid:  cid,
					Metadata: md,
				}
				if val.Provider != nil {
					candidate.MinerPeer = *val.Provider
				}
				matches = append(matches, candidate)
			}
		}
	}
	return matches, nil
}

func decodeMetadata(pr model.ProviderResult) (metadata.Metadata, error) {
	if len(pr.Metadata) == 0 {
		return metadata.Metadata{}, errors.New("no metadata")
	}
	// Metadata may contain more than one protocol, sorted by ascending order of their protocol ID.
	// Therefore, decode the metadata as metadata.Metadata, then check if it supports Graphsync.
	// See: https://github.com/ipni/specs/blob/main/IPNI.md#metadata
	dtm := metadata.Default.New()
	if err := dtm.UnmarshalBinary(pr.Metadata); err != nil {
		logger.Debugw("Failed to unmarshal metadata", "err", err)
		return metadata.Metadata{}, err
	}
	return dtm, nil
}

func (idxf *IndexerCandidateFinder) FindCandidatesAsync(ctx context.Context, c cid.Cid, cb func(types.RetrievalCandidate)) error {
	req, err := idxf.newFindHttpRequest(ctx, c)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/x-ndjson")
	logger.Debugw("sending outgoing request", "url", req.URL, "accept", req.Header.Get("Accept"))
	resp, err := idxf.httpClient.Do(req)
	if err != nil {
		logger.Debugw("Failed to perform streaming lookup", "err", err)
		return err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return idxf.decodeProviderResultStream(ctx, c, resp.Body, cb)
	case http.StatusNotFound:
		return nil
	default:
		return fmt.Errorf("batch find query failed: %v", http.StatusText(resp.StatusCode))
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
	if idxf.ipfsDhtCascade {
		query := req.URL.Query()
		query.Add("cascade", "ipfs-dht")
		req.URL.RawQuery = query.Encode()
	}
	if idxf.legacyCascade {
		query := req.URL.Query()
		query.Add("cascade", "legacy")
		req.URL.RawQuery = query.Encode()
	}
	return req, nil
}

func (idxf *IndexerCandidateFinder) decodeProviderResultStream(ctx context.Context, c cid.Cid, from io.ReadCloser, cb func(types.RetrievalCandidate)) error {
	defer from.Close()
	scanner := bufio.NewScanner(from)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if scanner.Scan() {
				line := scanner.Bytes()
				if len(line) == 0 {
					continue
				}
				var pr model.ProviderResult
				if err := json.Unmarshal(line, &pr); err != nil {
					return err
				}
				// skip results without decodable metadata
				if md, err := decodeMetadata(pr); err == nil {
					var candidate types.RetrievalCandidate
					if pr.Provider != nil {
						candidate.MinerPeer = *pr.Provider
					}
					candidate.RootCid = c
					candidate.Metadata = md
					cb(candidate)
				}
			} else if err := scanner.Err(); err != nil {
				return err
			} else {
				// There are no more lines remaining to scan as we have reached EOF.
				return nil
			}
		}
	}
}

func (idxf *IndexerCandidateFinder) findByMultihashEndpoint(mh multihash.Multihash) string {
	// TODO: Replace with URL.JoinPath once minimum go version in CI is updated to 1.19; like this:
	//       return idxf.httpEndpoint.JoinPath("multihash", mh.B58String()).String()
	return idxf.httpEndpoint.String() + path.Join("/multihash", mh.B58String())
}
