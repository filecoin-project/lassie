package indexerlookup

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/metadata"
	"github.com/multiformats/go-multihash"
)

var (
	_ types.CandidateSource = (*IndexerCandidateSource)(nil)

	logger = log.Logger("lassie/indexerlookup")
)

type IndexerCandidateSource struct {
	*options
}

func NewCandidateSource(o ...Option) (*IndexerCandidateSource, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	return &IndexerCandidateSource{
		options: opts,
	}, nil
}

func (idxf *IndexerCandidateSource) sendJsonRequest(req *http.Request) (*model.FindResponse, error) {
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

func (idxf *IndexerCandidateSource) FindCandidates(ctx context.Context, c cid.Cid, cb func(types.RetrievalCandidate)) error {
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

func (idxf *IndexerCandidateSource) newFindHttpRequest(ctx context.Context, c cid.Cid) (*http.Request, error) {
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

func (idxf *IndexerCandidateSource) decodeProviderResultStream(ctx context.Context, c cid.Cid, from io.ReadCloser, cb func(types.RetrievalCandidate)) error {
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

func (idxf *IndexerCandidateSource) findByMultihashEndpoint(mh multihash.Multihash) string {
	return idxf.httpEndpoint.JoinPath("multihash", mh.B58String()).String()
}
