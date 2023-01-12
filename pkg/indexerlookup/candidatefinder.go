package indexerlookup

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"

	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipni/storetheindex/api/v0/finder/model"
)

type IndexerCandidateFinder struct {
	c       *http.Client
	baseUrl string
}

func NewCandidateFinder(url string) *IndexerCandidateFinder {
	return &IndexerCandidateFinder{
		c: &http.Client{
			Timeout: time.Minute,
		},
		baseUrl: url,
	}
}

func (idxf *IndexerCandidateFinder) sendRequest(req *http.Request) (*model.FindResponse, error) {
	req.Header.Set("Content-Type", "application/json")
	resp, err := idxf.c.Do(req)
	if err != nil {
		return nil, err
	}
	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return &model.FindResponse{}, nil
		}
		return nil, fmt.Errorf("batch find query failed: %v", http.StatusText(resp.StatusCode))
	}

	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return model.UnmarshalFindResponse(b)
}

func (idxf *IndexerCandidateFinder) FindCandidates(ctx context.Context, cid cid.Cid) ([]types.RetrievalCandidate, error) {
	u := fmt.Sprint(idxf.baseUrl, "/multihash/", cid.Hash().B58String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	parsedResp, err := idxf.sendRequest(req)
	if err != nil {
		return nil, err
	}
	hash := string(cid.Hash())
	// turn parsedResp into records.
	var matches []types.RetrievalCandidate

	indices := rand.Perm(len(parsedResp.MultihashResults))
	for _, i := range indices {
		multihashResult := parsedResp.MultihashResults[i]

		if !(string(multihashResult.Multihash) == hash) {
			continue
		}
		for _, val := range multihashResult.ProviderResults {
			// filter out any results that aren't filecoin graphsync
			var dtm metadata.GraphsyncFilecoinV1
			if err := dtm.UnmarshalBinary(val.Metadata); err != nil {
				continue
			}

			matches = append(matches, types.RetrievalCandidate{
				RootCid:   cid,
				MinerPeer: val.Provider,
			})
		}
	}
	return matches, nil
}
