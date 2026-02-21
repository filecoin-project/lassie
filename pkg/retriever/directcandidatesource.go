package retriever

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/metadata"
)

var _ types.CandidateSource = &DirectCandidateSource{}

// DirectCandidateSource finds candidate protocols from a fixed set of peers
type DirectCandidateSource struct {
	providers []types.Provider
}

// NewDirectCandidateSource returns a new DirectCandidateFinder for the given providers
func NewDirectCandidateSource(providers []types.Provider) *DirectCandidateSource {
	return &DirectCandidateSource{
		providers: providers,
	}
}

// FindCandidates returns candidates for each configured provider
func (d *DirectCandidateSource) FindCandidates(ctx context.Context, c cid.Cid, cb func(types.RetrievalCandidate)) error {
	for _, provider := range d.providers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// If protocols are specified, use those
		if len(provider.Protocols) > 0 {
			cb(types.RetrievalCandidate{
				MinerPeer: provider.Peer,
				RootCid:   c,
				Metadata:  metadata.Default.New(provider.Protocols...),
			})
			continue
		}

		// For HTTP-only mode, assume HTTP protocol for all providers
		cb(types.RetrievalCandidate{
			MinerPeer: provider.Peer,
			RootCid:   c,
			Metadata:  metadata.Default.New(metadata.IpfsGatewayHttp{}),
		})
	}
	return nil
}
