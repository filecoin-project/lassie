package bitswaphelpers

import (
	"context"
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

var log = logging.Logger("lassie/bitswap")

var _ routing.Routing = (*IndexerRouting)(nil)

// IndexerRouting provides an interface that satisfies routing.Routing but only returns
// provider records based on a preset set of providers read from the context key.
// Bitswap will potentially make multiple FindProvidersAsync requests, and the cid passed will not always be the root
// As a result, we have to rely on the retrieval id within a context key
// Also while there is a delegated routing client that talks to the indexer, we use this cause we run it on
// top of the processing we're doing at a higher level with multiprotocol filtering
type IndexerRouting struct {
	routinghelpers.Null
	providerSets   map[types.RetrievalID][]types.RetrievalCandidate
	providerSetsLk sync.Mutex
	toRetrievalIDs func(cid.Cid) []types.RetrievalID
}

// NewIndexerRouting makes a new indexer routing instance
func NewIndexerRouting(toRetrievalID func(cid.Cid) []types.RetrievalID) *IndexerRouting {
	return &IndexerRouting{
		providerSets:   make(map[types.RetrievalID][]types.RetrievalCandidate),
		toRetrievalIDs: toRetrievalID,
	}
}

// RemoveProviders removes all provider records for a given retrieval id
func (ir *IndexerRouting) RemoveProviders(retrievalID types.RetrievalID) {
	ir.providerSetsLk.Lock()
	defer ir.providerSetsLk.Unlock()
	delete(ir.providerSets, retrievalID)
}

// AddProviders adds provider records to the total list for a given retrieval id
func (ir *IndexerRouting) AddProviders(retrievalID types.RetrievalID, providers []types.RetrievalCandidate) {
	// dedup results to provide better answers
	uniqueProvidersSet := make(map[string]struct{}, len(providers))
	uniqueProviders := make([]types.RetrievalCandidate, 0, len(providers))
	for _, p := range providers {
		if _, ok := uniqueProvidersSet[p.MinerPeer.String()]; !ok {
			uniqueProvidersSet[p.MinerPeer.String()] = struct{}{}
			uniqueProviders = append(uniqueProviders, p)
		}
	}
	ir.providerSetsLk.Lock()
	defer ir.providerSetsLk.Unlock()
	ir.providerSets[retrievalID] = append(ir.providerSets[retrievalID], uniqueProviders...)
}

// FindProvidersAsync returns providers based on the retrieval id in a context key
// It returns a channel with up to `max` providers, keeping the others around for a future call
// TODO: there is a slight risk that go-bitswap, which dedups requests by CID across multiple sessions,
// could accidentally read the wrong retrieval id if two retrievals were running at the same time. Not sure how much
// of a risk this really is, cause when requests are deduped, both calls still receive the results. See go-bitswap
// ProviderQueryManager for more specifics
func (ir *IndexerRouting) FindProvidersAsync(ctx context.Context, c cid.Cid, max int) <-chan peer.AddrInfo {
	resultsChan := make(chan peer.AddrInfo)

	go func() {
		defer close(resultsChan)

		retrievalIDs := ir.toRetrievalIDs(c)
		ir.providerSetsLk.Lock()
		var providers []types.RetrievalCandidate
		for _, retrievalID := range retrievalIDs {
			providers = append(providers, ir.providerSets[retrievalID]...)
			if len(providers) > max {
				providers, ir.providerSets[retrievalID] = providers[:max], providers[max:]
				break
			}
			if len(ir.providerSets) == 0 {
				delete(ir.providerSets, retrievalID)
			}
		}
		ir.providerSetsLk.Unlock()
		log.Debugw("provider records requested from bitswap, sending back indexer results", "providerCount", len(providers))
		for _, p := range providers {
			select {
			case <-ctx.Done():
				return
			case resultsChan <- p.MinerPeer:
			}
		}
	}()
	return resultsChan
}
