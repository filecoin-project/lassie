package blockbroker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime/linking"
	trustlessutils "github.com/ipld/go-trustless-utils"
	trustlesshttp "github.com/ipld/go-trustless-utils/http"
	"github.com/ipld/go-trustless-utils/traversal"
	"github.com/libp2p/go-libp2p/core/peer"
)

var logger = log.Logger("lassie/blockbroker")

var _ BlockSession = (*TrustlessGatewaySession)(nil)

// TrustlessGatewaySession fetches blocks from HTTP trustless gateways,
// caching discovered providers and evicting ones that fail.
type TrustlessGatewaySession struct {
	routing    types.CandidateSource
	httpClient *http.Client

	providers     []types.RetrievalCandidate
	providersLock sync.RWMutex

	evictedPeers     map[peer.ID]time.Time
	evictedPeersLock sync.RWMutex
	evictBackoff     time.Duration

	usedProviders     map[string]struct{}
	usedProvidersLock sync.RWMutex

	skipBlockVerification bool
}

func NewSession(
	routing types.CandidateSource,
	httpClient *http.Client,
	skipBlockVerification bool,
) *TrustlessGatewaySession {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	return &TrustlessGatewaySession{
		routing:               routing,
		httpClient:            httpClient,
		providers:             make([]types.RetrievalCandidate, 0),
		evictedPeers:          make(map[peer.ID]time.Time),
		evictBackoff:          30 * time.Second,
		usedProviders:         make(map[string]struct{}),
		skipBlockVerification: skipBlockVerification,
	}
}

func (s *TrustlessGatewaySession) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	providers := s.getProviders()
	if len(providers) > 0 {
		block, err := s.tryProviders(ctx, c, providers)
		if err == nil {
			return block, nil
		}
		logger.Debugw("all cached providers failed", "cid", c, "err", err)
	}

	if err := s.findNewProviders(ctx, c); err != nil {
		return nil, fmt.Errorf("no providers found for %s: %w", c, err)
	}

	providers = s.getProviders()
	if len(providers) == 0 {
		return nil, fmt.Errorf("no providers available for %s", c)
	}
	return s.tryProviders(ctx, c, providers)
}

func (s *TrustlessGatewaySession) GetSubgraph(ctx context.Context, c cid.Cid, lsys linking.LinkSystem) (int, error) {
	providers := s.getProviders()
	if len(providers) == 0 {
		if err := s.findNewProviders(ctx, c); err != nil {
			return 0, fmt.Errorf("no providers found for %s: %w", c, err)
		}
		providers = s.getProviders()
	}

	if len(providers) == 0 {
		return 0, fmt.Errorf("no providers available for %s", c)
	}

	var lastErr error
	for _, provider := range providers {
		blocksReceived, err := s.fetchSubgraphCAR(ctx, c, provider, lsys)
		if err == nil {
			logger.Debugw("subgraph CAR fetch succeeded", "cid", c, "provider", provider.Endpoint(), "blocks", blocksReceived)
			s.markProviderUsed(provider.Endpoint())
			return blocksReceived, nil
		}
		logger.Debugw("subgraph CAR fetch failed", "cid", c, "provider", provider.Endpoint(), "err", err)
		lastErr = err
	}

	return 0, fmt.Errorf("all providers failed for subgraph %s: %w", c, lastErr)
}

func (s *TrustlessGatewaySession) fetchSubgraphCAR(
	ctx context.Context,
	c cid.Cid,
	provider types.RetrievalCandidate,
	lsys linking.LinkSystem,
) (int, error) {
	providerURL, err := provider.ToURL()
	if err != nil {
		return 0, fmt.Errorf("failed to get URL for provider: %w", err)
	}

	reqURL := fmt.Sprintf("%s/ipfs/%s?dag-scope=all", providerURL, c)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", trustlesshttp.DefaultContentType().String())

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d from %s", resp.StatusCode, providerURL)
	}

	expectDuplicates := trustlesshttp.DefaultIncludeDupes
	if contentType, valid := trustlesshttp.ParseContentType(resp.Header.Get("Content-Type")); valid {
		expectDuplicates = contentType.Duplicates
	}

	defaultSelector := trustlessutils.Request{Scope: trustlessutils.DagScopeAll}.Selector()
	cfg := traversal.Config{
		Root:               c,
		Selector:           defaultSelector,
		ExpectDuplicatesIn: expectDuplicates,
		WriteDuplicatesOut: expectDuplicates,
	}

	result, err := cfg.VerifyCar(ctx, resp.Body, lsys)
	if err != nil {
		return int(result.BlocksIn), err
	}

	return int(result.BlocksIn), nil
}

func (s *TrustlessGatewaySession) getProviders() []types.RetrievalCandidate {
	s.providersLock.RLock()
	defer s.providersLock.RUnlock()
	result := make([]types.RetrievalCandidate, len(s.providers))
	copy(result, s.providers)
	return result
}

func (s *TrustlessGatewaySession) tryProviders(ctx context.Context, c cid.Cid, providers []types.RetrievalCandidate) (blocks.Block, error) {
	if len(providers) == 0 {
		return nil, fmt.Errorf("no providers available")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type fetchResult struct {
		block    blocks.Block
		err      error
		provider types.RetrievalCandidate
	}

	results := make(chan fetchResult, len(providers))
	for _, p := range providers {
		go func(provider types.RetrievalCandidate) {
			block, err := s.fetchBlock(ctx, c, provider)
			select {
			case results <- fetchResult{block: block, err: err, provider: provider}:
			case <-ctx.Done():
			}
		}(p)
	}

	var lastErr error
	for range providers {
		select {
		case result := <-results:
			if result.err != nil {
				logger.Debugw("provider failed", "cid", c, "provider", result.provider.Endpoint(), "err", result.err)
				s.evictProvider(result.provider.MinerPeer.ID)
				lastErr = result.err
				continue
			}
			cancel()
			s.markProviderUsed(result.provider.Endpoint())
			return result.block, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("all providers failed: %w", lastErr)
}

func (s *TrustlessGatewaySession) fetchBlock(
	ctx context.Context,
	c cid.Cid,
	provider types.RetrievalCandidate,
) (blocks.Block, error) {
	url, err := provider.ToURL()
	if err != nil {
		return nil, fmt.Errorf("failed to get URL for provider: %w", err)
	}

	reqURL := fmt.Sprintf("%s/ipfs/%s?format=raw", url, c)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.ipld.raw")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if !s.skipBlockVerification {
		computed, err := c.Prefix().Sum(data)
		if err != nil {
			return nil, fmt.Errorf("failed to compute hash: %w", err)
		}
		if !computed.Equals(c) {
			return nil, fmt.Errorf("cid mismatch: expected %s, got %s", c, computed)
		}
	}

	block, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return nil, fmt.Errorf("block creation failed: %w", err)
	}

	return block, nil
}

func (s *TrustlessGatewaySession) findNewProviders(ctx context.Context, c cid.Cid) error {
	var found int
	var mu sync.Mutex

	err := s.routing.FindCandidates(ctx, c, func(candidate types.RetrievalCandidate) {
		if !s.hasHTTPProtocol(candidate) {
			return
		}
		if s.isEvicted(candidate.MinerPeer.ID) {
			return
		}

		mu.Lock()
		s.addProvider(candidate)
		found++
		mu.Unlock()
	})

	if err != nil {
		return err
	}
	if found == 0 {
		return fmt.Errorf("no HTTP providers found for %s", c)
	}
	logger.Debugw("found new providers", "cid", c, "count", found)
	return nil
}

func (s *TrustlessGatewaySession) SeedProviders(ctx context.Context, c cid.Cid) {
	_ = s.findNewProviders(ctx, c)
}

func (s *TrustlessGatewaySession) addProvider(candidate types.RetrievalCandidate) {
	s.providersLock.Lock()
	defer s.providersLock.Unlock()

	for _, p := range s.providers {
		if p.MinerPeer.ID == candidate.MinerPeer.ID {
			return
		}
	}
	s.providers = append(s.providers, candidate)
}

func (s *TrustlessGatewaySession) evictProvider(peerID peer.ID) {
	s.evictedPeersLock.Lock()
	s.evictedPeers[peerID] = time.Now()
	s.evictedPeersLock.Unlock()

	s.providersLock.Lock()
	defer s.providersLock.Unlock()

	for i, p := range s.providers {
		if p.MinerPeer.ID == peerID {
			s.providers = append(s.providers[:i], s.providers[i+1:]...)
			return
		}
	}
}

func (s *TrustlessGatewaySession) isEvicted(peerID peer.ID) bool {
	s.evictedPeersLock.RLock()
	defer s.evictedPeersLock.RUnlock()

	evictTime, ok := s.evictedPeers[peerID]
	if !ok {
		return false
	}
	return time.Since(evictTime) < s.evictBackoff
}

func (s *TrustlessGatewaySession) hasHTTPProtocol(candidate types.RetrievalCandidate) bool {
	for _, addr := range candidate.MinerPeer.Addrs {
		for _, proto := range addr.Protocols() {
			if proto.Name == "http" || proto.Name == "https" {
				return true
			}
		}
	}
	return false
}

func (s *TrustlessGatewaySession) markProviderUsed(endpoint string) {
	s.usedProvidersLock.Lock()
	defer s.usedProvidersLock.Unlock()
	s.usedProviders[endpoint] = struct{}{}
}

func (s *TrustlessGatewaySession) UsedProviders() []string {
	s.usedProvidersLock.RLock()
	defer s.usedProvidersLock.RUnlock()
	result := make([]string, 0, len(s.usedProviders))
	for endpoint := range s.usedProviders {
		result = append(result, endpoint)
	}
	return result
}

func (s *TrustlessGatewaySession) GetSubgraphStream(ctx context.Context, c cid.Cid) (io.ReadCloser, string, error) {
	providers := s.getProviders()
	if len(providers) == 0 {
		if err := s.findNewProviders(ctx, c); err != nil {
			return nil, "", fmt.Errorf("no providers found for %s: %w", c, err)
		}
		providers = s.getProviders()
	}

	if len(providers) == 0 {
		return nil, "", fmt.Errorf("no providers available for %s", c)
	}

	var lastErr error
	for _, provider := range providers {
		rdr, err := s.fetchSubgraphStream(ctx, c, provider)
		if err == nil {
			s.markProviderUsed(provider.Endpoint())
			return rdr, provider.Endpoint(), nil
		}
		logger.Debugw("subgraph stream fetch failed", "cid", c, "provider", provider.Endpoint(), "err", err)
		lastErr = err
	}

	return nil, "", fmt.Errorf("all providers failed for subgraph stream %s: %w", c, lastErr)
}

func (s *TrustlessGatewaySession) fetchSubgraphStream(
	ctx context.Context,
	c cid.Cid,
	provider types.RetrievalCandidate,
) (io.ReadCloser, error) {
	providerURL, err := provider.ToURL()
	if err != nil {
		return nil, fmt.Errorf("failed to get URL for provider: %w", err)
	}

	reqURL := fmt.Sprintf("%s/ipfs/%s?dag-scope=all&dups=y", providerURL, c)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", trustlesshttp.DefaultContentType().String())

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, providerURL)
	}

	return resp.Body, nil
}

func (s *TrustlessGatewaySession) Close() error {
	s.providersLock.Lock()
	s.providers = nil
	s.providersLock.Unlock()

	s.evictedPeersLock.Lock()
	s.evictedPeers = nil
	s.evictedPeersLock.Unlock()

	s.usedProvidersLock.Lock()
	s.usedProviders = nil
	s.usedProvidersLock.Unlock()

	return nil
}
