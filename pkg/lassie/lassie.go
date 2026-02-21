package lassie

import (
	"context"
	"net/http"
	"time"

	"github.com/filecoin-project/lassie/pkg/extractor"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/session"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

var _ types.Fetcher = &Lassie{}

// Lassie represents a reusable retrieval client.
type Lassie struct {
	cfg       *LassieConfig
	retriever *retriever.Retriever
}

// LassieConfig customizes the behavior of a Lassie instance.
type LassieConfig struct {
	Source                types.CandidateSource
	GlobalTimeout         time.Duration
	ProviderBlockList     map[peer.ID]bool
	ProviderAllowList     map[peer.ID]bool
	SkipBlockVerification bool
}

type LassieOption func(cfg *LassieConfig)

// NewLassie creates a new Lassie instance.
func NewLassie(ctx context.Context, opts ...LassieOption) (*Lassie, error) {
	cfg := NewLassieConfig(opts...)
	return NewLassieWithConfig(ctx, cfg)
}

// NewLassieConfig creates a new LassieConfig instance with the given LassieOptions.
func NewLassieConfig(opts ...LassieOption) *LassieConfig {
	cfg := &LassieConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// NewLassieWithConfig creates a new Lassie instance with a custom
// configuration.
func NewLassieWithConfig(ctx context.Context, cfg *LassieConfig) (*Lassie, error) {
	// HTTP-only protocol
	protocols := []multicodec.Code{multicodec.TransportIpfsGatewayHttp}

	if cfg.Source == nil {
		var err error
		cfg.Source, err = indexerlookup.NewCandidateSource(
			indexerlookup.WithHttpClient(&http.Client{}),
			indexerlookup.WithProtocols(protocols),
		)
		if err != nil {
			return nil, err
		}
	}

	sessionConfig := session.DefaultConfig().
		WithProviderBlockList(cfg.ProviderBlockList).
		WithProviderAllowList(cfg.ProviderAllowList)
	sess := session.NewSession(sessionConfig, true)

	httpRetriever := retriever.NewHttpRetriever(sess, http.DefaultClient)
	ret, err := retriever.NewRetriever(ctx, sess, cfg.Source, httpRetriever, multicodec.TransportIpfsGatewayHttp)
	if err != nil {
		return nil, err
	}

	// Wrap the retriever with HybridRetriever for per-block fallback
	ret.WrapWithHybrid(cfg.Source, http.DefaultClient, cfg.SkipBlockVerification)

	ret.Start()

	l := &Lassie{
		cfg:       cfg,
		retriever: ret,
	}

	return l, nil
}

// WithCandidateSource allows you to specify a custom candidate finder.
func WithCandidateSource(finder types.CandidateSource) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.Source = finder
	}
}

// WithGlobalTimeout allows you to specify a custom timeout for the entire
// retrieval process.
func WithGlobalTimeout(timeout time.Duration) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.GlobalTimeout = timeout
	}
}

// WithProviderBlockList allows you to specify a custom provider block list.
func WithProviderBlockList(providerBlockList map[peer.ID]bool) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.ProviderBlockList = providerBlockList
	}
}

// WithProviderAllowList allows you to specify a custom set of providers to
// allow fetching from. If this is not set, all providers will be allowed unless
// they are in the block list.
func WithProviderAllowList(providerAllowList map[peer.ID]bool) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.ProviderAllowList = providerAllowList
	}
}

// WithSkipBlockVerification disables per-block hash verification.
// WARNING: This is dangerous - malicious gateways can serve arbitrary data!
func WithSkipBlockVerification(skip bool) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.SkipBlockVerification = skip
	}
}

// Fetch initiates a retrieval request and returns either some details about
// the retrieval or an error. The request should contain all of the parameters
// of the requested retrieval, including the LinkSystem where the blocks are
// intended to be stored.
func (l *Lassie) Fetch(ctx context.Context, request types.RetrievalRequest, opts ...types.FetchOption) (*types.RetrievalStats, error) {
	var cancel context.CancelFunc
	if l.cfg.GlobalTimeout != time.Duration(0) {
		ctx, cancel = context.WithTimeout(ctx, l.cfg.GlobalTimeout)
		defer cancel()
	}
	return l.retriever.Retrieve(ctx, request, types.NewFetchConfig(opts...).EventsCallback)
}

// RegisterSubscriber registers a subscriber to receive retrieval events.
// The returned function can be called to unregister the subscriber.
func (l *Lassie) RegisterSubscriber(subscriber types.RetrievalEventSubscriber) func() {
	return l.retriever.RegisterSubscriber(subscriber)
}

// Extract retrieves content and extracts it directly to disk.
// This is memory-efficient: blocks are processed once and discarded.
func (l *Lassie) Extract(
	ctx context.Context,
	rootCid cid.Cid,
	ext *extractor.Extractor,
	eventsCallback func(types.RetrievalEvent),
	onBlock func(int),
) (*types.RetrievalStats, error) {
	var cancel context.CancelFunc
	if l.cfg.GlobalTimeout != time.Duration(0) {
		ctx, cancel = context.WithTimeout(ctx, l.cfg.GlobalTimeout)
		defer cancel()
	}
	return l.retriever.RetrieveAndExtract(ctx, rootCid, ext, eventsCallback, onBlock)
}
