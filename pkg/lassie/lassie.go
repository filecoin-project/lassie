package lassie

import (
	"context"
	"time"

	"github.com/filecoin-project/lassie/pkg/client"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/internal"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
)

// Lassie represents a reusable retrieval client.
type Lassie struct {
	cfg       *LassieConfig
	retriever *retriever.Retriever
}

// LassieConfig customizes the behavior of a Lassie instance.
type LassieConfig struct {
	Finder                 retriever.CandidateFinder
	Host                   host.Host
	ProviderTimeout        time.Duration
	ConcurrentSPRetrievals uint
	GlobalTimeout          time.Duration
	Libp2pOptions          []libp2p.Option
}

type LassieOption func(cfg *LassieConfig)

// NewLassie creates a new Lassie instance.
func NewLassie(ctx context.Context, opts ...LassieOption) (*Lassie, error) {
	cfg := &LassieConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return NewLassieWithConfig(ctx, cfg)
}

// NewLassieWithConfig creates a new Lassie instance with a custom
// configuration.
func NewLassieWithConfig(ctx context.Context, cfg *LassieConfig) (*Lassie, error) {
	if cfg.Finder == nil {
		var err error
		cfg.Finder, err = indexerlookup.NewCandidateFinder()
		if err != nil {
			return nil, err
		}
	}

	if cfg.ProviderTimeout == 0 {
		cfg.ProviderTimeout = 20 * time.Second
	}

	datastore := sync.MutexWrap(datastore.NewMapDatastore())

	if cfg.Host == nil {
		var err error
		cfg.Host, err = internal.InitHost(ctx, cfg.Libp2pOptions)
		if err != nil {
			return nil, err
		}
	}

	retrievalClient, err := client.NewClient(datastore, cfg.Host, nil)
	if err != nil {
		return nil, err
	}

	bitswapRetriever := retriever.NewBitswapRetrieverFromHost(ctx, cfg.Host, retriever.BitswapConfig{
		BlockTimeout: cfg.ProviderTimeout,
	})
	retrieverCfg := retriever.RetrieverConfig{
		DefaultMinerConfig: retriever.MinerConfig{
			RetrievalTimeout:        cfg.ProviderTimeout,
			MaxConcurrentRetrievals: cfg.ConcurrentSPRetrievals,
		},
	}

	retriever, err := retriever.NewRetriever(ctx, retrieverCfg, retrievalClient, cfg.Finder, bitswapRetriever)
	if err != nil {
		return nil, err
	}
	retriever.Start()
	if err := retrievalClient.AwaitReady(); err != nil { // wait for dt setup
		return nil, err
	}

	lassie := &Lassie{
		cfg:       cfg,
		retriever: retriever,
	}

	return lassie, nil
}

// WithFinder allows you to specify a custom candidate finder.
func WithFinder(finder retriever.CandidateFinder) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.Finder = finder
	}
}

// WithProviderTimeout allows you to specify a custom timeout for retrieving
// data from a provider. Beyond this limit, when no data has been received,
// the retrieval will fail.
func WithProviderTimeout(timeout time.Duration) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.ProviderTimeout = timeout
	}
}

// WithGlobalTimeout allows you to specify a custom timeout for the entire
// retrieval process.
func WithGlobalTimeout(timeout time.Duration) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.GlobalTimeout = timeout
	}
}

// WithHost allows you to specify a custom libp2p host.
func WithHost(host host.Host) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.Host = host
	}
}

// WithLibp2pOpts allows you to specify custom libp2p options.
func WithLibp2pOpts(libp2pOptions ...libp2p.Option) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.Libp2pOptions = libp2pOptions
	}
}

func WithConcurrentSPRetrievals(maxConcurrentSPRtreievals uint) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.ConcurrentSPRetrievals = maxConcurrentSPRtreievals
	}
}

func (l *Lassie) Fetch(ctx context.Context, request types.RetrievalRequest) (*types.RetrievalStats, error) {
	var cancel context.CancelFunc
	if l.cfg.GlobalTimeout != time.Duration(0) {
		ctx, cancel = context.WithTimeout(ctx, l.cfg.GlobalTimeout)
		defer cancel()
	}
	return l.retriever.Retrieve(ctx, request, func(types.RetrievalEvent) {})
}

// RegisterSubscriber registers a subscriber to receive retrieval events.
// The returned function can be called to unregister the subscriber.
func (l *Lassie) RegisterSubscriber(subscriber types.RetrievalEventSubscriber) func() {
	return l.retriever.RegisterSubscriber(subscriber)
}
