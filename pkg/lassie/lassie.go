package lassie

import (
	"context"
	"time"

	"github.com/filecoin-project/lassie/pkg/client"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/internal"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
)

type Lassie struct {
	cfg       *LassieConfig
	retriever *retriever.Retriever
}

type LassieConfig struct {
	Finder                 retriever.CandidateFinder
	Host                   host.Host
	ProviderTimeout        time.Duration
	ConcurrentSPRetrievals uint
	GlobalTimeout          time.Duration
	Libp2pOptions          []libp2p.Option
}

type LassieOption func(cfg *LassieConfig)

func NewLassie(ctx context.Context, opts ...LassieOption) (*Lassie, error) {
	cfg := &LassieConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return NewLassieWithConfig(ctx, cfg)
}

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

	lassie := &Lassie{
		cfg:       cfg,
		retriever: retriever,
	}

	return lassie, nil
}

func WithFinder(finder retriever.CandidateFinder) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.Finder = finder
	}
}

func WithProviderTimeout(timeout time.Duration) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.ProviderTimeout = timeout
	}
}

func WithGlobalTimeout(timeout time.Duration) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.GlobalTimeout = timeout
	}
}
func WithHost(host host.Host) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.Host = host
	}
}

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

func (l *Lassie) Retrieve(ctx context.Context, request types.RetrievalRequest) (*types.RetrievalStats, error) {
	var cancel context.CancelFunc
	if l.cfg.GlobalTimeout != time.Duration(0) {
		ctx, cancel = context.WithTimeout(ctx, l.cfg.GlobalTimeout)
		defer cancel()
	}
	return l.retriever.Retrieve(ctx, request, func(types.RetrievalEvent) {})
}

func (l *Lassie) Fetch(ctx context.Context, rootCid cid.Cid, linkSystem linking.LinkSystem) (types.RetrievalID, *types.RetrievalStats, error) {
	// Assign an ID to this retrieval
	retrievalId, err := types.NewRetrievalID()
	if err != nil {
		return types.RetrievalID{}, nil, err
	}

	// retrieve!
	request := types.RetrievalRequest{RetrievalID: retrievalId, Cid: rootCid, LinkSystem: linkSystem}
	stats, err := l.retriever.Retrieve(ctx, request, func(types.RetrievalEvent) {})
	if err != nil {
		return retrievalId, nil, err
	}

	return retrievalId, stats, nil
}

func (l *Lassie) RegisterSubscriber(subscriber types.RetrievalEventSubscriber) func() {
	return l.retriever.RegisterSubscriber(subscriber)
}
