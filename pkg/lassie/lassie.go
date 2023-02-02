package lassie

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/internal"
	"github.com/filecoin-project/lassie/pkg/client"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/multiformats/go-multiaddr"
)

type Lassie struct {
	cfg       *LassieConfig
	retriever *retriever.Retriever
}

type LassieConfig struct {
	Finder  retriever.CandidateFinder
	Timeout time.Duration
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
		cfg.Finder = indexerlookup.NewCandidateFinder("https://cid.contact")
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = 20 * time.Second
	}

	datastore := sync.MutexWrap(datastore.NewMapDatastore())

	host, err := internal.InitHost(ctx, multiaddr.StringCast("/ip4/0.0.0.0/tcp/6746"))
	if err != nil {
		return nil, err
	}

	retrievalClient, err := client.NewClient(datastore, host, nil)
	if err != nil {
		return nil, err
	}

	retrieverCfg := retriever.RetrieverConfig{
		DefaultMinerConfig: retriever.MinerConfig{
			RetrievalTimeout: cfg.Timeout,
		},
	}

	retriever, err := retriever.NewRetriever(ctx, retrieverCfg, retrievalClient, cfg.Finder)
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

func WithTimeout(timeout time.Duration) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.Timeout = timeout
	}
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
		fmt.Println()
		return retrievalId, nil, err
	}

	return retrievalId, stats, nil
}

func (l *Lassie) RegisterSubscriber(subscriber types.RetrievalEventSubscriber) func() {
	return l.retriever.RegisterSubscriber(subscriber)
}
