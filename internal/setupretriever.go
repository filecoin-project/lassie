package internal

import (
	"context"
	"time"

	"github.com/filecoin-project/lassie/pkg/client"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	"github.com/multiformats/go-multiaddr"
)

func SetupRetriever(ctx context.Context, timeout time.Duration) (*retriever.Retriever, error) {
	return SetupRetrieverWithFinder(ctx, timeout, indexerlookup.NewCandidateFinder("https://cid.contact"))
}

func SetupRetrieverWithFinder(ctx context.Context, timeout time.Duration, finder retriever.CandidateFinder) (*retriever.Retriever, error) {
	datastore := dss.MutexWrap(datastore.NewMapDatastore())

	host, err := InitHost(ctx, multiaddr.StringCast("/ip4/0.0.0.0/tcp/6746"))
	if err != nil {
		return nil, err
	}

	retrievalClient, err := client.NewClient(datastore, host, nil)
	if err != nil {
		return nil, err
	}

	indexer := indexerlookup.NewCandidateFinder("https://cid.contact")

	retrieverCfg := retriever.RetrieverConfig{
		DefaultMinerConfig: retriever.MinerConfig{
			RetrievalTimeout: timeout,
		},
	}

	ret, err := retriever.NewRetriever(ctx, retrieverCfg, retrievalClient, indexer)
	if err != nil {
		return nil, err
	}
	ret.Start()
	return ret, nil
}
