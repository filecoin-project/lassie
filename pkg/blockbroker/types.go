package blockbroker

import (
	"context"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/linking"
)

type BlockSession interface {
	Get(ctx context.Context, c cid.Cid) (blocks.Block, error)
	GetSubgraph(ctx context.Context, c cid.Cid, lsys linking.LinkSystem) (int, error)
	GetSubgraphStream(ctx context.Context, c cid.Cid) (io.ReadCloser, string, error) // caller must close
	SeedProviders(ctx context.Context, c cid.Cid)
	UsedProviders() []string
	Close() error
}
