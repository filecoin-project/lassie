package retriever_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/retriever/testutil"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

func TestBitswapRetriever(t *testing.T) {
	ctx := context.Background()
	cid1 := testutil.GenerateCid()
	cid2 := testutil.GenerateCid()

	testCases := []struct {
		name             string
		localLinkSystem  map[cid.Cid]*linking.LinkSystem
		remoteLinkSystem map[cid.Cid]*linking.LinkSystem
		expectedEvents   map[cid.Cid][]types.EventCode
		expectedStats    map[cid.Cid]*types.RetrievalStats
		expectedErr      map[cid.Cid]error
	}{}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			req := require.New(t)
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			linkSystemForCid := func(c cid.Cid, lsMap map[cid.Cid]*linking.LinkSystem) *linking.LinkSystem {
				mockLinkSystem := cidlink.DefaultLinkSystem()
				mockLinkSystem.StorageReadOpener = func(linking.LinkContext, datamodel.Link) (io.Reader, error) {
					return nil, errors.New("not implemented")
				}
				mockLinkSystem.StorageWriteOpener = func(linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
					return nil, nil, errors.New("not implemented")
				}
				if lsMap == nil {
					return &mockLinkSystem
				}
				if lsys, ok := lsMap[c]; ok {
					return lsys
				}
				return &mockLinkSystem
			}
			rid1, err := types.NewRetrievalID()
			req.NoError(err)
			_ = types.RegisterRetrievalIDToContext(ctx, rid1)
			_ = types.RetrievalRequest{
				RetrievalID: rid1,
				Cid:         cid1,
				LinkSystem:  *linkSystemForCid(cid1, testCase.localLinkSystem),
			}
			rid2, err := types.NewRetrievalID()
			req.NoError(err)
			_ = types.RegisterRetrievalIDToContext(ctx, rid2)
			_ = types.RetrievalRequest{
				RetrievalID: rid2,
				Cid:         cid2,
				LinkSystem:  *linkSystemForCid(cid2, testCase.localLinkSystem),
			}

		})
	}
}

type mockExchange struct {
	getLsys func(ctx context.Context) (*linking.LinkSystem, error)
}

// GetBlock returns the block associated with a given key.
func (me *mockExchange) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	lsys, err := me.getLsys(ctx)
	if err != nil {
		return nil, err
	}
	r, err := lsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return blocks.NewBlockWithCid(data, c)
}

func (me *mockExchange) GetBlocks(_ context.Context, _ []cid.Cid) (<-chan blocks.Block, error) {
	panic("not implemented") // TODO: Implement
}

// NotifyNewBlocks tells the exchange that new blocks are available and can be served.
func (me *mockExchange) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	panic("not implemented") // TODO: Implement
}

func (me *mockExchange) Close() error {
	panic("not implemented") // TODO: Implement
}

func (me *mockExchange) NewSession(_ context.Context) exchange.Fetcher {
	return me
}
