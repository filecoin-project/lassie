package retriever

import (
	"testing"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"
)

func TestRetrievalProposalForAsk(t *testing.T) {
	cid1 := cid.MustParse("bafkqaalb")
	ask := &retrievalmarket.QueryResponse{
		MinPricePerByte:            abi.NewTokenAmount(1),
		MaxPaymentInterval:         2,
		MaxPaymentIntervalIncrease: 3,
		UnsealPrice:                abi.NewTokenAmount(4),
	}
	proposal, err := RetrievalProposalForAsk(ask, cid1, nil)
	require.NoError(t, err)
	require.NotZero(t, proposal.ID)
	require.Equal(t, cid1, proposal.PayloadCID)
	require.Same(t, selectorparse.CommonSelector_ExploreAllRecursively, proposal.Params.Selector.Node)
	require.Nil(t, proposal.Params.PieceCID)
	require.Equal(t, abi.NewTokenAmount(1), proposal.Params.PricePerByte)
	require.Equal(t, uint64(2), proposal.Params.PaymentInterval)
	require.Equal(t, uint64(3), proposal.Params.PaymentIntervalIncrease)
	require.Equal(t, abi.NewTokenAmount(4), proposal.Params.UnsealPrice)

	proposal, err = RetrievalProposalForAsk(ask, cid1, selectorparse.CommonSelector_MatchPoint)
	require.NoError(t, err)
	require.NotZero(t, proposal.ID)
	require.Equal(t, cid1, proposal.PayloadCID)
	require.Same(t, selectorparse.CommonSelector_MatchPoint, proposal.Params.Selector.Node)
	require.Nil(t, proposal.Params.PieceCID)
	require.Equal(t, abi.NewTokenAmount(1), proposal.Params.PricePerByte)
	require.Equal(t, uint64(2), proposal.Params.PaymentInterval)
	require.Equal(t, uint64(3), proposal.Params.PaymentIntervalIncrease)
	require.Equal(t, abi.NewTokenAmount(4), proposal.Params.UnsealPrice)
}
