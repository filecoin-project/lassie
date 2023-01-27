package retriever

import (
	"sync/atomic"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
)

// timeCounter is used to generate a monotonically increasing sequence.
// It starts at the current time, then increments on each call to next.
type TimeCounter struct {
	counter uint64
}

func NewTimeCounter() *TimeCounter {
	return &TimeCounter{counter: uint64(time.Now().UnixNano())}
}

func (tc *TimeCounter) Next() uint64 {
	counter := atomic.AddUint64(&tc.counter, 1)
	return counter
}

var dealIdGen = NewTimeCounter()

func RetrievalProposalForAsk(ask *types.QueryResponse, c cid.Cid, optionalSelector ipld.Node) (*types.DealProposal, error) {
	if optionalSelector == nil {
		optionalSelector = selectorparse.CommonSelector_ExploreAllRecursively
	}

	params, err := types.NewParamsV1(
		ask.MinPricePerByte,
		ask.MaxPaymentInterval,
		ask.MaxPaymentIntervalIncrease,
		optionalSelector,
		nil,
		ask.UnsealPrice,
	)
	if err != nil {
		return nil, err
	}
	return &types.DealProposal{
		PayloadCID: c,
		ID:         types.DealID(dealIdGen.Next()),
		Params:     params,
	}, nil
}
