package retriever

import (
	"sync/atomic"
	"time"

	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
)

var dealIdGen = NewTimeCounter()

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

func RetrievalProposalForAsk(ask *retrievaltypes.QueryResponse, c cid.Cid, optionalSelector ipld.Node) (*retrievaltypes.DealProposal, error) {
	if optionalSelector == nil {
		optionalSelector = selectorparse.CommonSelector_ExploreAllRecursively
	}

	params, err := retrievaltypes.NewParamsV1(
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
	return &retrievaltypes.DealProposal{
		PayloadCID: c,
		ID:         retrievaltypes.DealID(dealIdGen.Next()),
		Params:     params,
	}, nil
}
