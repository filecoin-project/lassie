package types

import (
	_ "embed"
	"fmt"
	"io"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/bindnode"
)

//TODO: remove this all when it's implemented in a shared go-markets-types repo

//go:embed query.ipldsch
var querySchema []byte

// QueryResponseFromReader reads a QueryResponse object in dag-cbor form from
// a stream
func QueryResponseFromReader(r io.Reader) (*retrievalmarket.QueryResponse, error) {
	dpIface, err := retrievalmarket.BindnodeRegistry.TypeFromReader(r, &retrievalmarket.QueryResponse{}, dagcbor.Decode)
	if err != nil {
		return nil, fmt.Errorf("invalid QueryResponse: %w", err)
	}
	dp, _ := dpIface.(*retrievalmarket.QueryResponse) // safe to assume type
	return dp, nil
}

// QueryToWriter writes a Query object in dag-cbor form to a stream
func QueryToWriter(q *retrievalmarket.Query, w io.Writer) error {
	return retrievalmarket.BindnodeRegistry.TypeToWriter(q, w, dagcbor.Encode)
}

func init() {
	// register into the retrievalmarket bindnode registry, it should be there
	// but hasn't been implemented there yet

	for _, r := range []struct {
		typ     interface{}
		typName string
		opts    []bindnode.Option
	}{
		{(*retrievalmarket.QueryResponse)(nil), "QueryResponse", []bindnode.Option{retrievalmarket.AddressBindnodeOption, retrievalmarket.TokenAmountBindnodeOption}},
		{(*retrievalmarket.Query)(nil), "Query", []bindnode.Option{}},
	} {
		if err := retrievalmarket.BindnodeRegistry.RegisterType(r.typ, string(querySchema), r.typName, r.opts...); err != nil {
			panic(err.Error())
		}
	}
}
