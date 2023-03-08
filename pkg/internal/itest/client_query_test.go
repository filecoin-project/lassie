package itest

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-address"
	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lassie/pkg/client"
	"github.com/filecoin-project/lassie/pkg/internal/itest/mocknet"
	"github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

var testQueryResponse = retrievaltypes.QueryResponse{
	Status:                     retrievaltypes.QueryResponseAvailable,
	PieceCIDFound:              retrievaltypes.QueryItemAvailable,
	Size:                       1234,
	PaymentAddress:             address.TestAddress,
	MinPricePerByte:            abi.NewTokenAmount(5678),
	MaxPaymentInterval:         4321,
	MaxPaymentIntervalIncrease: 0,
	Message:                    "yep!",
	UnsealPrice:                abi.NewTokenAmount(0),
}
var testCid1 = cid.MustParse("bafyreibdoxfay27gf4ye3t5a7aa5h4z2azw7hhhz36qrbf5qleldj76qfy")
var testCid2 = cid.MustParse("bafyreibdoxfay27gf4ye3t5a7aa5h4z2azw7hhhz36qrbf5qleldj76qfa")

func TestQuery(t *testing.T) {
	tests := []struct {
		name           string
		expectCid      cid.Cid
		requestCid     cid.Cid
		expectResponse retrievaltypes.QueryResponse
		resultResponse retrievaltypes.QueryResponse
		resultErr      bool
	}{
		{
			name:           "success",
			expectCid:      testCid1,
			requestCid:     testCid1,
			expectResponse: testQueryResponse,
			resultResponse: testQueryResponse,
			resultErr:      false,
		},
		{
			name:           "unavailable",
			expectCid:      testCid1,
			requestCid:     testCid2,
			expectResponse: testQueryResponse,
			resultResponse: retrievaltypes.QueryResponse{
				Status:                     retrievaltypes.QueryResponseUnavailable,
				PieceCIDFound:              retrievaltypes.QueryItemUnavailable,
				Size:                       0,
				PaymentAddress:             address.Address{},
				MinPricePerByte:            abi.NewTokenAmount(0),
				MaxPaymentInterval:         0,
				MaxPaymentIntervalIncrease: 0,
				Message:                    "",
				UnsealPrice:                abi.NewTokenAmount(0),
			},
			resultErr: false,
		},
		{
			name:       "error",
			expectCid:  testCid1,
			requestCid: mocknet.QueryErrorTriggerCid,
			resultErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			mrn := mocknet.NewMockRetrievalNet(ctx, t)
			mrn.AddGraphsyncPeers(1)
			mocknet.SetupQuery(t, mrn.Remotes[0], tt.expectCid, tt.expectResponse)
			mrn.MN.LinkAll()

			ds1 := dss.MutexWrap(datastore.NewMapDatastore())
			client, err := client.NewClient(ds1, mrn.Self, nil)
			require.NoError(t, err)

			var connected bool
			qr, err := client.RetrievalQueryToPeer(ctx, *mrn.Remotes[0].AddrInfo(), tt.requestCid, func() {
				connected = true
			})

			require.True(t, connected)

			if tt.resultErr {
				require.Error(t, err)
				require.Nil(t, qr)
			} else {
				require.NoError(t, err)
				require.NotNil(t, qr)
				require.Equal(t, tt.resultResponse, *qr)
			}
		})
	}
}
