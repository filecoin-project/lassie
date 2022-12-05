package client

import (
	"context"

	address "github.com/filecoin-project/go-address"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin/v8/paych"
)

type PayChannelManager interface {
	// lotus > paychmgr > store.go |
	// Allocates a new lane for a given channel
	AllocateLane(ctx context.Context, address address.Address) (uint64, error)

	// lotus > paychmgr > paych.go |
	// createVoucher creates a voucher with the given specification, setting its
	// nonce, signing the voucher and storing it in the local datastore.
	// If there are not enough funds in the channel to create the voucher, returns
	// the shortfall in funds.
	CreateVoucher(ctx context.Context, ch address.Address, voucher paych.SignedVoucher) (*paych.SignedVoucher, big.Int, error)

	// Creates a payment channel with the minimum number of funds required for the retrieval
	GetPayChannelWithMinFunds(ctx context.Context, dest address.Address) (address.Address, error)

	// lotus > paychmgr > manager.go |
	// Restarts tracking of any messages that were sent to chain.
	//
	// lotus > paychmgr > simple.go |
	// restartPending checks the datastore to see if there are any channels that
	// have outstanding create / add funds messages, and if so, waits on the
	// messages.
	// Outstanding messages can occur if a create / add funds message was sent and
	// then the system was shut down or crashed before the result was received.
	Start() error
}
