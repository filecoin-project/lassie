package types

import (
	_ "embed"
	"fmt"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	paychtypes "github.com/filecoin-project/go-state-types/builtin/v8/paych"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	bindnoderegistry "github.com/ipld/go-ipld-prime/node/bindnode/registry"
	"golang.org/x/xerrors"
)

//go:generate cbor-gen-for --map-encoding Query QueryResponse DealProposal DealResponse Params QueryParams DealPayment

//go:embed markets.ipldsch
var embedSchema []byte

type DealStatus uint64

const (
	// DealStatusNew is a deal that nothing has happened with yet
	DealStatusNew DealStatus = iota

	// DealStatusUnsealing means the provider is unsealing data
	DealStatusUnsealing

	// DealStatusUnsealed means the provider has finished unsealing data
	DealStatusUnsealed

	// DealStatusWaitForAcceptance means we're waiting to hear back if the provider accepted our deal
	DealStatusWaitForAcceptance

	// DealStatusPaymentChannelCreating is the status set while waiting for the
	// payment channel creation to complete
	DealStatusPaymentChannelCreating

	// DealStatusPaymentChannelAddingFunds is the status when we are waiting for funds
	// to finish being sent to the payment channel
	DealStatusPaymentChannelAddingFunds

	// DealStatusAccepted means a deal has been accepted by a provider
	// and its is ready to proceed with retrieval
	DealStatusAccepted

	// DealStatusFundsNeededUnseal means a deal has been accepted by a provider
	// and payment is needed to unseal the data
	DealStatusFundsNeededUnseal

	// DealStatusFailing indicates something went wrong during a retrieval,
	// and we are cleaning up before terminating with an error
	DealStatusFailing

	// DealStatusRejected indicates the provider rejected a client's deal proposal
	// for some reason
	DealStatusRejected

	// DealStatusFundsNeeded indicates the provider needs a payment voucher to
	// continue processing the deal
	DealStatusFundsNeeded

	// DealStatusSendFunds indicates the client is now going to send funds because we reached the threshold of the last payment
	DealStatusSendFunds

	// DealStatusSendFundsLastPayment indicates the client is now going to send final funds because
	// we reached the threshold of the final payment
	DealStatusSendFundsLastPayment

	// DealStatusOngoing indicates the provider is continuing to process a deal
	DealStatusOngoing

	// DealStatusFundsNeededLastPayment indicates the provider needs a payment voucher
	// in order to complete a deal
	DealStatusFundsNeededLastPayment

	// DealStatusCompleted indicates a deal is complete
	DealStatusCompleted

	// DealStatusDealNotFound indicates an update was received for a deal that could
	// not be identified
	DealStatusDealNotFound

	// DealStatusErrored indicates a deal has terminated in an error
	DealStatusErrored

	// DealStatusBlocksComplete indicates that all blocks have been processed for the piece
	DealStatusBlocksComplete

	// DealStatusFinalizing means the last payment has been received and
	// we are just confirming the deal is complete
	DealStatusFinalizing

	// DealStatusCompleting is just an inbetween state to perform final cleanup of
	// complete deals
	DealStatusCompleting

	// DealStatusCheckComplete is used for when the provided completes without a last payment
	// requested cycle, to verify we have received all blocks
	DealStatusCheckComplete

	// DealStatusCheckFunds means we are looking at the state of funding for the channel to determine
	// if more money is incoming
	DealStatusCheckFunds

	// DealStatusInsufficientFunds indicates we have depleted funds for the retrieval payment channel
	// - we can resume after funds are added
	DealStatusInsufficientFunds

	// DealStatusPaymentChannelAllocatingLane is the status when we are making a lane for this channel
	DealStatusPaymentChannelAllocatingLane

	// DealStatusCancelling means we are cancelling an inprogress deal
	DealStatusCancelling

	// DealStatusCancelled means a deal has been cancelled
	DealStatusCancelled

	// DealStatusRetryLegacy means we're attempting the deal proposal for a second time using the legacy datatype
	DealStatusRetryLegacy

	// DealStatusWaitForAcceptanceLegacy means we're waiting to hear the results on the legacy protocol
	DealStatusWaitForAcceptanceLegacy

	// DealStatusClientWaitingForLastBlocks means that the provider has told
	// the client that all blocks were sent for the deal, and the client is
	// waiting for the last blocks to arrive. This should only happen when
	// the deal price per byte is zero (if it's not zero the provider asks
	// for final payment after sending the last blocks).
	DealStatusClientWaitingForLastBlocks

	// DealStatusPaymentChannelAddingInitialFunds means that a payment channel
	// exists from an earlier deal between client and provider, but we need
	// to add funds to the channel for this particular deal
	DealStatusPaymentChannelAddingInitialFunds

	// DealStatusErroring means that there was an error and we need to
	// do some cleanup before moving to the error state
	DealStatusErroring

	// DealStatusRejecting means that the deal was rejected and we need to do
	// some cleanup before moving to the rejected state
	DealStatusRejecting

	// DealStatusDealNotFoundCleanup means that the deal was not found and we
	// need to do some cleanup before moving to the not found state
	DealStatusDealNotFoundCleanup

	// DealStatusFinalizingBlockstore means that all blocks have been received,
	// and the blockstore is being finalized
	DealStatusFinalizingBlockstore
)

// DealStatuses maps deal status to a human readable representation
var DealStatuses = map[DealStatus]string{
	DealStatusNew:                              "DealStatusNew",
	DealStatusUnsealing:                        "DealStatusUnsealing",
	DealStatusUnsealed:                         "DealStatusUnsealed",
	DealStatusWaitForAcceptance:                "DealStatusWaitForAcceptance",
	DealStatusPaymentChannelCreating:           "DealStatusPaymentChannelCreating",
	DealStatusPaymentChannelAddingFunds:        "DealStatusPaymentChannelAddingFunds",
	DealStatusAccepted:                         "DealStatusAccepted",
	DealStatusFundsNeededUnseal:                "DealStatusFundsNeededUnseal",
	DealStatusFailing:                          "DealStatusFailing",
	DealStatusRejected:                         "DealStatusRejected",
	DealStatusFundsNeeded:                      "DealStatusFundsNeeded",
	DealStatusSendFunds:                        "DealStatusSendFunds",
	DealStatusSendFundsLastPayment:             "DealStatusSendFundsLastPayment",
	DealStatusOngoing:                          "DealStatusOngoing",
	DealStatusFundsNeededLastPayment:           "DealStatusFundsNeededLastPayment",
	DealStatusCompleted:                        "DealStatusCompleted",
	DealStatusDealNotFound:                     "DealStatusDealNotFound",
	DealStatusErrored:                          "DealStatusErrored",
	DealStatusBlocksComplete:                   "DealStatusBlocksComplete",
	DealStatusFinalizing:                       "DealStatusFinalizing",
	DealStatusCompleting:                       "DealStatusCompleting",
	DealStatusCheckComplete:                    "DealStatusCheckComplete",
	DealStatusCheckFunds:                       "DealStatusCheckFunds",
	DealStatusInsufficientFunds:                "DealStatusInsufficientFunds",
	DealStatusPaymentChannelAllocatingLane:     "DealStatusPaymentChannelAllocatingLane",
	DealStatusCancelling:                       "DealStatusCancelling",
	DealStatusCancelled:                        "DealStatusCancelled",
	DealStatusRetryLegacy:                      "DealStatusRetryLegacy",
	DealStatusWaitForAcceptanceLegacy:          "DealStatusWaitForAcceptanceLegacy",
	DealStatusClientWaitingForLastBlocks:       "DealStatusWaitingForLastBlocks",
	DealStatusPaymentChannelAddingInitialFunds: "DealStatusPaymentChannelAddingInitialFunds",
	DealStatusErroring:                         "DealStatusErroring",
	DealStatusRejecting:                        "DealStatusRejecting",
	DealStatusDealNotFoundCleanup:              "DealStatusDealNotFoundCleanup",
	DealStatusFinalizingBlockstore:             "DealStatusFinalizingBlockstore",
}

func (s DealStatus) String() string {
	str, ok := DealStatuses[s]
	if ok {
		return str
	}
	return fmt.Sprintf("DealStatusUnknown - %d", s)
}

// QueryResponseStatus indicates whether a queried piece is available
type QueryResponseStatus uint64

const (
	// QueryResponseAvailable indicates a provider has a piece and is prepared to
	// return it
	QueryResponseAvailable QueryResponseStatus = iota

	// QueryResponseUnavailable indicates a provider either does not have or cannot
	// serve the queried piece to the client
	QueryResponseUnavailable

	// QueryResponseError indicates something went wrong generating a query response
	QueryResponseError
)

// QueryItemStatus (V1) indicates whether the requested part of a piece (payload or selector)
// is available for retrieval
type QueryItemStatus uint64

const (
	// QueryItemAvailable indicates requested part of the piece is available to be
	// served
	QueryItemAvailable QueryItemStatus = iota

	// QueryItemUnavailable indicates the piece either does not contain the requested
	// item or it cannot be served
	QueryItemUnavailable

	// QueryItemUnknown indicates the provider cannot determine if the given item
	// is part of the requested piece (for example, if the piece is sealed and the
	// miner does not maintain a payload CID index)
	QueryItemUnknown
)

// QueryParams - V1 - indicate what specific information about a piece that a retrieval
// client is interested in, as well as specific parameters the client is seeking
// for the retrieval deal
type QueryParams struct {
	PieceCID *cid.Cid // optional, query if miner has this cid in this piece. some miners may not be able to respond.
	//Selector                   datamodel.Node // optional, query if miner has this cid in this piece. some miners may not be able to respond.
	//MaxPricePerByte            abi.TokenAmount    // optional, tell miner uninterested if more expensive than this
	//MinPaymentInterval         uint64    // optional, tell miner uninterested unless payment interval is greater than this
	//MinPaymentIntervalIncrease uint64    // optional, tell miner uninterested unless payment interval increase is greater than this
}

// Query is a query to a given provider to determine information about a piece
// they may have available for retrieval
type Query struct {
	PayloadCID  cid.Cid // V0
	QueryParams         // V1
}

// QueryUndefined is a query with no values
var QueryUndefined = Query{}

// NewQueryV1 creates a V1 query (which has an optional pieceCID)
func NewQueryV1(payloadCID cid.Cid, pieceCID *cid.Cid) Query {
	return Query{
		PayloadCID: payloadCID,
		QueryParams: QueryParams{
			PieceCID: pieceCID,
		},
	}
}

// QueryResponse is a miners response to a given retrieval query
type QueryResponse struct {
	Status        QueryResponseStatus
	PieceCIDFound QueryItemStatus // V1 - if a PieceCID was requested, the result
	//SelectorFound   QueryItemStatus // V1 - if a Selector was requested, the result

	Size uint64 // Total size of piece in bytes
	//ExpectedPayloadSize uint64 // V1 - optional, if PayloadCID + selector are specified and miner knows, can offer an expected size

	PaymentAddress             address.Address // address to send funds to -- may be different than miner addr
	MinPricePerByte            abi.TokenAmount
	MaxPaymentInterval         uint64
	MaxPaymentIntervalIncrease uint64
	Message                    string
	UnsealPrice                abi.TokenAmount
}

// QueryResponseUndefined is an empty QueryResponse
var QueryResponseUndefined = QueryResponse{}

// PieceRetrievalPrice is the total price to retrieve the piece (size * MinPricePerByte + UnsealedPrice)
func (qr QueryResponse) PieceRetrievalPrice() abi.TokenAmount {
	return big.Add(big.Mul(qr.MinPricePerByte, abi.NewTokenAmount(int64(qr.Size))), qr.UnsealPrice)
}

// Params are the parameters requested for a retrieval deal proposal
type Params struct {
	Selector                CborGenCompatibleNode // V1
	PieceCID                *cid.Cid
	PricePerByte            abi.TokenAmount
	PaymentInterval         uint64 // when to request payment
	PaymentIntervalIncrease uint64
	UnsealPrice             abi.TokenAmount
}

// paramsBindnodeOptions is the bindnode options required to convert custom
// types used by the Param type
var paramsBindnodeOptions = []bindnode.Option{
	CborGenCompatibleNodeBindnodeOption,
	TokenAmountBindnodeOption,
}

func (p Params) SelectorSpecified() bool {
	return !p.Selector.IsNull()
}

func (p Params) IntervalLowerBound(currentInterval uint64) uint64 {
	intervalSize := p.PaymentInterval
	var lowerBound uint64
	var target uint64
	for target <= currentInterval {
		lowerBound = target
		target += intervalSize
		intervalSize += p.PaymentIntervalIncrease
	}
	return lowerBound
}

// OutstandingBalance produces the amount owed based on the deal params
// for the given transfer state and funds received
func (p Params) OutstandingBalance(fundsReceived abi.TokenAmount, sent uint64, inFinalization bool) big.Int {
	// Check if the payment covers unsealing
	if fundsReceived.LessThan(p.UnsealPrice) {
		return big.Sub(p.UnsealPrice, fundsReceived)
	}

	// if unsealing funds are received and the retrieval is free, proceed
	if p.PricePerByte.IsZero() {
		return big.Zero()
	}

	// Calculate how much payment has been made for transferred data
	transferPayment := big.Sub(fundsReceived, p.UnsealPrice)

	// The provider sends data and the client sends payment for the data.
	// The provider will send a limited amount of extra data before receiving
	// payment. Given the current limit, check if the client has paid enough
	// to unlock the next interval.
	minimumBytesToPay := sent // for last payment, we need to get past zero
	if !inFinalization {
		minimumBytesToPay = p.IntervalLowerBound(sent)
	}

	// Calculate the minimum required payment
	totalPaymentRequired := big.Mul(big.NewInt(int64(minimumBytesToPay)), p.PricePerByte)

	// Calculate payment owed
	owed := big.Sub(totalPaymentRequired, transferPayment)
	if owed.LessThan(big.Zero()) {
		return big.Zero()
	}
	return owed
}

// NextInterval produces the maximum data that can be transferred before more
// payment is request
func (p Params) NextInterval(fundsReceived abi.TokenAmount) uint64 {
	if p.PricePerByte.NilOrZero() {
		return 0
	}
	currentInterval := uint64(0)
	bytesPaid := fundsReceived
	if !p.UnsealPrice.NilOrZero() {
		bytesPaid = big.Sub(bytesPaid, p.UnsealPrice)
	}
	bytesPaid = big.Div(bytesPaid, p.PricePerByte)
	if bytesPaid.GreaterThan(big.Zero()) {
		currentInterval = bytesPaid.Uint64()
	}
	return p.nextInterval(currentInterval)
}

func (p Params) nextInterval(currentInterval uint64) uint64 {
	intervalSize := p.PaymentInterval
	var nextInterval uint64
	for nextInterval <= currentInterval {
		nextInterval += intervalSize
		intervalSize += p.PaymentIntervalIncrease
	}
	return nextInterval
}

// NewParamsV1 generates parameters for a retrieval deal, including a selector
func NewParamsV1(pricePerByte abi.TokenAmount, paymentInterval uint64, paymentIntervalIncrease uint64, sel datamodel.Node, pieceCid *cid.Cid, unsealPrice abi.TokenAmount) (Params, error) {
	if sel == nil {
		return Params{}, xerrors.New("selector required for NewParamsV1")
	}

	return Params{
		Selector:                CborGenCompatibleNode{Node: sel},
		PieceCID:                pieceCid,
		PricePerByte:            pricePerByte,
		PaymentInterval:         paymentInterval,
		PaymentIntervalIncrease: paymentIntervalIncrease,
		UnsealPrice:             unsealPrice,
	}, nil
}

// DealID is an identifier for a retrieval deal (unique to a client)
type DealID uint64

func (d DealID) String() string {
	return fmt.Sprintf("%d", d)
}

// DealProposal is a proposal for a new retrieval deal
type DealProposal struct {
	PayloadCID cid.Cid
	ID         DealID
	Params
}

// DealProposalType is the DealProposal voucher type
const DealProposalType = datatransfer.TypeIdentifier("RetrievalDealProposal/1")

// dealProposalBindnodeOptions is the bindnode options required to convert
// custom types used by the DealProposal type; the only custom types involved
// are for Params so we can reuse those options.
var dealProposalBindnodeOptions = paramsBindnodeOptions

func DealProposalFromNode(node datamodel.Node) (*DealProposal, error) {
	if node == nil {
		return nil, fmt.Errorf("empty voucher")
	}
	dpIface, err := BindnodeRegistry.TypeFromNode(node, &DealProposal{})
	if err != nil {
		return nil, fmt.Errorf("invalid DealProposal: %w", err)
	}
	dp, _ := dpIface.(*DealProposal) // safe to assume type
	return dp, nil
}

// DealProposalUndefined is an undefined deal proposal
var DealProposalUndefined = DealProposal{}

// DealResponse is a response to a retrieval deal proposal
type DealResponse struct {
	Status DealStatus
	ID     DealID

	// payment required to proceed
	PaymentOwed abi.TokenAmount

	Message string
}

// DealResponseType is the DealResponse usable as a voucher type
const DealResponseType = datatransfer.TypeIdentifier("RetrievalDealResponse/1")

// dealResponseBindnodeOptions is the bindnode options required to convert custom
// types used by the DealResponse type
var dealResponseBindnodeOptions = []bindnode.Option{TokenAmountBindnodeOption}

// DealResponseUndefined is an undefined deal response
var DealResponseUndefined = DealResponse{}

func DealResponseFromNode(node datamodel.Node) (*DealResponse, error) {
	if node == nil {
		return nil, fmt.Errorf("empty voucher")
	}
	dpIface, err := BindnodeRegistry.TypeFromNode(node, &DealResponse{})
	if err != nil {
		return nil, fmt.Errorf("invalid DealResponse: %w", err)
	}
	dp, _ := dpIface.(*DealResponse) // safe to assume type
	return dp, nil
}

// DealPayment is a payment for an in progress retrieval deal
type DealPayment struct {
	ID             DealID
	PaymentChannel address.Address
	PaymentVoucher *paychtypes.SignedVoucher
}

// DealPaymentType is the DealPayment voucher type
const DealPaymentType = datatransfer.TypeIdentifier("RetrievalDealPayment/1")

// dealPaymentBindnodeOptions is the bindnode options required to convert custom
// types used by the DealPayment type
var dealPaymentBindnodeOptions = []bindnode.Option{
	SignatureBindnodeOption,
	AddressBindnodeOption,
	BigIntBindnodeOption,
	TokenAmountBindnodeOption,
}

// DealPaymentUndefined is an undefined deal payment
var DealPaymentUndefined = DealPayment{}

func DealPaymentFromNode(node datamodel.Node) (*DealPayment, error) {
	if node == nil {
		return nil, fmt.Errorf("empty voucher")
	}
	dpIface, err := BindnodeRegistry.TypeFromNode(node, &DealPayment{})
	if err != nil {
		return nil, fmt.Errorf("invalid DealPayment: %w", err)
	}
	dp, _ := dpIface.(*DealPayment) // safe to assume type
	return dp, nil
}

var BindnodeRegistry = bindnoderegistry.NewRegistry()

func init() {
	for _, r := range []struct {
		typ     interface{}
		typName string
		opts    []bindnode.Option
	}{
		{(*Params)(nil), "Params", paramsBindnodeOptions},
		{(*DealProposal)(nil), "DealProposal", dealProposalBindnodeOptions},
		{(*DealResponse)(nil), "DealResponse", dealResponseBindnodeOptions},
		{(*DealPayment)(nil), "DealPayment", dealPaymentBindnodeOptions},
	} {
		if err := BindnodeRegistry.RegisterType(r.typ, string(embedSchema), r.typName, r.opts...); err != nil {
			panic(err.Error())
		}
	}
}
