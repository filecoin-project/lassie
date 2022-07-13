package eventrecorder

import (
	"bytes"
	"fmt"
	"net/http"
	"time"

	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	bnregistry "github.com/ipld/go-ipld-prime/node/bindnode/registry"
	"github.com/libp2p/go-libp2p-core/peer"
)

func NewEventRecorder(endpointURL string) *EventRecorder {
	return &EventRecorder{endpointURL: endpointURL}
}

type EventRecorder struct {
	endpointURL string
}

var eventSchema string = `
type Event struct {
	cid                   &Any
	rootCid      optional &Any
	startTime             String
	durationMs   optional Int
	size         optional Int
	askPrice     optional String
	totalPayment optional Int
	numPayments  optional Int
	error        optional String
}
`

type event struct {
	Cid          cid.Cid         `json:"cid"`               // will use dag-json CID style thanks to Cid#MarshalJSON
	RootCid      *cid.Cid        `json:"rootCid,omitempty"` // ditto ^
	StartTime    time.Time       `json:"startTime"`         // will use time.RFC3339Nano format
	DurationMs   uint64          `json:"durationMs,omitempty"`
	Size         uint64          `json:"size,omitempty"`
	AskPrice     abi.TokenAmount `json:"askPrice,omitempty"`
	TotalPayment abi.TokenAmount `json:"totalPayment,omitempty"`
	NumPayments  uint            `json:"numPayments,omitempty"`
	Error        string          `json:"error,omitempty"`
}

var bindnodeRegistry bnregistry.BindnodeRegistry
var timeConverter bindnode.Option = bindnode.TypedStringConverter(
	(*time.Time)(nil),
	func(string) (interface{}, error) {
		panic("conversion from String to time.Time not supported")
	},
	func(ti interface{}) (string, error) {
		return ti.(*time.Time).Format(time.RFC3339Nano), nil
	},
)
var tokenAmountConverter bindnode.Option = bindnode.TypedStringConverter(
	(*abi.TokenAmount)(nil),
	func(string) (interface{}, error) {
		panic("conversion from String to abi.TokenAmount not supported")
	},
	func(tai interface{}) (string, error) {
		return tai.(*abi.TokenAmount).String(), nil
	},
)

func init() {
	bindnodeRegistry = bnregistry.NewRegistry()
	if err := bindnodeRegistry.RegisterType((*event)(nil), eventSchema, "event"); err != nil {
		panic(err)
	}
}

func newEvent(
	requestCid cid.Cid,
	rootCid cid.Cid,
	startTime time.Time,
	retrievalStats *filclient.RetrievalStats,
	retrievalError error,
) event {

	var rcid *cid.Cid
	if !requestCid.Equals(rootCid) {
		rcid = &rootCid
	}

	evt := event{
		Cid:       requestCid,
		RootCid:   rcid,
		StartTime: startTime,
	}
	if retrievalStats != nil {
		evt.DurationMs = uint64(retrievalStats.Duration.Milliseconds())
		evt.Size = retrievalStats.Size
		evt.AskPrice = retrievalStats.AskPrice
		evt.TotalPayment = retrievalStats.TotalPayment
		evt.NumPayments = uint(retrievalStats.NumPayments)
	}
	if retrievalError != nil {
		evt.Error = retrievalError.Error()
	}
	return evt
}

// RecordSuccess PUTs a notification of the retrieval to
// http://.../retrieval-event/~retrievalId~/providers/~minerId. The body of the
// PUT will look something like:
//
// 	{
// 	  "cid":{"/":"bafybeihrqe2hmfauph5yfbd6ucv7njqpiy4tvbewlvhzjl4bhnyiu6h7pm"},
// 	  "startTime":"2022-07-12T19:57:53.112375079+10:00",
// 	  "size":2020,
// 	  "askPrice":"3030",
// 	  "totalPayment":"4040",
// 	  "numPayments":2
// 	}
func (er *EventRecorder) RecordSuccess(
	retrievalId uuid.UUID,
	cid cid.Cid,
	rootCid cid.Cid,
	startTime time.Time,
	retrievalStats *filclient.RetrievalStats) error {

	return er.recordEvent(retrievalId, retrievalStats.Peer, newEvent(cid, rootCid, startTime, retrievalStats, nil))
}

// RecordFailure PUTs a notification of the retrieval failure to
// http://.../retrieval-event/~retrievalId~/providers/~minerId. In this case,
// most of the body is omitted but an "error" field is present with the error
// message.
func (er *EventRecorder) RecordFailure(
	retrievalId uuid.UUID,
	minerId peer.ID,
	cid cid.Cid,
	rootCid cid.Cid,
	startTime time.Time,
	retrievalError error) error {

	return er.recordEvent(retrievalId, minerId, newEvent(cid, rootCid, startTime, nil, retrievalError))
}

func (er *EventRecorder) recordEvent(retrievalId uuid.UUID, minerId peer.ID, evt event) error {
	// https://.../event/~uuid~/providers/~peerID
	url := fmt.Sprintf("%s/retrieval-event/%s/providers/%s", er.endpointURL, retrievalId.String(), minerId.String())

	byts, err := bindnodeRegistry.TypeToBytes(&evt, dagjson.Encode)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("PUT", url, bytes.NewReader(byts))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close() // error not so important at this point
	return nil
}
