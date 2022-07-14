package eventrecorder

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/application-research/filclient"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/libp2p/go-libp2p-core/peer"
)

func NewEventRecorder(endpointURL string) *EventRecorder {
	return &EventRecorder{endpointURL: endpointURL}
}

type EventRecorder struct {
	endpointURL string
}

var eventSchema string = `
type event struct {
	cid                   Link
	rootCid      optional Link
	startTime             String
	durationMs   optional Int
	size         optional Int
	askPrice     optional String
	totalPayment optional String
	numPayments  optional Int
	error        optional String
}
`

type event struct {
	Cid          cid.Cid  // will use dag-json CID style thanks to Cid#MarshalJSON
	RootCid      *cid.Cid // ditto ^
	StartTime    string   // time.RFC3339Nano format
	DurationMs   *uint64
	Size         *uint64
	AskPrice     *string // abi.TokenAmount / big.Int
	TotalPayment *string // abi.TokenAmount / big.Int
	NumPayments  *uint
	Error        *string
}

var eventType schema.Type

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
		StartTime: startTime.Format(time.RFC3339Nano),
	}
	if retrievalStats != nil {
		dms := uint64(retrievalStats.Duration.Milliseconds())
		evt.DurationMs = &dms
		evt.Size = &retrievalStats.Size
		ap := retrievalStats.AskPrice.String()
		evt.AskPrice = &ap
		tp := retrievalStats.TotalPayment.String()
		evt.TotalPayment = &tp
		np := uint(retrievalStats.NumPayments)
		evt.NumPayments = &np
	}
	if retrievalError != nil {
		es := retrievalError.Error()
		evt.Error = &es
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
	// https://.../retrieval-event/~uuid~/providers/~peerID~
	url := fmt.Sprintf("%s/retrieval-event/%s/providers/%s", er.endpointURL, retrievalId.String(), minerId.String())

	node := bindnode.Wrap(&evt, eventType)
	byts, err := ipld.Encode(node.Representation(), dagjson.Encode)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("PUT", url, bytes.NewBufferString(string(byts)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() // error not so important at this point
	return nil
}

func init() {
	typeSystem, err := ipld.LoadSchemaBytes([]byte(eventSchema))
	if err != nil {
		panic(err)
	}
	eventType = typeSystem.TypeByName("event")
	if eventType == nil {
		panic(errors.New("failed to extract `event` from schema"))
	}
	_ = bindnode.Prototype((*event)(nil), eventType) // check for problems
}
