package eventrecorder

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/application-research/filclient"
	"github.com/application-research/filclient/rep"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/libp2p/go-libp2p-core/peer"
)

func NewEventRecorder(instanceId string, endpointURL string) *EventRecorder {
	return &EventRecorder{instanceId, endpointURL}
}

type EventRecorder struct {
	instanceId  string
	endpointURL string
}

var eventSchema string = `
type event struct {
	instanceId             String # Set in the autoretrieve config
	cid                    Link   # The CID that was originally requested
	rootCid       optional Link   # The CID that we are retrieving _if_ it's different to the originally requested CID (only set on "start" event)
	time                   String # The timestamp of this event
	stage                  String # The stage of this event (see filclient/rep/RetrievalEventCode - plus a custom "start" event)

	# For a successful retrieval
	size          optional Int    # Size in bytes of the transfer
	askPrice      optional String # The asking price from the storage provider
	totalPayment  optional String # The total payment that was transferred to the miner
	numPayments   optional Int    # The number of separate payments made during the retrieval

	# For an unsuccessful retrieval
	error         optional String # The full error message
}
`

// TODO: use time.Time and abi.TokenAmount and use bindnode converters to convert to strings
// when we can upgrade to >=go-ipld-prime@v0.17.0
type event struct {
	InstanceId   string
	Cid          cid.Cid  // will use dag-json CID style thanks to Cid#MarshalJSON
	RootCid      *cid.Cid // ditto ^
	Time         string   // time.RFC3339Nano format
	Stage        string
	Size         *uint64
	AskPrice     *string // abi.TokenAmount / big.Int
	TotalPayment *string // abi.TokenAmount / big.Int
	NumPayments  *uint
	Error        *string
}

var eventType schema.Type

// RecordSuccess PUTs a notification of the retrieval to
// http://.../retrieval-event/~retrievalId~/providers/~storageProviderId. The body of the
// PUT will look something like:
//
// 	{
// 	  "cid":{"/":"bafybeihrqe2hmfauph5yfbd6ucv7njqpiy4tvbewlvhzjl4bhnyiu6h7pm"},
// 	  "time":"2022-07-12T19:57:53.112375079+10:00",
// 	  "size":2020,
// 	  "askPrice":"3030",
// 	  "totalPayment":"4040",
// 	  "numPayments":2
// 	}

func (er *EventRecorder) RecordStart(
	retrievalId uuid.UUID,
	requestCid cid.Cid,
	rootCid cid.Cid,
	storageProviderId peer.ID,
	startTime time.Time, // the only event where we take a time, so we can match logs & other metrics
) error {

	var rcid *cid.Cid
	if !requestCid.Equals(rootCid) {
		rcid = &rootCid
	}

	evt := event{
		InstanceId: er.instanceId,
		Stage:      "start",
		Cid:        requestCid,
		RootCid:    rcid,
		Time:       startTime.Format(time.RFC3339Nano),
	}

	return er.recordEvent(retrievalId, storageProviderId, evt)
}

func (er *EventRecorder) RecordProgress(
	retrievalId uuid.UUID,
	requestCid cid.Cid,
	storageProviderId peer.ID,
	stage rep.RetrievalEventCode,
) error {

	evt := event{
		InstanceId: er.instanceId,
		Stage:      string(stage),
		Cid:        requestCid,
		Time:       time.Now().Format(time.RFC3339Nano),
	}

	return er.recordEvent(retrievalId, storageProviderId, evt)
}

func (er *EventRecorder) RecordSuccess(
	retrievalId uuid.UUID,
	requestCid cid.Cid,
	storageProviderId peer.ID,
	retrievalStats *filclient.RetrievalStats,
) error {

	ap := retrievalStats.AskPrice.String()
	tp := retrievalStats.TotalPayment.String()
	np := uint(retrievalStats.NumPayments)
	evt := event{
		InstanceId:   er.instanceId,
		Stage:        string(rep.RetrievalEventSuccess),
		Cid:          requestCid,
		Time:         time.Now().Format(time.RFC3339Nano),
		Size:         &retrievalStats.Size,
		AskPrice:     &ap,
		TotalPayment: &tp,
		NumPayments:  &np,
	}

	return er.recordEvent(retrievalId, storageProviderId, evt)
}

// RecordFailure PUTs a notification of the retrieval failure to
// http://.../retrieval-event/~retrievalId~/providers/~storageProviderId. In this case,
// most of the body is omitted but an "error" field is present with the error
// message.
func (er *EventRecorder) RecordFailure(
	retrievalId uuid.UUID,
	requestCid cid.Cid,
	storageProviderId peer.ID,
	retrievalError error) error {

	es := retrievalError.Error()
	evt := event{
		InstanceId: er.instanceId,
		Stage:      string(rep.RetrievalEventFailure),
		Cid:        requestCid,
		Time:       time.Now().Format(time.RFC3339Nano),
		Error:      &es,
	}
	return er.recordEvent(retrievalId, storageProviderId, evt)
}

func (er *EventRecorder) recordEvent(retrievalId uuid.UUID, storageProviderId peer.ID, evt event) error {
	// https://.../retrieval-event/~uuid~/providers/~peerID~
	url := fmt.Sprintf("%s/retrieval-event/%s/providers/%s", er.endpointURL, retrievalId.String(), storageProviderId.String())

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
