package eventrecorder

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/application-research/autoretrieve/filecoin"
	"github.com/application-research/filclient"
	"github.com/application-research/filclient/rep"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/libp2p/go-libp2p-core/peer"
)

var _ filecoin.RetrievalEventListener = (*EventRecorder)(nil)

var log = logging.Logger("filecoin")

func NewEventRecorder(instanceId string, endpointURL string) *EventRecorder {
	return &EventRecorder{instanceId, endpointURL}
}

type EventRecorder struct {
	instanceId  string
	endpointURL string
}

var eventSchema string = `
type event struct {
	instanceId              String # Set in the autoretrieve config
	cid                     Link   # The CID that was originally requested
	rootCid        optional Link   # The CID that we are retrieving _if_ it's different to the originally requested CID (only set on "start" event)
	time                    String # The timestamp of this event
	stage                   String # The stage of this event (see filclient/rep/RetrievalEventCode - plus a custom "start" event)
	candidateCount optional Int    # For the start of a query, expect this number of query failures or successes

	queryResponse  optional eventQueryResponse    # For a successful query
	success        optional eventRetrievalSuccess # For a successful retrieval
	error          optional String                # The full error message for an unsuccessful query or retrieval
}

type eventRetrievalSuccess struct {
	status                     Int
	pieceCidFound              Int    # if a PieceCID was requested, the result
	size                       Int    # Total size of piece in bytes
	paymentAddress             String # (address.Address) address to send funds to
	minPricePerByte            String # abi.TokenAmount
	maxPaymentInterval         Int
	maxPaymentIntervalIncrease Int
	message                    String
	unsealPrice                String # abi.TokenAmount
}

type eventQueryResponse struct {
	size         Int    # Size in bytes of the transfer
	askPrice     String # The asking price from the storage provider
	totalPayment String # The total payment that was transferred to the miner
	numPayments  Int    # The number of separate payments made during the retrieval
}
`

// TODO: use time.Time and abi.TokenAmount and use bindnode converters to convert to strings
// when we can upgrade to >=go-ipld-prime@v0.17.0
type event struct {
	InstanceId     string
	Cid            cid.Cid  // will use dag-json CID style thanks to Cid#MarshalJSON
	RootCid        *cid.Cid // ditto ^
	Time           string   // time.RFC3339Nano format
	Stage          string
	CandidateCount *int // number of candidates available for querying for this CID
	Success        *eventRetrievalSuccess
	QueryResponse  *eventQueryResponse
	Error          *string
}

type eventRetrievalSuccess struct {
	Size         uint64
	AskPrice     string // abi.TokenAmount / big.Int
	TotalPayment string // abi.TokenAmount / big.Int
	NumPayments  uint
}

type eventQueryResponse struct {
	Status                     uint64
	PieceCIDFound              uint64 // if a PieceCID was requested, the result
	Size                       uint64 // Total size of piece in bytes
	PaymentAddress             string // (address.Address) address to send funds to
	MinPricePerByte            string // abi.TokenAmount
	MaxPaymentInterval         uint64
	MaxPaymentIntervalIncrease uint64
	Message                    string
	UnsealPrice                string // abi.TokenAmount
}

var eventSchemaType schema.Type

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

// RetrievalQueryStart defines the start of the process of querying all
// storage providers that are known to have this CID
func (er *EventRecorder) RetrievalQueryStart(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, candidateCount int) {
	evt := event{
		InstanceId:     er.instanceId,
		Stage:          "start",
		Cid:            requestedCid,
		Time:           timestamp.Format(time.RFC3339Nano),
		CandidateCount: &candidateCount,
	}

	er.recordEvent("RetrievalQueryStart", retrievalId, true, "", evt)
}

// RetrievalQueryProgress events occur during the query process, stages
// include: connect and query-ask
func (er *EventRecorder) RetrievalQueryProgress(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage rep.RetrievalEventCode) {
	evt := event{
		InstanceId: er.instanceId,
		Stage:      string(stage),
		Cid:        requestedCid,
		Time:       time.Now().Format(time.RFC3339Nano),
	}

	er.recordEvent("RetrievalQueryProgress", retrievalId, true, storageProviderId, evt)
}

// RetrievalQueryFailure events occur on the failure of querying a storage
// provider. A query will result in either a RetrievalQueryFailure or
// a RetrievalQuerySuccess event.
func (er *EventRecorder) RetrievalQueryFailure(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, storageProviderId peer.ID, retrievalError error) {
	es := retrievalError.Error()
	evt := event{
		InstanceId: er.instanceId,
		Stage:      string(rep.RetrievalEventFailure),
		Cid:        requestedCid,
		Time:       time.Now().Format(time.RFC3339Nano),
		Error:      &es,
	}
	er.recordEvent("RetrievalQueryFailure", retrievalId, true, storageProviderId, evt)
}

// RetrievalQueryFailure events occur on successfully querying a storage
// provider. A query will result in either a RetrievalQueryFailure or
// a RetrievalQuerySuccess event.
func (er *EventRecorder) RetrievalQuerySuccess(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse) {
	eqs := &eventQueryResponse{
		Status:                     uint64(queryResponse.Status),
		PieceCIDFound:              uint64(queryResponse.PieceCIDFound),
		Size:                       queryResponse.Size,
		PaymentAddress:             queryResponse.PaymentAddress.String(),
		MinPricePerByte:            queryResponse.MinPricePerByte.String(),
		MaxPaymentInterval:         queryResponse.MaxPaymentInterval,
		MaxPaymentIntervalIncrease: queryResponse.MaxPaymentIntervalIncrease,
		Message:                    queryResponse.Message,
		UnsealPrice:                queryResponse.UnsealPrice.String(),
	}
	evt := event{
		InstanceId:    er.instanceId,
		Stage:         string(rep.RetrievalEventSuccess),
		Cid:           requestedCid,
		Time:          time.Now().Format(time.RFC3339Nano),
		QueryResponse: eqs,
	}

	er.recordEvent("RetrievalQuerySuccess", retrievalId, false, storageProviderId, evt)
}

// RetrievalStart events are fired at the beginning of retrieval-proper, once
// a storage provider is selected for retrieval. Note that upon failure,
// another storage provider may be selected so multiple RetrievalStart events
// may occur for the same retrieval.
func (er *EventRecorder) RetrievalStart(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, rootCid cid.Cid, storageProviderId peer.ID) {
	var rcid *cid.Cid
	if !requestedCid.Equals(rootCid) {
		rcid = &rootCid
	}

	evt := event{
		InstanceId: er.instanceId,
		Stage:      "start",
		Cid:        requestedCid,
		RootCid:    rcid,
		Time:       timestamp.Format(time.RFC3339Nano),
	}

	er.recordEvent("RetrievalStart", retrievalId, false, storageProviderId, evt)
}

// RetrievalProgress events occur during the process of a retrieval. The
// Success and failure progress event types are not reported here, but are
// signalled via RetrievalSuccess or RetrievalFailure.
func (er *EventRecorder) RetrievalProgress(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage rep.RetrievalEventCode) {
	evt := event{
		InstanceId: er.instanceId,
		Stage:      string(stage),
		Cid:        requestedCid,
		Time:       time.Now().Format(time.RFC3339Nano),
	}

	er.recordEvent("RetrievalProgress", retrievalId, false, storageProviderId, evt)
}

// RetrievalSuccess events occur on the success of a retrieval. A retrieval
// will result in either a RetrievalQueryFailure or a RetrievalQuerySuccess
// event.
func (er *EventRecorder) RetrievalSuccess(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, storageProviderId peer.ID, retrievalStats filclient.RetrievalStats) {
	ers := &eventRetrievalSuccess{
		Size:         retrievalStats.Size,
		AskPrice:     retrievalStats.AskPrice.String(),
		TotalPayment: retrievalStats.TotalPayment.String(),
		NumPayments:  uint(retrievalStats.NumPayments),
	}
	evt := event{
		InstanceId: er.instanceId,
		Stage:      string(rep.RetrievalEventSuccess),
		Cid:        requestedCid,
		Time:       time.Now().Format(time.RFC3339Nano),
		Success:    ers,
	}

	er.recordEvent("RetrievalSuccess", retrievalId, false, storageProviderId, evt)
}

// RetrievalFailure events occur on the failure of a retrieval. A retrieval
// will result in either a RetrievalQueryFailure or a RetrievalQuerySuccess
// event.
func (er *EventRecorder) RetrievalFailure(retrievalId uuid.UUID, timestamp time.Time, requestedCid cid.Cid, storageProviderId peer.ID, retrievalError error) {
	es := retrievalError.Error()
	evt := event{
		InstanceId: er.instanceId,
		Stage:      string(rep.RetrievalEventFailure),
		Cid:        requestedCid,
		Time:       time.Now().Format(time.RFC3339Nano),
		Error:      &es,
	}
	er.recordEvent("RetrievalFailure", retrievalId, false, storageProviderId, evt)
}

func (er *EventRecorder) recordEvent(eventSource string, retrievalId uuid.UUID, queryMode bool, storageProviderId peer.ID, evt event) {
	// https://.../retrieval-event/~uuid~/providers/~peerID~
	// or
	// https://.../query-event/~uuid~
	// or
	// https://.../query-event/~uuid~/providers/~peerID~
	eventType := "retrieval"
	if queryMode {
		eventType = "query"
	}
	prov := ""
	if storageProviderId != "" {
		prov = fmt.Sprintf("/providers/%s", storageProviderId.String())
	}
	url := fmt.Sprintf("%s/%s-event/%s%s", er.endpointURL, eventType, retrievalId.String(), prov)

	node := bindnode.Wrap(&evt, eventSchemaType)
	byts, err := ipld.Encode(node.Representation(), dagjson.Encode)
	if err != nil {
		log.Errorf("Failed to wrap and encode event [%s]: %w", eventSource, err.Error())
		return
	}
	req, err := http.NewRequest("PUT", url, bytes.NewBufferString(string(byts)))
	if err != nil {
		log.Errorf("Failed to create PUT request [%s] for recorder [%s]: %w", eventSource, url, err.Error())
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Errorf("Failed to PUT event [%s] to recorder [%s]: %w", eventSource, url, err.Error())
		return
	}
	defer resp.Body.Close() // error not so important at this point
	return
}

func init() {
	typeSystem, err := ipld.LoadSchemaBytes([]byte(eventSchema))
	if err != nil {
		panic(err)
	}
	eventSchemaType = typeSystem.TypeByName("event")
	if eventSchemaType == nil {
		panic(errors.New("failed to extract `event` from schema"))
	}
	_ = bindnode.Prototype((*event)(nil), eventSchemaType) // check for problems
}
