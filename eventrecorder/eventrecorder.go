package eventrecorder

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/application-research/autoretrieve/filecoin"
	"github.com/application-research/filclient/rep"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
)

var _ filecoin.RetrievalEventListener = (*EventRecorder)(nil)

var log = logging.Logger("eventrecorder")

// NewEventRecorder creates a new event recorder with the ID of this instance
// and the URL to POST to
func NewEventRecorder(instanceId string, endpointURL string, endpointAuthorization string) *EventRecorder {
	return &EventRecorder{instanceId, endpointURL, endpointAuthorization}
}

// EventRecorder receives events from the retrieval manager and posts event data
// to a given endpoint as POSTs with JSON bodies
type EventRecorder struct {
	instanceId            string
	endpointURL           string
	endpointAuthorization string
}

type eventReport struct {
	RetrievalId       uuid.UUID   `json:"retrievalId"`
	InstanceId        string      `json:"instanceId"`
	Cid               string      `json:"cid"`
	StorageProviderId peer.ID     `json:"storageProviderId"`
	Phase             rep.Phase   `json:"phase"`
	PhaseStartTime    time.Time   `json:"phaseStartTime"`
	Event             rep.Code    `json:"event"`
	EventTime         time.Time   `json:"eventTime"`
	EventDetails      interface{} `json:"eventDetails,omitempty"`
}

// eventDetailsSuccess is for the EventDetails in the case of a retrieval
// success
type eventDetailsSuccess struct {
	ReceivedSize uint64 `json:"receivedSize"`
	ReceivedCids int64  `json:"receivedCids"`
}

// eventDetailsError is for the EventDetails in the case of a query or retrieval
// failure
type eventDetailsError struct {
	Error string `json:"error"`
}

// QueryProgress events occur during the query process
func (er *EventRecorder) QueryProgress(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, retrievalCid cid.Cid, storageProviderId peer.ID, event rep.Code) {
	evt := eventReport{retrievalId, er.instanceId, retrievalCid.String(), storageProviderId, rep.QueryPhase, phaseStartTime, event, eventTime, nil}
	er.recordEvent("QueryProgress", evt)
}

// QueryFailure events occur on the failure of querying a storage
// provider. A query will result in either a QueryFailure or
// a QuerySuccess event.
func (er *EventRecorder) QueryFailure(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, retrievalCid cid.Cid, storageProviderId peer.ID, errorString string) {
	evt := eventReport{retrievalId, er.instanceId, retrievalCid.String(), storageProviderId, rep.QueryPhase, phaseStartTime, rep.FailureCode, eventTime, nil}
	evt.EventDetails = &eventDetailsError{errorString}
	er.recordEvent("QueryFailure", evt)
}

// QuerySuccess events occur on successfully querying a storage
// provider. A query will result in either a QueryFailure or
// a QuerySuccess event.
func (er *EventRecorder) QuerySuccess(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, retrievalCid cid.Cid, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse) {
	evt := eventReport{retrievalId, er.instanceId, retrievalCid.String(), storageProviderId, rep.QueryPhase, phaseStartTime, rep.QueryAskedCode, eventTime, nil}
	evt.EventDetails = &queryResponse
	er.recordEvent("QuerySuccess", evt)
}

// RetrievalProgress events occur during the process of a retrieval. The
// Success and failure progress event types are not reported here, but are
// signalled via RetrievalSuccess or RetrievalFailure.
func (er *EventRecorder) RetrievalProgress(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, retrievalCid cid.Cid, storageProviderId peer.ID, event rep.Code) {
	evt := eventReport{retrievalId, er.instanceId, retrievalCid.String(), storageProviderId, rep.RetrievalPhase, phaseStartTime, event, eventTime, nil}
	er.recordEvent("RetrievalProgress", evt)
}

// RetrievalSuccess events occur on the success of a retrieval. A retrieval
// will result in either a QueryFailure or a QuerySuccess
// event.
func (er *EventRecorder) RetrievalSuccess(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, retrievalCid cid.Cid, storageProviderId peer.ID, retrievedSize uint64, receivedCids int64) {
	evt := eventReport{retrievalId, er.instanceId, retrievalCid.String(), storageProviderId, rep.RetrievalPhase, phaseStartTime, rep.SuccessCode, eventTime, nil}
	evt.EventDetails = &eventDetailsSuccess{retrievedSize, receivedCids}
	er.recordEvent("RetrievalSuccess", evt)
}

// RetrievalFailure events occur on the failure of a retrieval. A retrieval
// will result in either a QueryFailure or a QuerySuccess event.
func (er *EventRecorder) RetrievalFailure(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, retrievalCid cid.Cid, storageProviderId peer.ID, errorString string) {
	evt := eventReport{retrievalId, er.instanceId, retrievalCid.String(), storageProviderId, rep.RetrievalPhase, phaseStartTime, rep.FailureCode, eventTime, nil}
	evt.EventDetails = &eventDetailsError{errorString}
	er.recordEvent("RetrievalFailure", evt)
}

func (er *EventRecorder) recordEvent(eventSource string, evt eventReport) {
	byts, err := json.Marshal(evt)
	if err != nil {
		log.Errorf("Failed to JSONify and encode event [%s]: %w", eventSource, err.Error())
		return
	}

	req, err := http.NewRequest("POST", er.endpointURL, bytes.NewBufferString(string(byts)))
	if err != nil {
		log.Errorf("Failed to create POST request [%s] for recorder [%s]: %w", eventSource, er.endpointURL, err.Error())
		return
	}

	req.Header.Set("Content-Type", "application/json")

	// set authorization header if configured
	if er.endpointAuthorization != "" {
		req.Header.Set("Authorization", er.endpointAuthorization)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Errorf("Failed to POST event [%s] to recorder [%s]: %w", eventSource, er.endpointURL, err.Error())
		return
	}

	defer resp.Body.Close() // error not so important at this point
}
