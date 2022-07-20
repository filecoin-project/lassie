package eventrecorder

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func NewEventRecorder(instanceId string, endpointURL string) *EventRecorder {
	return &EventRecorder{instanceId, fmt.Sprintf("%s/retrieval-events", endpointURL)}
}

type EventRecorder struct {
	instanceId  string
	endpointURL string
}

type eventReport struct {
	RetrievalId       uuid.UUID              `json:"retrievalId"`
	InstanceId        string                 `json:"instanceId"`
	Cid               string                 `json:"cid"`
	StorageProviderId peer.ID                `json:"storageProviderId"`
	Phase             rep.Phase              `json:"phase"`
	PhaseStartTime    time.Time              `json:"phaseStartTime"`
	Event             rep.RetrievalEventCode `json:"event"`
	EventTime         time.Time              `json:"eventTime"`
	EventDetails      interface{}            `json:"eventDetails"`
}

type eventDetailsSuccess struct {
	ReceivedSize uint64 `json:"receivedSize,omitempty"`
}

type eventDetailsError struct {
	Error string `json:"error,omitempty"`
}

func (er EventRecorder) newEventReport(
	retrievalId uuid.UUID,
	retrievalCid cid.Cid,
	storageProviderId peer.ID,
	phase rep.Phase,
	phaseStartTime time.Time,
	event rep.RetrievalEventCode,
	eventTime time.Time,
) eventReport {
	return eventReport{retrievalId, er.instanceId, retrievalCid.String(), storageProviderId, phase, phaseStartTime, event, eventTime, nil}
}

// RetrievalQueryProgress events occur during the query process, events
// include: connect and query-ask
func (er *EventRecorder) RetrievalQueryProgress(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, retrievalCid cid.Cid, storageProviderId peer.ID, event rep.RetrievalEventCode) {
	evt := er.newEventReport(retrievalId, retrievalCid, storageProviderId, rep.QueryPhase, phaseStartTime, event, eventTime)
	er.recordEvent("RetrievalQueryProgress", evt)
}

// RetrievalQueryFailure events occur on the failure of querying a storage
// provider. A query will result in either a RetrievalQueryFailure or
// a RetrievalQuerySuccess event.
func (er *EventRecorder) RetrievalQueryFailure(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, retrievalCid cid.Cid, storageProviderId peer.ID, errorString string) {
	evt := er.newEventReport(retrievalId, retrievalCid, storageProviderId, rep.QueryPhase, phaseStartTime, rep.RetrievalEventFailure, eventTime)
	evt.EventDetails = &eventDetailsError{errorString}
	er.recordEvent("RetrievalQueryFailure", evt)
}

// RetrievalQuerySuccess events occur on successfully querying a storage
// provider. A query will result in either a RetrievalQueryFailure or
// a RetrievalQuerySuccess event.
func (er *EventRecorder) RetrievalQuerySuccess(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, retrievalCid cid.Cid, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse) {
	evt := er.newEventReport(retrievalId, retrievalCid, storageProviderId, rep.QueryPhase, phaseStartTime, rep.RetrievalEventQueryAsk, eventTime)
	evt.EventDetails = &queryResponse
	er.recordEvent("RetrievalQuerySuccess", evt)
}

// RetrievalProgress events occur during the process of a retrieval. The
// Success and failure progress event types are not reported here, but are
// signalled via RetrievalSuccess or RetrievalFailure.
func (er *EventRecorder) RetrievalProgress(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, retrievalCid cid.Cid, storageProviderId peer.ID, event rep.RetrievalEventCode) {
	evt := er.newEventReport(retrievalId, retrievalCid, storageProviderId, rep.RetrievalPhase, phaseStartTime, event, eventTime)
	er.recordEvent("RetrievalProgress", evt)
}

// RetrievalSuccess events occur on the success of a retrieval. A retrieval
// will result in either a RetrievalQueryFailure or a RetrievalQuerySuccess
// event.
func (er *EventRecorder) RetrievalSuccess(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, retrievalCid cid.Cid, storageProviderId peer.ID, retrievedSize uint64) {
	evt := er.newEventReport(retrievalId, retrievalCid, storageProviderId, rep.RetrievalPhase, phaseStartTime, rep.RetrievalEventSuccess, eventTime)
	evt.EventDetails = &eventDetailsSuccess{retrievedSize}
	er.recordEvent("RetrievalSuccess", evt)
}

// RetrievalFailure events occur on the failure of a retrieval. A retrieval
// will result in either a RetrievalQueryFailure or a RetrievalQuerySuccess
// event.
func (er *EventRecorder) RetrievalFailure(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, retrievalCid cid.Cid, storageProviderId peer.ID, errorString string) {
	evt := er.newEventReport(retrievalId, retrievalCid, storageProviderId, rep.RetrievalPhase, phaseStartTime, rep.RetrievalEventFailure, eventTime)
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

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Errorf("Failed to POST event [%s] to recorder [%s]: %w", eventSource, er.endpointURL, err.Error())
		return
	}

	defer resp.Body.Close() // error not so important at this point
}
