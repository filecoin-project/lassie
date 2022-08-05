package eventrecorder

import (
	"bytes"
	"context"
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

var HttpTimeout = 5 * time.Second
var ParallelPosters = 5

var _ filecoin.RetrievalEventListener = (*EventRecorder)(nil)

var log = logging.Logger("eventrecorder")

// NewEventRecorder creates a new event recorder with the ID of this instance
// and the URL to POST to
func NewEventRecorder(ctx context.Context, instanceId string, endpointURL string, endpointAuthorization string) *EventRecorder {
	er := &EventRecorder{
		ctx,
		instanceId,
		endpointURL,
		endpointAuthorization,
		make(chan report),
		make(chan report),
	}

	go er.ingestReports()
	for i := 0; i < ParallelPosters; i++ {
		go er.postReports()
	}

	return er
}

type report struct {
	src string
	evt eventReport
}

// EventRecorder receives events from the retrieval manager and posts event data
// to a given endpoint as POSTs with JSON bodies
type EventRecorder struct {
	ctx                   context.Context
	instanceId            string
	endpointURL           string
	endpointAuthorization string
	incomingReportChan    chan report
	reportChan            chan report
}

type eventReport struct {
	RetrievalId       uuid.UUID   `json:"retrievalId"`
	InstanceId        string      `json:"instanceId"`
	Cid               string      `json:"cid"`
	StorageProviderId peer.ID     `json:"storageProviderId"`
	Phase             rep.Phase   `json:"phase"`
	PhaseStartTime    time.Time   `json:"phaseStartTime"`
	EventName         rep.Code    `json:"eventName"`
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
func (er *EventRecorder) QueryProgress(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, retrievalCid cid.Cid, storageProviderId peer.ID, eventName rep.Code) {
	evt := eventReport{retrievalId, er.instanceId, retrievalCid.String(), storageProviderId, rep.QueryPhase, phaseStartTime, eventName, eventTime, nil}
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
func (er *EventRecorder) RetrievalProgress(retrievalId uuid.UUID, phaseStartTime, eventTime time.Time, retrievalCid cid.Cid, storageProviderId peer.ID, eventName rep.Code) {
	evt := eventReport{retrievalId, er.instanceId, retrievalCid.String(), storageProviderId, rep.RetrievalPhase, phaseStartTime, eventName, eventTime, nil}
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
	select {
	case <-er.ctx.Done():
	case er.incomingReportChan <- report{eventSource, evt}:
	}
}

func (er *EventRecorder) ingestReports() {
	var queuedReports []report
	var nextReport report
	var reportChan chan report = nil
	for {
		select {
		case <-er.ctx.Done():
			return
		// read incoming report
		case report := <-er.incomingReportChan:
			if reportChan == nil {
				// if queue had been idle, reactivate and set this report to publish next
				reportChan = er.reportChan
				nextReport = report
			} else {
				// otherwise, add it to the outgoing queue once the current one finishes
				queuedReports = append(queuedReports, report)
			}
		// send outgoing report when queue is active (when reportChan == nil, this path is never followed)
		case reportChan <- nextReport:
			if len(queuedReports) > 0 {
				// if we haven't exhausted our queue, then add the next report for posting
				nextReport = queuedReports[0]
				queuedReports = queuedReports[1:]
			} else {
				// otherwise, deactivate posting cause we are now idle
				reportChan = nil
			}
		}
	}
}

func (er *EventRecorder) postReports() {
	client := http.Client{Timeout: HttpTimeout}

	for {
		select {
		case <-er.ctx.Done():
			return
		case report := <-er.reportChan:
			byts, err := json.Marshal(report.evt)
			if err != nil {
				log.Errorf("Failed to JSONify and encode event [%s]: %w", report.src, err.Error())
				return
			}

			req, err := http.NewRequest("POST", er.endpointURL, bytes.NewBufferString(string(byts)))
			if err != nil {
				log.Errorf("Failed to create POST request [%s] for recorder [%s]: %w", report.src, er.endpointURL, err.Error())
				return
			}

			req.Header.Set("Content-Type", "application/json")

			// set authorization header if configured
			if er.endpointAuthorization != "" {
				req.Header.Set("Authorization", fmt.Sprintf("Basic %s", er.endpointAuthorization))
			}

			resp, err := client.Do(req)
			if err != nil {
				log.Errorf("Failed to POST event [%s] to recorder [%s]: %w", report.src, er.endpointURL, err.Error())
				return
			}

			defer resp.Body.Close() // error not so important at this point
			if resp.StatusCode <= 200 || resp.StatusCode >= 299 {
				log.Errorf("Expected success response code from event recorder server, got: %s", http.StatusText(resp.StatusCode))
			}
		}
	}
}
