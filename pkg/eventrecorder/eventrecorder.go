package eventrecorder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

var HttpTimeout = 5 * time.Second
var ParallelPosters = 5

var log = logging.Logger("eventrecorder")

const BitswapPeerID = "bitswap-peer"

// NewEventRecorder creates a new event recorder with the ID of this instance
// and the URL to POST to
func NewEventRecorder(ctx context.Context, instanceId string, endpointURL string, endpointAuthorization string) *EventRecorder {
	er := &EventRecorder{
		ctx,
		instanceId,
		endpointURL,
		endpointAuthorization,
		make(chan report),
		make(chan []report),
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
	reportChan            chan []report
}

type eventReport struct {
	RetrievalId       types.RetrievalID `json:"retrievalId"`
	InstanceId        string            `json:"instanceId"`
	Cid               string            `json:"cid"`
	StorageProviderId string            `json:"storageProviderId"`
	Phase             types.Phase       `json:"phase"`
	PhaseStartTime    time.Time         `json:"phaseStartTime"`
	EventName         types.EventCode   `json:"eventName"`
	EventTime         time.Time         `json:"eventTime"`
	EventDetails      interface{}       `json:"eventDetails,omitempty"`
}

// eventDetailsSuccess is for the EventDetails in the case of a retrieval
// success
type EventDetailsSuccess struct {
	ReceivedSize uint64 `json:"receivedSize"`
	ReceivedCids uint64 `json:"receivedCids"`
	Duration     uint64 `json:"durationMs"`
}

// eventDetailsError is for the EventDetails in the case of a query or retrieval
// failure
type EventDetailsError struct {
	Error string `json:"error"`
}

func (er *EventRecorder) RecordEvent(event types.RetrievalEvent) {
	if event.Phase() == types.IndexerPhase {
		// ignore indexer events for now, it can get very chatty in the autoretrieve
		// case where every request results in an indexer lookup
		return
	}

	// TODO: We really need to change the schema here to include protocols
	// For now, we double up the string here, which isn't great
	// -- there are no peer ids for SPs so you just record the word
	// "bitswap-peer" as the SP id
	var spID string
	if event.StorageProviderId() != peer.ID("") {
		spID = event.StorageProviderId().String()
	} else {
		protocols := event.Protocols()
		if len(protocols) == 1 && protocols[0] == multicodec.TransportBitswap {
			spID = BitswapPeerID
		}
	}
	evt := eventReport{
		RetrievalId:       event.RetrievalId(),
		InstanceId:        er.instanceId,
		Cid:               event.PayloadCid().String(),
		StorageProviderId: spID,
		Phase:             event.Phase(),
		PhaseStartTime:    event.PhaseStartTime(),
		EventName:         event.Code(),
		EventTime:         event.Time(),
	}

	switch ret := event.(type) {
	case events.EventWithCandidates: // events.RetrievalEventCandidatesFound, events.RetrievalEventCandidatesFiltered:
	case events.RetrievalEventConnected:
	case events.EventWithQueryResponse: // events.RetrievalEventQueryAsked, events.RetrievalEventQueryAskedFiltered:
		qr := ret.QueryResponse()
		evt.EventDetails = &qr
	case events.RetrievalEventProposed:
	case events.RetrievalEventAccepted:
	case events.RetrievalEventFirstByte:
	case events.RetrievalEventFailed:
		evt.EventDetails = &EventDetailsError{ret.ErrorMessage()}
	case events.RetrievalEventSuccess:
		evt.EventDetails = &EventDetailsSuccess{ret.ReceivedSize(), ret.ReceivedCids(), uint64(ret.Duration().Milliseconds())}
	}

	er.recordEvent(reflect.TypeOf(event).Name(), evt)
}

func (er *EventRecorder) recordEvent(eventSource string, evt eventReport) {
	select {
	case <-er.ctx.Done():
	case er.incomingReportChan <- report{eventSource, evt}:
	}
}

func (er *EventRecorder) ingestReports() {
	var queuedReports []report
	var reportChan chan []report = nil
	for {
		select {
		case <-er.ctx.Done():
			return
		// read incoming report
		case report := <-er.incomingReportChan:
			queuedReports = append(queuedReports, report)
			if reportChan == nil {
				// if queue had been idle, reactivate and set this report to publish next
				reportChan = er.reportChan
			}
		// send outgoing report when queue is active (when reportChan == nil, this path is never followed)
		case reportChan <- queuedReports:
			queuedReports = nil
			reportChan = nil
		}
	}
}

// postReports picks reports from the reportChan and processes, them, an http
// Client will be kept locally for each call to this function, one call per
// desired parallelism can be made within goroutines, each with its own Client.
func (er *EventRecorder) postReports() {
	client := http.Client{Timeout: HttpTimeout}

	for {
		select {
		case <-er.ctx.Done():
			return
		case reports := <-er.reportChan:
			er.handleReports(client, reports)
		}
	}
}

type MultiEventReport struct {
	Events []eventReport `json:"events"`
}

// handleReport turns a report into an HTTP post to the event reporting URL
// using the provided Client. Errors are not returned, only logged.
func (er *EventRecorder) handleReports(client http.Client, reports []report) {
	eventReports := make([]eventReport, 0, len(reports))
	sources := ""
	for idx, report := range reports {
		eventReports = append(eventReports, report.evt)
		sources += report.src
		if idx < len(reports)-1 {
			sources += ", "
		}
	}

	byts, err := json.Marshal(MultiEventReport{eventReports})
	if err != nil {
		log.Errorf("Failed to JSONify and encode event [%s]: %w", sources, err.Error())
		return
	}

	req, err := http.NewRequest("POST", er.endpointURL, bytes.NewBufferString(string(byts)))
	if err != nil {
		log.Errorf("Failed to create POST request [%s] for recorder [%s]: %w", sources, er.endpointURL, err.Error())
		return
	}

	req.Header.Set("Content-Type", "application/json")

	// set authorization header if configured
	if er.endpointAuthorization != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Basic %s", er.endpointAuthorization))
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("Failed to POST event [%s] to recorder [%s]: %w", sources, er.endpointURL, err.Error())
		return
	}

	defer resp.Body.Close() // error not so important at this point
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		log.Errorf("Expected success response code from event recorder server, got: %s", http.StatusText(resp.StatusCode))
	}
}
