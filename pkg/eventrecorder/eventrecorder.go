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
	"github.com/multiformats/go-multicodec"
)

var HttpTimeout = 5 * time.Second
var ParallelPosters = 5

var log = logging.Logger("eventrecorder")

type EventRecorderConfig struct {
	DisableIndexerEvents  bool
	InstanceID            string
	EndpointURL           string
	EndpointAuthorization string
}

// NewEventRecorder creates a new event recorder with the ID of this instance
// and the URL to POST to
func NewEventRecorder(ctx context.Context, cfg EventRecorderConfig) *EventRecorder {
	er := &EventRecorder{
		ctx,
		make(chan report),
		make(chan []report),
		cfg,
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
	ctx                context.Context
	incomingReportChan chan report
	reportChan         chan []report
	cfg                EventRecorderConfig
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

type EventDetailsIndexer struct {
	CandidateCount uint64   `json:"candidateCount"`
	Protocols      []string `json:"protocols"`
}

func toStrings(protocols []multicodec.Code) []string {
	protocolStrings := make([]string, 0, len(protocols))
	for _, protocol := range protocols {
		protocolStrings = append(protocolStrings, protocol.String())
	}
	return protocolStrings
}

func (er *EventRecorder) RecordEvent(event types.RetrievalEvent) {
	if event.Phase() == types.FetchPhase {
		// ignored for now because it's not recognized upstream
		return
	}
	if er.cfg.DisableIndexerEvents && event.Phase() == types.IndexerPhase {
		// ignore indexer events for now, it can get very chatty in the autoretrieve
		// case where every request results in an indexer lookup
		return
	}

	// TODO: We really need to change the schema here to include protocols
	// For now, we double up the string here, which isn't great
	// -- there are no peer ids for SPs so you just record the word
	// "bitswap-peer" as the SP id

	evt := eventReport{
		RetrievalId:       event.RetrievalId(),
		InstanceId:        er.cfg.InstanceID,
		Cid:               event.PayloadCid().String(),
		StorageProviderId: types.Identifier(event),
		Phase:             event.Phase(),
		PhaseStartTime:    event.PhaseStartTime(),
		EventName:         event.Code(),
		EventTime:         event.Time(),
	}

	switch ret := event.(type) {
	case events.EventWithCandidates: // events.RetrievalEventCandidatesFound, events.RetrievalEventCandidatesFiltered:
		evt.EventDetails = &EventDetailsIndexer{
			CandidateCount: uint64(len(ret.Candidates())),
			Protocols:      toStrings(ret.Protocols()),
		}
	case events.RetrievalEventConnected:
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

	req, err := http.NewRequest("POST", er.cfg.EndpointURL, bytes.NewBufferString(string(byts)))
	if err != nil {
		log.Errorf("Failed to create POST request [%s] for recorder [%s]: %w", sources, er.cfg.EndpointURL, err.Error())
		return
	}

	req.Header.Set("Content-Type", "application/json")

	// set authorization header if configured
	if er.cfg.EndpointAuthorization != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Basic %s", er.cfg.EndpointAuthorization))
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("Failed to POST event [%s] to recorder [%s]: %w", sources, er.cfg.EndpointURL, err.Error())
		return
	}

	defer resp.Body.Close() // error not so important at this point
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		log.Errorf("Expected success response code from event recorder server, got: %s", http.StatusText(resp.StatusCode))
	}
}
