package aggregateeventrecorder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-log/v2"
)

var logger = log.Logger("lassie/aggregateeventrecorder")

const httpTimeout = 5 * time.Second
const parallelPosters = 5

type tempData struct {
	startTime                time.Time
	candidatesFound          int
	timeToFirstIndexerResult string
	candidatesFiltered       int
	firstByteTime            time.Time
	ttfb                     string
	success                  bool
	spId                     string
	bandwidth                uint64
	bytesTransferred         uint64
	allowedProtocols         []string
	attemptedProtocolSet     map[string]struct{}
	successfulProtocol       string
	retrievalAttempts        map[string]*RetrievalAttempt
}

type RetrievalAttempt struct {
	Error           string `json:"error,omitempty"`
	TimeToFirstByte string `json:"timeToFirstByte,omitempty"`
	Protocol        string `json:"protocol,omitempty"`
}

type AggregateEvent struct {
	InstanceID        string    `json:"instanceId"`                  // The ID of the Lassie instance generating the event
	RetrievalID       string    `json:"retrievalId"`                 // The unique ID of the retrieval
	StorageProviderID string    `json:"storageProviderId,omitempty"` // The ID of the storage provider that served the retrieval content
	TimeToFirstByte   string    `json:"timeToFirstByte,omitempty"`   // The time it took to receive the first byte in milliseconds
	Bandwidth         uint64    `json:"bandwidth,omitempty"`         // The bandwidth of the retrieval in bytes per second
	BytesTransferred  uint64    `json:"bytesTransferred,omitempty"`  // The total transmitted deal size
	Success           bool      `json:"success"`                     // Wether or not the retreival ended with a success event
	StartTime         time.Time `json:"startTime"`                   // The time the retrieval started
	EndTime           time.Time `json:"endTime"`                     // The time the retrieval ended

	TimeToFirstIndexerResult  string                       `json:"timeToFirstIndexerResult,omitempty"` // time it took to receive our first "CandidateFound" event
	IndexerCandidatesReceived int                          `json:"indexerCandidatesReceived"`          // The number of candidates received from the indexer
	IndexerCandidatesFiltered int                          `json:"indexerCandidatesFiltered"`          // The number of candidates that made it through the filtering stage
	ProtocolsAllowed          []string                     `json:"protocolsAllowed,omitempty"`         // The available protocols that could be used for this retrieval
	ProtocolsAttempted        []string                     `json:"protocolsAttempted,omitempty"`       // The protocols that were used to attempt this retrieval
	ProtocolSucceeded         string                       `json:"protocolSucceeded,omitempty"`        // The protocol used for a successful event
	RetrievalAttempts         map[string]*RetrievalAttempt `json:"retrievalAttempts,omitempty"`        // All of the retrieval attempts, indexed by their SP ID
}

type batchedEvents struct {
	Events []AggregateEvent `json:"events"`
}

type aggregateEventRecorder struct {
	ctx                   context.Context
	instanceID            string                    // The ID of the instance generating the event
	endpointURL           string                    // The URL to POST the events to
	endpointAuthorization string                    // The key to use in an Authorization header
	ingestChan            chan types.RetrievalEvent // A channel for incoming events
	postChan              chan []AggregateEvent     // A channel for posting events
}

type EventRecorderConfig struct {
	InstanceID            string
	EndpointURL           string
	EndpointAuthorization string
}

func NewAggregateEventRecorder(ctx context.Context, eventRecorderConfig EventRecorderConfig) *aggregateEventRecorder {
	recorder := &aggregateEventRecorder{
		ctx:                   ctx,
		instanceID:            eventRecorderConfig.InstanceID,
		endpointURL:           eventRecorderConfig.EndpointURL,
		endpointAuthorization: eventRecorderConfig.EndpointAuthorization,
		ingestChan:            make(chan types.RetrievalEvent),
		postChan:              make(chan []AggregateEvent),
	}

	go recorder.ingestEvents()
	for i := 0; i < parallelPosters; i++ {
		go recorder.postEvents()
	}

	return recorder
}

// RetrievalEventSubsciber returns a RetrievalEventSubscriber that POSTs
// a batch of aggregated retrieval events to an event recorder API endpoint
func (a *aggregateEventRecorder) RetrievalEventSubscriber() types.RetrievalEventSubscriber {
	return func(event types.RetrievalEvent) {
		// Process the incoming event
		select {
		case <-a.ctx.Done():
		case a.ingestChan <- event:
		}
	}
}

// ingestEvents receives and stores event data from ingestChan
// to generate an aggregated event upon receiving a finished event.
func (a *aggregateEventRecorder) ingestEvents() {
	var batchedData []AggregateEvent
	var emptyGaurdChan chan []AggregateEvent = nil
	eventTempMap := make(map[types.RetrievalID]*tempData)

	for {
		select {
		case <-a.ctx.Done():

		// Read incoming data
		case event := <-a.ingestChan:
			id := event.RetrievalId()
			if event.Code() == types.StartedCode && event.Phase() == types.FetchPhase {
				allowedProtocols := make([]string, 0, len(event.Protocols()))
				for _, codec := range event.Protocols() {
					allowedProtocols = append(allowedProtocols, codec.String())
				}
				// Initialize the temp data for tracking retrieval stats
				eventTempMap[id] = &tempData{
					startTime:                event.Time(),
					candidatesFound:          0,
					candidatesFiltered:       0,
					firstByteTime:            time.Time{},
					spId:                     "",
					success:                  false,
					bandwidth:                0,
					ttfb:                     "",
					timeToFirstIndexerResult: "",
					allowedProtocols:         allowedProtocols,
					attemptedProtocolSet:     make(map[string]struct{}),
					successfulProtocol:       "",
					retrievalAttempts:        make(map[string]*RetrievalAttempt),
				}
				continue
			}
			tempData, ok := eventTempMap[id]
			if !ok {
				if event.Code() == types.FinishedCode {
					logger.Errorf("Received Finished event but can't find aggregate data. Skipping creation of aggregate event.")
				}
				continue
			}
			switch event.Code() {
			case types.StartedCode:

				// What we want to do depends which phase the Started event was emitted from
				switch event.Phase() {
				case types.FetchPhase:

				case types.RetrievalPhase:
					// Create a retrieval attempt
					var attempt RetrievalAttempt
					if len(event.Protocols()) > 0 {
						attempt.Protocol = event.Protocols()[0].String()
					}
					spid := types.Identifier(event)

					// Save the retrieval attempt
					tempData.retrievalAttempts[spid] = &attempt

					// Add any event protocols to the set of attempted protocols
					for _, protocol := range event.(events.RetrievalEventStarted).Protocols() {
						tempData.attemptedProtocolSet[protocol.String()] = struct{}{}
					}

				}

			case types.CandidatesFoundCode:
				if tempData.timeToFirstIndexerResult == "" {
					tempData.timeToFirstIndexerResult = event.Time().Sub(tempData.startTime).String()
				}
				tempData.candidatesFound += len(event.(events.RetrievalEventCandidatesFound).Candidates())

			case types.CandidatesFilteredCode:
				tempData.candidatesFiltered += len(event.(events.RetrievalEventCandidatesFiltered).Candidates())

			case types.FirstByteCode:
				// Calculate time to first byte
				spid := types.Identifier(event)
				retrievalTtfb := event.Time().Sub(tempData.startTime).String()
				spTtfb := event.(events.RetrievalEventFirstByte).Duration().String()
				tempData.retrievalAttempts[spid].TimeToFirstByte = spTtfb
				if tempData.ttfb == "" {
					tempData.firstByteTime = event.Time()
					tempData.ttfb = retrievalTtfb
				}
			case types.FailedCode:
				switch event.Phase() {
				case types.RetrievalPhase:
					spid := types.Identifier(event)
					errorMsg := event.(events.RetrievalEventFailed).ErrorMessage()

					// Add an error message to the retrieval attempt
					tempData.retrievalAttempts[spid].Error = errorMsg
				}
			case types.SuccessCode:
				tempData.success = true
				tempData.successfulProtocol = event.(events.RetrievalEventSuccess).Protocol().String()
				tempData.spId = types.Identifier(event)

				// Calculate bandwidth
				receivedSize := event.(events.RetrievalEventSuccess).ReceivedSize()
				tempData.bytesTransferred = receivedSize
				duration := event.Time().Sub(tempData.firstByteTime).Seconds()
				if duration != 0 {
					tempData.bandwidth = uint64(float64(receivedSize) / duration)
				}
			case types.FinishedCode:

				// Create a slice of attempted protocols
				var protocolsAttempted []string
				for protocol := range tempData.attemptedProtocolSet {
					protocolsAttempted = append(protocolsAttempted, protocol)
				}

				// Create the aggregate event
				aggregatedEvent := AggregateEvent{
					InstanceID:        a.instanceID,
					RetrievalID:       id.String(),
					StorageProviderID: tempData.spId,
					TimeToFirstByte:   tempData.ttfb,
					Bandwidth:         tempData.bandwidth,
					BytesTransferred:  tempData.bytesTransferred,
					Success:           tempData.success,
					StartTime:         tempData.startTime,
					EndTime:           event.Time(),

					TimeToFirstIndexerResult:  tempData.timeToFirstIndexerResult,
					IndexerCandidatesReceived: tempData.candidatesFound,
					IndexerCandidatesFiltered: tempData.candidatesFiltered,
					ProtocolsAllowed:          tempData.allowedProtocols,
					ProtocolsAttempted:        protocolsAttempted,
					ProtocolSucceeded:         tempData.successfulProtocol,
					RetrievalAttempts:         tempData.retrievalAttempts,
				}

				// Delete the key when we're done with the data
				delete(eventTempMap, id)

				batchedData = append(batchedData, aggregatedEvent)
				if emptyGaurdChan == nil {
					emptyGaurdChan = a.postChan // emptyGuardChan is no longer nil when we add to the batch
				}
			}

		// Output batched data to another channel and set channel and batchedData to nil to prevent
		// sending empty batches
		case emptyGaurdChan <- batchedData: // won't execute while emptyGaurdChan is nil
			batchedData = nil
			emptyGaurdChan = nil
		}
	}
}

// postEvents receives batched aggregated events from postChan
// and sends a POST request to the provided endpointURL. If an
// endpointAuthorization is provided, it's used in the Authorization
// header.
func (a *aggregateEventRecorder) postEvents() {
	client := http.Client{Timeout: httpTimeout}

	for {
		select {
		case <-a.ctx.Done():
			return
		case batchedData := <-a.postChan:
			byts, err := json.Marshal(batchedEvents{batchedData})
			if err != nil {
				logger.Errorf("Failed to JSONify and encode event: %w", err.Error())
				continue
			}

			req, err := http.NewRequest("POST", a.endpointURL, bytes.NewBufferString(string(byts)))
			if err != nil {
				logger.Errorf("Failed to create POST request for [%s]: %w", a.endpointURL, err.Error())
				continue
			}

			req.Header.Set("Content-Type", "application/json")

			// set authorization header if configured
			if a.endpointAuthorization != "" {
				req.Header.Set("Authorization", fmt.Sprintf("Basic %s", a.endpointAuthorization))
			}

			resp, err := client.Do(req)
			if err != nil {
				logger.Errorf("Failed to POST event to [%s]: %w", a.endpointURL, err.Error())
				continue
			}

			defer resp.Body.Close() // error not so important at this point
			if resp.StatusCode < 200 || resp.StatusCode > 299 {
				logger.Errorf("Expected success response code from server, got: %s", http.StatusText(resp.StatusCode))
			}
		}
	}
}
