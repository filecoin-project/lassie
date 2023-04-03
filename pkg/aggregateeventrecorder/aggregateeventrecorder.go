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

var logging = log.Logger("aggregateeventrecorder")

const httpTimeout = 5 * time.Second
const parallelPosters = 5

type tempData struct {
	startTime            time.Time
	candidatesFound      int
	candidatesFoundTime  time.Time
	candidatesFiltered   int
	firstByteTime        time.Time
	ttfb                 int64
	success              bool
	spId                 string
	bandwidth            uint64
	attemptedProtocolSet map[string]struct{}
	retrievalAttemptMap  map[string]RetrievalAttempt
}

type RetrievalAttempt struct {
	StorageProviderID string `json:"storageProviderId,omitempty"`
	Error             string `json:"error"`
}

type AggregateEvent struct {
	InstanceID        string    `json:"instanceId"`                  // The ID of the Lassie instance generating the event
	RetrievalID       string    `json:"retrievalId"`                 // The unique ID of the retrieval
	StorageProviderID string    `json:"storageProviderId,omitempty"` // The ID of the storage provider that served the retrieval content
	TimeToFirstByte   int64     `json:"timeToFirstByte,omitempty"`   // The time it took to receive the first byte in milliseconds
	Bandwidth         uint64    `json:"bandwidth,omitempty"`         // The bandwidth of the retrieval in bytes per second
	Success           bool      `json:"success"`                     // Wether or not the retreival ended with a success event
	StartTime         time.Time `json:"startTime"`                   // The time the retrieval started
	EndTime           time.Time `json:"endTime"`                     // The time the retrieval ended

	TimeToFirstIndexerResult  time.Time          `json:"timeToFirstIndexerResult,omitempty"` // Time we received our first "CandidateFound" event
	IndexerCandidatesReceived int                `json:"indexerCandidatesReceived"`          // The number of candidates received from the indexer
	IndexerCandidatesFiltered int                `json:"indexerCandidatesFiltered"`          // The number of candidates that made it through the filtering stage
	ProtocolsAllowed          []string           `json:"protocolsAllowed,omitempty"`         // The available protocols that could be used for this retrieval
	ProtocolsAttempted        []string           `json:"protocolsAttempted,omitempty"`       // The protocols that were used to attempt this retrieval
	RetrievalAttempts         []RetrievalAttempt `json:"retrievalAttempts,omitempty"`        // All of the retrieval attempts
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

func NewAggregateEventRecorder(ctx context.Context, instanceID string, endpointURL string, endpointAuthorization string) *aggregateEventRecorder {
	recorder := &aggregateEventRecorder{
		ctx:                   ctx,
		instanceID:            instanceID,
		endpointURL:           endpointURL,
		endpointAuthorization: endpointAuthorization,
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
	eventTempMap := make(map[types.RetrievalID]tempData)

	for {
		select {
		case <-a.ctx.Done():

		// Read incoming data
		case event := <-a.ingestChan:
			id := event.RetrievalId()

			switch event.Code() {
			case types.StartedCode:

				// What we want to do depends which phase the Started event was emitted from
				switch event.Phase() {
				case types.FetchPhase:
					// Initialize the temp data for tracking retrieval stats
					eventTempMap[id] = tempData{
						startTime:            event.Time(),
						candidatesFound:      0,
						candidatesFiltered:   0,
						firstByteTime:        time.Time{},
						spId:                 "",
						success:              false,
						bandwidth:            0,
						ttfb:                 0,
						attemptedProtocolSet: make(map[string]struct{}),
						retrievalAttemptMap:  make(map[string]RetrievalAttempt),
					}

				case types.RetrievalPhase:
					// Create a retrieval attempt
					var attempt RetrievalAttempt
					spid := event.(events.RetrievalEventStarted).StorageProviderId().String()
					attempt.StorageProviderID = spid

					// Save the retrieval attempt
					tempData := eventTempMap[id]
					retrievalAttemptMap := tempData.retrievalAttemptMap
					retrievalAttemptMap[spid] = attempt
					tempData.retrievalAttemptMap = retrievalAttemptMap

					// Add any event protocols to the set of attempted protocols
					for _, protocol := range event.(events.RetrievalEventStarted).Protocols() {
						tempData.attemptedProtocolSet[protocol.String()] = struct{}{}
					}

					eventTempMap[id] = tempData
				}

			case types.CandidatesFoundCode:
				tempData := eventTempMap[id]
				tempData.candidatesFoundTime = event.Time()
				tempData.candidatesFound = len(event.(events.RetrievalEventCandidatesFound).Candidates())
				eventTempMap[id] = tempData

			case types.CandidatesFilteredCode:
				tempData := eventTempMap[id]
				tempData.candidatesFiltered = len(event.(events.RetrievalEventCandidatesFiltered).Candidates())
				eventTempMap[id] = tempData

			case types.FirstByteCode:
				// Calculate time to first byte
				tempData := eventTempMap[id]
				tempData.firstByteTime = event.Time()
				tempData.ttfb = event.Time().Sub(tempData.startTime).Milliseconds()
				eventTempMap[id] = tempData

			case types.FailedCode:
				tempData := eventTempMap[id]
				spid := event.(events.RetrievalEventFailed).StorageProviderId().String()
				errorMsg := event.(events.RetrievalEventFailed).ErrorMessage()

				// Add an error message to the retrieval attempt
				retrievalAttemptMap := tempData.retrievalAttemptMap
				attempt := retrievalAttemptMap[spid]
				attempt.Error = errorMsg

				// Save the attempt back to the map
				retrievalAttemptMap[spid] = attempt
				tempData.retrievalAttemptMap = retrievalAttemptMap

				eventTempMap[id] = tempData

			case types.SuccessCode:
				tempData := eventTempMap[id]
				tempData.success = true
				tempData.spId = event.(events.RetrievalEventSuccess).StorageProviderId().String()

				// Calculate bandwidth
				receivedSize := event.(events.RetrievalEventSuccess).ReceivedSize()
				duration := event.Time().Sub(tempData.firstByteTime).Seconds()
				if duration != 0 {
					tempData.bandwidth = uint64(float64(receivedSize) / duration)
				}

				eventTempMap[id] = tempData

			case types.FinishedCode:
				tempData, ok := eventTempMap[id]
				if !ok {
					logging.Errorf("Received Finished event but can't find aggregate data. Skipping creation of aggregate event.")
					continue
				}

				// Create a slice of attempted protocols
				var protocolsAttempted []string
				for protocol := range tempData.attemptedProtocolSet {
					protocolsAttempted = append(protocolsAttempted, protocol)
				}

				// Create a slice of retrieval attempts
				var retrievalAttempts []RetrievalAttempt
				for _, attempt := range tempData.retrievalAttemptMap {
					retrievalAttempts = append(retrievalAttempts, attempt)
				}

				// Create the aggregate event
				aggregatedEvent := AggregateEvent{
					InstanceID:        a.instanceID,
					RetrievalID:       id.String(),
					StorageProviderID: tempData.spId,
					TimeToFirstByte:   tempData.ttfb,
					Bandwidth:         tempData.bandwidth,
					Success:           tempData.success,
					StartTime:         tempData.startTime,
					EndTime:           event.Time(),

					TimeToFirstIndexerResult:  tempData.candidatesFoundTime,
					IndexerCandidatesReceived: tempData.candidatesFound,
					IndexerCandidatesFiltered: tempData.candidatesFiltered,
					ProtocolsAllowed:          make([]string, 0),
					ProtocolsAttempted:        protocolsAttempted,
					RetrievalAttempts:         retrievalAttempts,
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
				logging.Errorf("Failed to JSONify and encode event: %w", err.Error())
				continue
			}

			req, err := http.NewRequest("POST", a.endpointURL, bytes.NewBufferString(string(byts)))
			if err != nil {
				logging.Errorf("Failed to create POST request for [%s]: %w", a.endpointURL, err.Error())
				continue
			}

			req.Header.Set("Content-Type", "application/json")

			// set authorization header if configured
			if a.endpointAuthorization != "" {
				req.Header.Set("Authorization", fmt.Sprintf("Basic %s", a.endpointAuthorization))
			}

			resp, err := client.Do(req)
			if err != nil {
				logging.Errorf("Failed to POST event to [%s]: %w", a.endpointURL, err.Error())
				continue
			}

			defer resp.Body.Close() // error not so important at this point
			if resp.StatusCode < 200 || resp.StatusCode > 299 {
				logging.Errorf("Expected success response code from server, got: %s", http.StatusText(resp.StatusCode))
			}
		}
	}
}
