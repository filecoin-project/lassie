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
	startTime     time.Time
	firstByteTime time.Time
	ttfb          int64
	success       bool
	spId          string
	bandwidth     uint64
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
}

type batchedEvents struct {
	Events []AggregateEvent `json:"events"`
}

type aggregateEventRecorder struct {
	ctx                   context.Context
	instanceID            string                    // The ID of the instance generating the event
	endpointURL           string                    // The URL to POST the events to
	endpointAuthorization string                    // The key to use in an Authorization header
	disableIndexerEvents  bool                      // Whether or not to disable indexer events. Defaults to false.
	ingestChan            chan types.RetrievalEvent // A channel for incoming events
	postChan              chan []AggregateEvent     // A channel for posting events
	eventTempMap          map[string]tempData       // Holds data about multiple events for aggregation
}

func NewAggregateEventRecorder(ctx context.Context, instanceID string, endpointURL string, endpointAuthorization string, disableIndexerEvents bool) *aggregateEventRecorder {
	recorder := &aggregateEventRecorder{
		ctx:                   ctx,
		instanceID:            instanceID,
		endpointURL:           endpointURL,
		endpointAuthorization: endpointAuthorization,
		disableIndexerEvents:  disableIndexerEvents,
		ingestChan:            make(chan types.RetrievalEvent),
		postChan:              make(chan []AggregateEvent),
		eventTempMap:          make(map[string]tempData),
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
		if a.disableIndexerEvents && event.Phase() == types.IndexerPhase {
			return
		}

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

	for {
		select {
		case <-a.ctx.Done():
		// read incoming data
		case event := <-a.ingestChan:
			id := event.RetrievalId().String()

			switch event.Code() {
			case types.StartedCode:
				// Record when first phase starts to help calculate time to first byte later
				if event.Phase() == types.FetchPhase {
					a.eventTempMap[id] = tempData{
						startTime:     event.Time(),
						firstByteTime: time.Time{},
						spId:          "",
						success:       false,
						bandwidth:     0,
						ttfb:          0,
					}
				}

			case types.FirstByteCode:
				// Calculate time to first byte
				tempData := a.eventTempMap[id]
				tempData.firstByteTime = event.Time()
				tempData.ttfb = event.Time().Sub(tempData.startTime).Milliseconds()
				a.eventTempMap[id] = tempData

			case types.SuccessCode:
				tempData := a.eventTempMap[id]
				tempData.success = true
				tempData.spId = event.(events.RetrievalEventSuccess).StorageProviderId().String()

				// Calculate bandwidth
				receivedSize := event.(events.RetrievalEventSuccess).ReceivedSize()
				duration := event.Time().Sub(tempData.firstByteTime).Seconds()
				if duration != 0 {
					tempData.bandwidth = uint64(float64(receivedSize) / duration)
				}

				a.eventTempMap[id] = tempData

			case types.FinishedCode:
				tempData, ok := a.eventTempMap[id]
				if !ok {
					logging.Errorf("Received Finished event but can't find aggregate data. Skipping creation of aggregate event.")
					continue
				}

				aggregatedEvent := AggregateEvent{
					InstanceID:        a.instanceID,
					RetrievalID:       id,
					StorageProviderID: tempData.spId,
					TimeToFirstByte:   tempData.ttfb,
					Bandwidth:         tempData.bandwidth,
					Success:           tempData.success,
					StartTime:         tempData.startTime,
					EndTime:           event.Time(),
				}

				// Delete the key when we're done with the data
				delete(a.eventTempMap, id)

				batchedData = append(batchedData, aggregatedEvent)
				if emptyGaurdChan == nil {
					emptyGaurdChan = a.postChan // emptyGuardChan is no longer nil when we add to the batch
				}
			}

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
