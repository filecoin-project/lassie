package httpserver

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	servertiming "github.com/mitchellh/go-server-timing"
)

// servertimingsSubscriber is a retrieval event subscriber that records
// RetrievalEvents and generates a Server-Timing header on an http request.
// The "dur" field is the duration since the "started-fetch" event in milliseconds.
// The extra fields are the other events that took place and their durations in
// nanoseconds since the "started-fetch" event.
//
// We are unable to get the duration of the entire fetch or successful retrievals
// due to the way in which the headers are written. Since the headers are written
// before an http `Write` occurs, we can only collect info about the retrievals
// until a `first-byte-received` event that results in data being written to the client.
// The http `Write` ends up occurring before the success and finished events are emitted,
// therefore cutting off the trailing events that occur for any given retrieval.
// Because of this, the `started-fetch`, `success`, and `finished` events are not processed.
//
// Additionally, we use the `dur` field to record the time since the `started-fetch` event
// instead of the duration of the event itself. We do this to render something in the browser
// since the "dur" field is the only field rendered.
func servertimingsSubscriber(req *http.Request) types.RetrievalEventSubscriber {
	var fetchStartTime time.Time

	var candidateFindingMetric *servertiming.Metric
	var candidateFindingStartTime time.Time

	retrievalMetricMap := make(map[string]*servertiming.Metric)
	retrievalTimingMap := make(map[string]time.Time)

	return func(re types.RetrievalEvent) {
		timing := servertiming.FromContext(req.Context())
		if timing == nil {
			return
		}

		timing.Lock()
		defer timing.Unlock()

		switch event := re.(type) {
		case events.StartedFetchEvent:
			fetchStartTime = re.Time()

		// Candidate finding cases
		case events.StartedFindingCandidatesEvent:
			timing.Unlock()
			candidateFindingMetric = timing.NewMetric(string(re.Code()))
			timing.Lock()
			candidateFindingMetric.Extra = make(map[string]string)
			candidateFindingMetric.Extra["dur"] = formatDuration(re.Time().Sub(fetchStartTime))
			candidateFindingStartTime = re.Time()
		case events.CandidatesFoundEvent:
			candidateFindingMetric.Duration = re.Time().Sub(candidateFindingStartTime)
			candidateFindingMetric.Extra[string(re.Code())] = fmt.Sprintf("%d", re.Time().Sub(candidateFindingStartTime))
		case events.CandidatesFilteredEvent:
			candidateFindingMetric.Duration = re.Time().Sub(candidateFindingStartTime)
			candidateFindingMetric.Extra[string(re.Code())] = fmt.Sprintf("%d", re.Time().Sub(candidateFindingStartTime))

		// Retrieval cases
		case events.StartedRetrievalEvent:
			name := fmt.Sprintf("retrieval-%s", events.Identifier(re))
			timing.Unlock()
			retrievalMetric := timing.NewMetric(name)
			timing.Lock()
			retrievalMetric.Extra = make(map[string]string)
			// We're using "dur" here to render the time since the "started-fetch" in the browser
			retrievalMetric.Extra["dur"] = formatDuration(re.Time().Sub(fetchStartTime))
			retrievalMetricMap[name] = retrievalMetric
			retrievalTimingMap[name] = re.Time()
		case events.FirstByteEvent:
			name := fmt.Sprintf("retrieval-%s", events.Identifier(event))
			if retrievalMetric, ok := retrievalMetricMap[name]; ok {
				retrievalMetric.Extra[string(re.Code())] = fmt.Sprintf("%d", re.Time().Sub(retrievalTimingMap[name]))
			}
		case events.FailedRetrievalEvent:
			name := fmt.Sprintf("retrieval-%s", events.Identifier(re))
			if retrievalMetric, ok := retrievalMetricMap[name]; ok {
				retrievalMetric.Extra[string(re.Code())] = fmt.Sprintf("%d", re.Time().Sub(retrievalTimingMap[name]))
				retrievalMetric.Duration = re.Time().Sub(retrievalTimingMap[name])
			}

		// Due to the timing in which the Server-Timing header is written,
		// the success and finished events are never emitted in time to make it into the header.
		case events.SucceededEvent:
		case events.FinishedEvent:

		default:
			if event, ok := re.(events.EventWithProviderID); ok {
				name := fmt.Sprintf("retrieval-%s", events.Identifier(event))
				if retrievalMetric, ok := retrievalMetricMap[name]; ok {
					retrievalMetric.Extra[string(re.Code())] = fmt.Sprintf("%d", re.Time().Sub(retrievalTimingMap[name]))
				}
			}
		}
	}
}

// formatDuration formats a duration in milliseconds in the same way the go-server-timing library does.
func formatDuration(d time.Duration) string {
	return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', -1, 64)
}
