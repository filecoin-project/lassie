package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
)

var (
	_ types.RetrievalEvent = ExtractionStartedEvent{}
	_ EventWithEndpoint    = ExtractionStartedEvent{}
)

type ExtractionStartedEvent struct {
	providerRetrievalEvent
}

func (e ExtractionStartedEvent) Code() types.EventCode { return types.StartedRetrievalCode }
func (e ExtractionStartedEvent) String() string {
	return fmt.Sprintf("ExtractionStartedEvent<%s, %s, %s>", e.eventTime, e.rootCid, e.endpoint)
}

func ExtractionStarted(at time.Time, rootCid cid.Cid, endpoint string) ExtractionStartedEvent {
	return ExtractionStartedEvent{providerRetrievalEvent{retrievalEvent{at, types.RetrievalID{}, rootCid}, endpoint}}
}

var (
	_ types.RetrievalEvent = ExtractionSucceededEvent{}
	_ EventWithEndpoint    = ExtractionSucceededEvent{}
)

type ExtractionSucceededEvent struct {
	providerRetrievalEvent
	bytesExtracted  uint64
	blocksExtracted uint64
	duration        time.Duration
}

func (e ExtractionSucceededEvent) Code() types.EventCode   { return types.SuccessCode }
func (e ExtractionSucceededEvent) BytesExtracted() uint64  { return e.bytesExtracted }
func (e ExtractionSucceededEvent) BlocksExtracted() uint64 { return e.blocksExtracted }
func (e ExtractionSucceededEvent) Duration() time.Duration { return e.duration }
func (e ExtractionSucceededEvent) String() string {
	return fmt.Sprintf("ExtractionSucceededEvent<%s, %s, %s, %d bytes, %d blocks, %s>",
		e.eventTime, e.rootCid, e.endpoint, e.bytesExtracted, e.blocksExtracted, e.duration)
}

func ExtractionSucceeded(at time.Time, rootCid cid.Cid, endpoint string, bytes, blocks uint64, duration time.Duration) ExtractionSucceededEvent {
	return ExtractionSucceededEvent{
		providerRetrievalEvent: providerRetrievalEvent{retrievalEvent{at, types.RetrievalID{}, rootCid}, endpoint},
		bytesExtracted:         bytes,
		blocksExtracted:        blocks,
		duration:               duration,
	}
}
