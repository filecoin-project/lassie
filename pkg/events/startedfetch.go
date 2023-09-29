package events

import (
	"fmt"
	"time"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
)

var (
	_ types.RetrievalEvent = StartedFetchEvent{}
	_ EventWithProtocols   = StartedFetchEvent{}
)

// StartedFetchEvent signals the start of a Lassie fetch. It is emitted when a fetch is started.
type StartedFetchEvent struct {
	retrievalEvent
	urlPath            string
	supportedProtocols []multicodec.Code
}

func (e StartedFetchEvent) Code() types.EventCode        { return types.StartedFetchCode }
func (e StartedFetchEvent) UrlPath() string              { return e.urlPath }
func (e StartedFetchEvent) Protocols() []multicodec.Code { return e.supportedProtocols }
func (e StartedFetchEvent) String() string {
	return fmt.Sprintf("StartedFetchEvent<%s, %s, %s>", e.eventTime, e.retrievalId, e.rootCid)
}

func StartedFetch(at time.Time, retrievalId types.RetrievalID, rootCid cid.Cid, urlPath string, supportedProtocols ...multicodec.Code) StartedFetchEvent {
	return StartedFetchEvent{retrievalEvent{at, retrievalId, rootCid}, urlPath, supportedProtocols}
}
