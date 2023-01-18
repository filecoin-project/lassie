package events_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestEventManager(t *testing.T) {
	em := events.NewEventManager(context.Background())
	<-em.Start()
	id := types.RetrievalID(uuid.New())
	cid := cid.MustParse("bafkqaalb")
	indexerStart := time.Now().Add(-time.Second)
	queryStart := time.Now().Add(-time.Second * 2)
	retrievalStart := time.Now().Add(-time.Second * 3)
	listener1 := &eventsListener{t: t, id: id, cid: cid, indexerStart: indexerStart, queryStart: queryStart, retrievalStart: retrievalStart}
	listener2 := &eventsListener{t: t, id: id, cid: cid, indexerStart: indexerStart, queryStart: queryStart, retrievalStart: retrievalStart}
	unregister1 := em.RegisterListener(listener1)
	unregister2 := em.RegisterListener(listener2)

	em.FireIndexerProgress(id, cid, indexerStart, events.StartedCode)
	em.FireIndexerCandidates(id, cid, indexerStart, events.StartedCode, []peer.ID{peer.ID("A"), peer.ID("B"), peer.ID("C")})
	em.FireQueryProgress(id, cid, queryStart, peer.ID("A"), events.StartedCode)
	em.FireQueryFailure(id, cid, queryStart, peer.ID("A"), "error @ query failure")
	em.FireQuerySuccess(id, cid, queryStart, peer.ID("A"), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseError, Size: 100, Message: "error @ response"})
	em.FireRetrievalProgress(id, cid, retrievalStart, peer.ID("B"), events.StartedCode)
	em.FireRetrievalSuccess(id, cid, retrievalStart, peer.ID("B"), 100, 200, true)
	em.FireRetrievalFailure(id, cid, retrievalStart, peer.ID("B"), "error @ retrieval failure")

	time.Sleep(50 * time.Millisecond)

	unregister1()
	unregister2()

	// these should go nowhere and not be counted
	em.FireIndexerProgress(id, cid, indexerStart, events.StartedCode)
	em.FireIndexerCandidates(id, cid, indexerStart, events.StartedCode, []peer.ID{peer.ID("A"), peer.ID("B"), peer.ID("C")})
	em.FireQueryProgress(id, cid, queryStart, peer.ID("A"), events.StartedCode)
	em.FireQueryFailure(id, cid, queryStart, peer.ID("A"), "error @ query failure")
	em.FireQuerySuccess(id, cid, queryStart, peer.ID("A"), retrievalmarket.QueryResponse{Status: retrievalmarket.QueryResponseError, Size: 100, Message: "error @ response"})
	em.FireRetrievalProgress(id, cid, retrievalStart, peer.ID("B"), events.StartedCode)
	em.FireRetrievalSuccess(id, cid, retrievalStart, peer.ID("B"), 100, 200, true)
	em.FireRetrievalFailure(id, cid, retrievalStart, peer.ID("B"), "error @ retrieval failure")

	select {
	case <-em.Stop():
	case <-time.After(time.Millisecond * 50):
		require.Fail(t, "timed out waiting for event manager to stop")
	}
	require.Equal(t, 1, listener1.gotIndexerProgress)
	require.Equal(t, 1, listener2.gotIndexerProgress)
	require.Equal(t, 1, listener1.gotIndexerCandidates)
	require.Equal(t, 1, listener2.gotIndexerCandidates)
	require.Equal(t, 1, listener1.gotQueryProgress)
	require.Equal(t, 1, listener2.gotQueryProgress)
	require.Equal(t, 1, listener1.gotQueryFailure)
	require.Equal(t, 1, listener2.gotQueryFailure)
	require.Equal(t, 1, listener1.gotQuerySuccess)
	require.Equal(t, 1, listener2.gotQuerySuccess)
	require.Equal(t, 1, listener1.gotRetrievalProgress)
	require.Equal(t, 1, listener2.gotRetrievalProgress)
	require.Equal(t, 1, listener1.gotRetrievalSuccess)
	require.Equal(t, 1, listener2.gotRetrievalSuccess)
	require.Equal(t, 1, listener1.gotRetrievalFailure)
	require.Equal(t, 1, listener2.gotRetrievalFailure)
}

var _ events.RetrievalEventListener = &eventsListener{}

type eventsListener struct {
	t                    *testing.T
	id                   types.RetrievalID
	cid                  cid.Cid
	indexerStart         time.Time
	queryStart           time.Time
	retrievalStart       time.Time
	gotIndexerProgress   int
	gotIndexerCandidates int
	gotQueryProgress     int
	gotQueryFailure      int
	gotQuerySuccess      int
	gotRetrievalProgress int
	gotRetrievalSuccess  int
	gotRetrievalFailure  int
}

func (el *eventsListener) IndexerProgress(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, stage events.Code) {
	require.Equal(el.t, retrievalId, el.id)
	require.Equal(el.t, requestedCid, el.cid)
	require.Equal(el.t, phaseStartTime, el.indexerStart)
	require.Equal(el.t, events.StartedCode, stage)
	el.gotIndexerProgress++
}

func (el *eventsListener) IndexerCandidates(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, stage events.Code, storageProviderIds []peer.ID) {
	require.Equal(el.t, retrievalId, el.id)
	require.Equal(el.t, requestedCid, el.cid)
	require.Equal(el.t, phaseStartTime, el.indexerStart)
	require.Equal(el.t, events.StartedCode, stage)
	require.Len(el.t, storageProviderIds, 3)
	require.Equal(el.t, peer.ID("A"), storageProviderIds[0])
	require.Equal(el.t, peer.ID("B"), storageProviderIds[1])
	require.Equal(el.t, peer.ID("C"), storageProviderIds[2])
	el.gotIndexerCandidates++
}

func (el *eventsListener) QueryProgress(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage events.Code) {
	require.Equal(el.t, retrievalId, el.id)
	require.Equal(el.t, requestedCid, el.cid)
	require.Equal(el.t, el.queryStart, phaseStartTime)
	require.Equal(el.t, events.StartedCode, stage)
	require.Equal(el.t, peer.ID("A"), storageProviderId)
	el.gotQueryProgress++
}

func (el *eventsListener) QueryFailure(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, errString string) {
	require.Equal(el.t, retrievalId, el.id)
	require.Equal(el.t, requestedCid, el.cid)
	require.Equal(el.t, el.queryStart, phaseStartTime)
	require.Equal(el.t, peer.ID("A"), storageProviderId)
	require.Equal(el.t, "error @ query failure", errString)
	el.gotQueryFailure++
}

func (el *eventsListener) QuerySuccess(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, queryResponse retrievalmarket.QueryResponse) {
	require.Equal(el.t, retrievalId, el.id)
	require.Equal(el.t, requestedCid, el.cid)
	require.Equal(el.t, el.queryStart, phaseStartTime)
	require.Equal(el.t, peer.ID("A"), storageProviderId)
	require.Equal(el.t, retrievalmarket.QueryResponseError, queryResponse.Status)
	require.Equal(el.t, uint64(100), queryResponse.Size)
	require.Equal(el.t, "error @ response", queryResponse.Message)
	el.gotQuerySuccess++
}

func (el *eventsListener) RetrievalProgress(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, stage events.Code) {
	require.Equal(el.t, retrievalId, el.id)
	require.Equal(el.t, requestedCid, el.cid)
	require.Equal(el.t, el.retrievalStart, phaseStartTime)
	require.Equal(el.t, events.StartedCode, stage)
	require.Equal(el.t, peer.ID("B"), storageProviderId)
	el.gotRetrievalProgress++
}

func (el *eventsListener) RetrievalSuccess(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, receivedSize uint64, receivedCids uint64, confirmed bool) {
	require.Equal(el.t, retrievalId, el.id)
	require.Equal(el.t, requestedCid, el.cid)
	require.Equal(el.t, el.retrievalStart, phaseStartTime)
	require.Equal(el.t, peer.ID("B"), storageProviderId)
	require.Equal(el.t, uint64(100), receivedSize)
	require.Equal(el.t, uint64(200), receivedCids)
	require.True(el.t, confirmed)
	el.gotRetrievalSuccess++
}

func (el *eventsListener) RetrievalFailure(retrievalId types.RetrievalID, phaseStartTime, eventTime time.Time, requestedCid cid.Cid, storageProviderId peer.ID, errString string) {
	require.Equal(el.t, retrievalId, el.id)
	require.Equal(el.t, requestedCid, el.cid)
	require.Equal(el.t, el.retrievalStart, phaseStartTime)
	require.Equal(el.t, peer.ID("B"), storageProviderId)
	require.Equal(el.t, "error @ retrieval failure", errString)
	el.gotRetrievalFailure++
}
