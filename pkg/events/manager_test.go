package events_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestEventManager(t *testing.T) {
	em := events.NewEventManager(context.Background())
	em.Start()
	id := types.RetrievalID(uuid.New())
	cid := cid.MustParse("bafkqaalb")
	peerA := peer.ID("A")
	peerB := peer.ID("B")
	peerC := peer.ID("C")
	gotEvents1 := make([]types.RetrievalEvent, 0)
	gotEvents2 := make([]types.RetrievalEvent, 0)
	subscriber1 := func(event types.RetrievalEvent) {
		gotEvents1 = append(gotEvents1, event)
	}
	subscriber2 := func(event types.RetrievalEvent) {
		gotEvents2 = append(gotEvents2, event)
	}
	unregister1 := em.RegisterSubscriber(subscriber1)
	unregister2 := em.RegisterSubscriber(subscriber2)

	em.DispatchEvent(events.StartedFetch(time.Now(), id, cid, "", multicodec.TransportGraphsyncFilecoinv1))
	em.DispatchEvent(events.StartedFindingCandidates(time.Now(), id, cid))
	em.DispatchEvent(events.CandidatesFound(time.Now(), id, cid, []types.RetrievalCandidate{{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerC}, RootCid: cid}}))
	em.DispatchEvent(events.CandidatesFiltered(time.Now(), id, cid, []types.RetrievalCandidate{{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerC}, RootCid: cid}}))
	em.DispatchEvent(events.StartedRetrieval(time.Now(), id, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, multicodec.TransportGraphsyncFilecoinv1))
	em.DispatchEvent(events.Success(time.Now(), id, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, 100, 200, time.Second*300, multicodec.TransportGraphsyncFilecoinv1))
	em.DispatchEvent(events.FailedRetrieval(time.Now(), id, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, multicodec.TransportGraphsyncFilecoinv1, "error @ retrieval failure"))

	time.Sleep(50 * time.Millisecond)

	unregister1()
	unregister2()

	// these should go nowhere and not be counted
	em.DispatchEvent(events.StartedFetch(time.Now(), id, cid, "", multicodec.TransportGraphsyncFilecoinv1))
	em.DispatchEvent(events.StartedFindingCandidates(time.Now(), id, cid))
	em.DispatchEvent(events.CandidatesFound(time.Now(), id, cid, []types.RetrievalCandidate{{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerC}, RootCid: cid}}))
	em.DispatchEvent(events.CandidatesFiltered(time.Now(), id, cid, []types.RetrievalCandidate{{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerC}, RootCid: cid}}))
	em.DispatchEvent(events.StartedRetrieval(time.Now(), id, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, multicodec.TransportGraphsyncFilecoinv1))
	em.DispatchEvent(events.Success(time.Now(), id, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, 100, 200, time.Second*300, multicodec.TransportGraphsyncFilecoinv1))
	em.DispatchEvent(events.FailedRetrieval(time.Now(), id, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, multicodec.TransportGraphsyncFilecoinv1, "error @ retrieval failure"))

	select {
	case <-em.Stop():
	case <-time.After(time.Millisecond * 50):
		require.Fail(t, "timed out waiting for event manager to stop")
	}

	verifyEvent := func(list []types.RetrievalEvent, code types.EventCode, verify func(types.RetrievalEvent)) {
		var found bool
		for _, e := range list {
			if e.Code() == code {
				if found {
					require.Fail(t, "found more than one event")
				}

				found = true
				verify(e)
			}
		}
		if !found {
			require.Fail(t, "can't find event")
		}
	}

	verifyIndexerStarted := func(event types.RetrievalEvent) {
		require.IsType(t, events.StartedFindingCandidatesEvent{}, event)
		require.Equal(t, id, event.RetrievalId())
		require.Equal(t, cid, event.RootCid())
		require.Equal(t, types.StartedFindingCandidatesCode, event.Code())
	}
	verifyEvent(gotEvents1, types.StartedFindingCandidatesCode, verifyIndexerStarted)
	verifyEvent(gotEvents2, types.StartedFindingCandidatesCode, verifyIndexerStarted)

	verifyIndexerCandidatesFound := func(event types.RetrievalEvent) {
		require.IsType(t, events.CandidatesFoundEvent{}, event)
		require.Equal(t, id, event.RetrievalId())
		require.Equal(t, cid, event.RootCid())
		require.Equal(t, types.CandidatesFoundCode, event.Code())
		storageProviderIds := event.(events.CandidatesFoundEvent).Candidates()
		require.Len(t, storageProviderIds, 3)
		require.Equal(t, peerA, storageProviderIds[0].MinerPeer.ID)
		require.Equal(t, peerB, storageProviderIds[1].MinerPeer.ID)
		require.Equal(t, peerC, storageProviderIds[2].MinerPeer.ID)
	}
	verifyEvent(gotEvents1, types.CandidatesFoundCode, verifyIndexerCandidatesFound)
	verifyEvent(gotEvents2, types.CandidatesFoundCode, verifyIndexerCandidatesFound)

	verifyIndexerCandidatesFiltered := func(event types.RetrievalEvent) {
		require.IsType(t, events.CandidatesFilteredEvent{}, event)
		require.Equal(t, id, event.RetrievalId())
		require.Equal(t, cid, event.RootCid())
		require.Equal(t, types.CandidatesFilteredCode, event.Code())
		storageProviderIds := event.(events.CandidatesFilteredEvent).Candidates()
		require.Len(t, storageProviderIds, 3)
		require.Equal(t, peerA, storageProviderIds[0].MinerPeer.ID)
		require.Equal(t, peerB, storageProviderIds[1].MinerPeer.ID)
		require.Equal(t, peerC, storageProviderIds[2].MinerPeer.ID)
	}
	verifyEvent(gotEvents1, types.CandidatesFilteredCode, verifyIndexerCandidatesFiltered)
	verifyEvent(gotEvents2, types.CandidatesFilteredCode, verifyIndexerCandidatesFiltered)

	verifyRetrievalStarted := func(event types.RetrievalEvent) {
		require.IsType(t, events.StartedRetrievalEvent{}, event)
		require.Equal(t, id, event.RetrievalId())
		require.Equal(t, cid, event.RootCid())
		require.Equal(t, types.StartedRetrievalCode, event.Code())
		require.Equal(t, peerB, event.(events.StartedRetrievalEvent).ProviderId())
	}
	verifyEvent(gotEvents1, types.StartedRetrievalCode, verifyRetrievalStarted)
	verifyEvent(gotEvents2, types.StartedRetrievalCode, verifyRetrievalStarted)

	verifyRetrievalSuccess := func(event types.RetrievalEvent) {
		require.IsType(t, events.SucceededEvent{}, event)
		require.Equal(t, id, event.RetrievalId())
		require.Equal(t, cid, event.RootCid())

		successEvent := event.(events.SucceededEvent)
		require.Equal(t, peerB, successEvent.ProviderId())
		require.Equal(t, uint64(100), successEvent.ReceivedBytesSize())
		require.Equal(t, uint64(200), successEvent.ReceivedCidsCount())
	}
	verifyEvent(gotEvents1, types.SuccessCode, verifyRetrievalSuccess)
	verifyEvent(gotEvents2, types.SuccessCode, verifyRetrievalSuccess)

	verifyRetrievalFailure := func(event types.RetrievalEvent) {
		require.IsType(t, events.FailedRetrievalEvent{}, event)
		require.Equal(t, id, event.RetrievalId())
		require.Equal(t, cid, event.RootCid())

		failedEvent := event.(events.FailedRetrievalEvent)
		require.Equal(t, peerB, failedEvent.ProviderId())
		require.Equal(t, "error @ retrieval failure", failedEvent.ErrorMessage())
	}
	verifyEvent(gotEvents1, types.FailedRetrievalCode, verifyRetrievalFailure)
	verifyEvent(gotEvents2, types.FailedRetrievalCode, verifyRetrievalFailure)
}
