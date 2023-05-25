package events_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-state-types/big"
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
	indexerStart := time.Now().Add(-time.Second)
	queryStart := time.Now().Add(-time.Second * 2)
	retrievalStart := time.Now().Add(-time.Second * 3)
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

	em.DispatchEvent(events.Started(time.Now(), id, indexerStart, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid}))
	em.DispatchEvent(events.CandidatesFound(time.Now(), id, indexerStart, cid, []types.RetrievalCandidate{{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerC}, RootCid: cid}}))
	em.DispatchEvent(events.CandidatesFiltered(time.Now(), id, indexerStart, cid, []types.RetrievalCandidate{{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerC}, RootCid: cid}}))
	em.DispatchEvent(events.Started(time.Now(), id, queryStart, types.QueryPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid}))
	em.DispatchEvent(events.Failed(time.Now(), id, queryStart, types.QueryPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid}, "error @ query failure"))
	em.DispatchEvent(events.Started(time.Now(), id, retrievalStart, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}))
	em.DispatchEvent(events.Success(time.Now(), id, retrievalStart, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, 100, 200, time.Second*300, big.NewInt(400), 55, multicodec.TransportGraphsyncFilecoinv1))
	em.DispatchEvent(events.Failed(time.Now(), id, retrievalStart, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, "error @ retrieval failure"))

	time.Sleep(50 * time.Millisecond)

	unregister1()
	unregister2()

	// these should go nowhere and not be counted
	em.DispatchEvent(events.Started(time.Now(), id, indexerStart, types.IndexerPhase, types.RetrievalCandidate{RootCid: cid}))
	em.DispatchEvent(events.CandidatesFound(time.Now(), id, indexerStart, cid, []types.RetrievalCandidate{{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerC}, RootCid: cid}}))
	em.DispatchEvent(events.CandidatesFiltered(time.Now(), id, indexerStart, cid, []types.RetrievalCandidate{{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, {MinerPeer: peer.AddrInfo{ID: peerC}, RootCid: cid}}))
	em.DispatchEvent(events.Started(time.Now(), id, queryStart, types.QueryPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid}))
	em.DispatchEvent(events.Failed(time.Now(), id, queryStart, types.QueryPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerA}, RootCid: cid}, "error @ query failure"))
	em.DispatchEvent(events.Started(time.Now(), id, indexerStart, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}))
	em.DispatchEvent(events.Success(time.Now(), id, retrievalStart, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, 100, 200, time.Second*300, big.NewInt(400), 55, multicodec.TransportGraphsyncFilecoinv1))
	em.DispatchEvent(events.Failed(time.Now(), id, retrievalStart, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerB}, RootCid: cid}, "error @ retrieval failure"))

	select {
	case <-em.Stop():
	case <-time.After(time.Millisecond * 50):
		require.Fail(t, "timed out waiting for event manager to stop")
	}

	verifyEvent := func(list []types.RetrievalEvent, code types.EventCode, phase types.Phase, verify func(types.RetrievalEvent)) {
		var found bool
		for _, e := range list {
			if e.Code() == code && e.Phase() == phase {
				if found {
					require.Fail(t, "found two matching events")
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
		require.IsType(t, events.RetrievalEventStarted{}, event)
		require.Equal(t, id, event.RetrievalId())
		require.Equal(t, cid, event.PayloadCid())
		require.Equal(t, indexerStart, event.PhaseStartTime())
		require.Equal(t, types.IndexerPhase, event.Phase())
		require.Equal(t, types.StartedCode, event.Code())
	}
	verifyEvent(gotEvents1, types.StartedCode, types.IndexerPhase, verifyIndexerStarted)
	verifyEvent(gotEvents2, types.StartedCode, types.IndexerPhase, verifyIndexerStarted)

	verifyIndexerCandidatesFound := func(event types.RetrievalEvent) {
		require.IsType(t, events.RetrievalEventCandidatesFound{}, event)
		require.Equal(t, id, event.RetrievalId())
		require.Equal(t, cid, event.PayloadCid())
		require.Equal(t, indexerStart, event.PhaseStartTime())
		require.Equal(t, types.IndexerPhase, event.Phase())
		require.Equal(t, types.CandidatesFoundCode, event.Code())
		storageProviderIds := event.(events.RetrievalEventCandidatesFound).Candidates()
		require.Len(t, storageProviderIds, 3)
		require.Equal(t, peerA, storageProviderIds[0].MinerPeer.ID)
		require.Equal(t, peerB, storageProviderIds[1].MinerPeer.ID)
		require.Equal(t, peerC, storageProviderIds[2].MinerPeer.ID)
	}
	verifyEvent(gotEvents1, types.CandidatesFoundCode, types.IndexerPhase, verifyIndexerCandidatesFound)
	verifyEvent(gotEvents2, types.CandidatesFoundCode, types.IndexerPhase, verifyIndexerCandidatesFound)

	verifyIndexerCandidatesFiltered := func(event types.RetrievalEvent) {
		require.IsType(t, events.RetrievalEventCandidatesFiltered{}, event)
		require.Equal(t, id, event.RetrievalId())
		require.Equal(t, cid, event.PayloadCid())
		require.Equal(t, indexerStart, event.PhaseStartTime())
		require.Equal(t, types.IndexerPhase, event.Phase())
		require.Equal(t, types.CandidatesFilteredCode, event.Code())
		storageProviderIds := event.(events.RetrievalEventCandidatesFiltered).Candidates()
		require.Len(t, storageProviderIds, 3)
		require.Equal(t, peerA, storageProviderIds[0].MinerPeer.ID)
		require.Equal(t, peerB, storageProviderIds[1].MinerPeer.ID)
		require.Equal(t, peerC, storageProviderIds[2].MinerPeer.ID)
	}
	verifyEvent(gotEvents1, types.CandidatesFilteredCode, types.IndexerPhase, verifyIndexerCandidatesFiltered)
	verifyEvent(gotEvents2, types.CandidatesFilteredCode, types.IndexerPhase, verifyIndexerCandidatesFiltered)

	verifyRetrievalStarted := func(event types.RetrievalEvent) {
		require.IsType(t, events.RetrievalEventStarted{}, event)
		require.Equal(t, id, event.RetrievalId())
		require.Equal(t, cid, event.PayloadCid())
		require.Equal(t, retrievalStart, event.PhaseStartTime())
		require.Equal(t, types.RetrievalPhase, event.Phase())
		require.Equal(t, types.StartedCode, event.Code())
		require.Equal(t, peerB, event.(events.RetrievalEventStarted).StorageProviderId())
	}
	verifyEvent(gotEvents1, types.StartedCode, types.RetrievalPhase, verifyRetrievalStarted)
	verifyEvent(gotEvents2, types.StartedCode, types.RetrievalPhase, verifyRetrievalStarted)

	verifyRetrievalSuccess := func(event types.RetrievalEvent) {
		require.IsType(t, events.RetrievalEventSuccess{}, event)
		require.Equal(t, id, event.RetrievalId())
		require.Equal(t, cid, event.PayloadCid())
		require.Equal(t, retrievalStart, event.PhaseStartTime())
		require.Equal(t, types.RetrievalPhase, event.Phase())
		require.Equal(t, peerB, event.(events.RetrievalEventSuccess).StorageProviderId())
		require.Equal(t, uint64(100), event.(events.RetrievalEventSuccess).ReceivedSize())
		require.Equal(t, uint64(200), event.(events.RetrievalEventSuccess).ReceivedCids())
	}
	verifyEvent(gotEvents1, types.SuccessCode, types.RetrievalPhase, verifyRetrievalSuccess)
	verifyEvent(gotEvents2, types.SuccessCode, types.RetrievalPhase, verifyRetrievalSuccess)

	verifyRetrievalFailure := func(event types.RetrievalEvent) {
		require.IsType(t, events.RetrievalEventFailed{}, event)
		require.Equal(t, id, event.RetrievalId())
		require.Equal(t, cid, event.PayloadCid())
		require.Equal(t, retrievalStart, event.PhaseStartTime())
		require.Equal(t, peerB, event.(events.RetrievalEventFailed).StorageProviderId())
		require.Equal(t, "error @ retrieval failure", event.(events.RetrievalEventFailed).ErrorMessage())
	}
	verifyEvent(gotEvents1, types.FailedCode, types.RetrievalPhase, verifyRetrievalFailure)
	verifyEvent(gotEvents2, types.FailedCode, types.RetrievalPhase, verifyRetrievalFailure)
}
