package aggregateeventrecorder_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/aggregateeventrecorder"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func TestAggregateEventRecorder(t *testing.T) {
	var req datamodel.Node
	var path string
	receivedChan := make(chan bool, 1)
	authHeaderValue := "applesauce"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		req, err = ipld.DecodeStreaming(r.Body, dagjson.Decode)
		require.NoError(t, err)
		path = r.URL.Path
		require.Equal(t, "Basic applesauce", r.Header.Get("Authorization"))
		receivedChan <- true
	}))
	defer ts.Close()

	testCid1 := testutil.GenerateCid()
	bitswapCandidates := testutil.GenerateRetrievalCandidatesForCID(t, 2, testCid1, &metadata.Bitswap{})
	graphsyncCandidates := testutil.GenerateRetrievalCandidatesForCID(t, 3, testCid1, &metadata.GraphsyncFilecoinV1{})

	tests := []struct {
		name string
		exec func(t *testing.T, ctx context.Context, subscriber types.RetrievalEventSubscriber, id types.RetrievalID)
	}{
		{
			name: "Retrieval Success",
			exec: func(t *testing.T, ctx context.Context, subscriber types.RetrievalEventSubscriber, id types.RetrievalID) {
				clock := clock.NewMock()

				subscriber(events.StartedFetch(clock.Now(), id, clock.Now(), testCid1, "/applesauce", multicodec.TransportGraphsyncFilecoinv1, multicodec.TransportBitswap))
				fetchPhaseStartTime := clock.Now()
				indexerStartTime := clock.Now()
				clock.Add(10 * time.Millisecond)
				subscriber(events.CandidatesFound(clock.Now(), id, indexerStartTime, testCid1, graphsyncCandidates))
				subscriber(events.CandidatesFiltered(clock.Now(), id, indexerStartTime, testCid1, graphsyncCandidates[:2]))
				subscriber(events.Started(clock.Now(), id, clock.Now(), types.RetrievalPhase, graphsyncCandidates[0], multicodec.TransportGraphsyncFilecoinv1))
				subscriber(events.Started(clock.Now(), id, clock.Now(), types.RetrievalPhase, graphsyncCandidates[1], multicodec.TransportGraphsyncFilecoinv1))
				graphsyncCandidateStartTime := clock.Now()
				clock.Add(10 * time.Millisecond)
				subscriber(events.CandidatesFound(clock.Now(), id, indexerStartTime, testCid1, bitswapCandidates[:2]))
				subscriber(events.CandidatesFiltered(clock.Now(), id, indexerStartTime, testCid1, bitswapCandidates[:1]))
				bitswapPeer := types.NewRetrievalCandidate(peer.ID(""), nil, testCid1, &metadata.Bitswap{})
				subscriber(events.Started(clock.Now(), id, clock.Now(), types.RetrievalPhase, bitswapPeer, multicodec.TransportBitswap))
				bitswapCandidateStartTime := clock.Now()
				clock.Add(20 * time.Millisecond)
				subscriber(events.FirstByte(clock.Now(), id, bitswapCandidateStartTime, bitswapPeer, 20*time.Millisecond))
				subscriber(events.Failed(clock.Now(), id, graphsyncCandidateStartTime, types.RetrievalPhase, graphsyncCandidates[0], "failed to dial"))
				clock.Add(20 * time.Millisecond)
				subscriber(events.FirstByte(clock.Now(), id, graphsyncCandidateStartTime, graphsyncCandidates[1], 50*time.Millisecond))
				clock.Add(30 * time.Millisecond)
				subscriber(events.Success(clock.Now(), id, bitswapCandidateStartTime, bitswapPeer, uint64(10000), 3030, 4*time.Second, big.Zero(), 55, multicodec.TransportBitswap))
				subscriber(events.Finished(clock.Now(), id, fetchPhaseStartTime, bitswapPeer))

				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-receivedChan:
				}

				require.Equal(t, int64(1), req.Length())
				eventList := verifyListNode(t, req, "events", 1)
				event := verifyListElement(t, eventList, 0)
				require.Equal(t, int64(16), event.Length())
				verifyStringNode(t, event, "instanceId", "test-instance")
				verifyStringNode(t, event, "retrievalId", id.String())
				verifyStringNode(t, event, "storageProviderId", types.BitswapIndentifier)
				verifyStringNode(t, event, "timeToFirstByte", "40ms")
				verifyStringNode(t, event, "timeToFirstIndexerResult", "10ms")
				verifyIntNode(t, event, "bandwidth", 200000)
				verifyBoolNode(t, event, "success", true)
				verifyStringNode(t, event, "startTime", fetchPhaseStartTime.Format(time.RFC3339Nano))
				verifyStringNode(t, event, "endTime", fetchPhaseStartTime.Add(90*time.Millisecond).Format(time.RFC3339Nano))

				verifyIntNode(t, event, "indexerCandidatesReceived", 5)
				verifyIntNode(t, event, "indexerCandidatesFiltered", 3)
				protocolsAllowed := verifyListNode(t, event, "protocolsAllowed", 2)
				verifyStringListElementsMatch(t, protocolsAllowed, []string{"transport-graphsync-filecoinv1", "transport-bitswap"})
				protocolsAttempted := verifyListNode(t, event, "protocolsAttempted", 2)
				verifyStringListElementsMatch(t, protocolsAttempted, []string{"transport-graphsync-filecoinv1", "transport-bitswap"})
				verifyStringNode(t, event, "protocolSucceeded", "transport-bitswap")
				retrievalAttempts, err := event.LookupByString("retrievalAttempts")
				require.NoError(t, err)
				require.Equal(t, int64(3), retrievalAttempts.Length())
				sp1Attempt, err := retrievalAttempts.LookupByString(graphsyncCandidates[0].MinerPeer.ID.String())
				require.NoError(t, err)
				require.Equal(t, int64(2), sp1Attempt.Length())
				verifyStringNode(t, sp1Attempt, "protocol", multicodec.TransportGraphsyncFilecoinv1.String())
				verifyStringNode(t, sp1Attempt, "error", "failed to dial")
				sp2Attempt, err := retrievalAttempts.LookupByString(graphsyncCandidates[1].MinerPeer.ID.String())
				require.NoError(t, err)
				require.Equal(t, int64(2), sp2Attempt.Length())
				verifyStringNode(t, sp2Attempt, "protocol", multicodec.TransportGraphsyncFilecoinv1.String())
				verifyStringNode(t, sp2Attempt, "timeToFirstByte", "50ms")
				bitswapAttempt, err := retrievalAttempts.LookupByString(types.BitswapIndentifier)
				require.NoError(t, err)
				require.Equal(t, int64(2), bitswapAttempt.Length())
				verifyStringNode(t, bitswapAttempt, "timeToFirstByte", "20ms")
				verifyStringNode(t, bitswapAttempt, "protocol", multicodec.TransportBitswap.String())

			},
		},
		{
			name: "Retrieval Failure, Never Reached First Byte",
			exec: func(t *testing.T, ctx context.Context, subscriber types.RetrievalEventSubscriber, id types.RetrievalID) {
				clock := clock.NewMock()
				fetchPhaseStartTime := clock.Now()
				subscriber(events.StartedFetch(clock.Now(), id, fetchPhaseStartTime, testCid1, "/applesauce"))
				subscriber(events.Finished(clock.Now(), id, fetchPhaseStartTime, types.RetrievalCandidate{RootCid: testCid1}))

				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-receivedChan:
				}

				require.Equal(t, int64(1), req.Length())
				eventList := verifyListNode(t, req, "events", 1)
				event := verifyListElement(t, eventList, 0)
				require.Equal(t, int64(7), event.Length())
				verifyStringNode(t, event, "instanceId", "test-instance")
				verifyStringNode(t, event, "retrievalId", id.String())
				verifyBoolNode(t, event, "success", false)
				verifyStringNode(t, event, "startTime", fetchPhaseStartTime.Format(time.RFC3339Nano))
				verifyStringNode(t, event, "endTime", fetchPhaseStartTime.Format(time.RFC3339Nano))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			subscriber := aggregateeventrecorder.NewAggregateEventRecorder(
				ctx,
				aggregateeventrecorder.EventRecorderConfig{
					InstanceID:            "test-instance",
					EndpointURL:           fmt.Sprintf("%s/test-path/here", ts.URL),
					EndpointAuthorization: authHeaderValue,
				},
			).RetrievalEventSubscriber()
			id, err := types.NewRetrievalID()
			require.NoError(t, err)
			test.exec(t, ctx, subscriber, id)
			require.NotNil(t, req)
			require.Equal(t, "/test-path/here", path)
		})
	}
}

func verifyListNode(t *testing.T, node datamodel.Node, key string, expectedLength int64) datamodel.Node {
	subNode, err := node.LookupByString(key)
	require.NoError(t, err)
	require.Equal(t, datamodel.Kind_List, subNode.Kind())
	require.Equal(t, int64(expectedLength), subNode.Length())
	return subNode
}

func verifyListElement(t *testing.T, node datamodel.Node, index int64) datamodel.Node {
	element, err := node.LookupByIndex(index)
	require.NoError(t, err)
	return element
}

func verifyStringListElementsMatch(t *testing.T, node datamodel.Node, expected []string) {
	iter := node.ListIterator()
	var received []string
	for !iter.Done() {
		_, element, err := iter.Next()
		require.NoError(t, err)
		str, err := element.AsString()
		require.NoError(t, err)
		received = append(received, str)
	}
	require.ElementsMatch(t, expected, received)
}

func verifyStringNode(t *testing.T, node datamodel.Node, key string, expected string) {
	str := nodeToString(t, node, key)
	require.Equal(t, expected, str)
}

func nodeToString(t *testing.T, node datamodel.Node, key string) string {
	subNode, err := node.LookupByString(key)
	require.NoError(t, err)
	str, err := subNode.AsString()
	require.NoError(t, err)
	return str
}

func verifyBoolNode(t *testing.T, node datamodel.Node, key string, expected bool) {
	subNode, err := node.LookupByString(key)
	require.NoError(t, err)
	ii, err := subNode.AsBool()
	require.NoError(t, err)
	require.Equal(t, expected, ii)
}

func verifyIntNode(t *testing.T, node datamodel.Node, key string, expected int64) {
	subNode, err := node.LookupByString(key)
	require.NoError(t, err)
	ii, err := subNode.AsInt()
	require.NoError(t, err)
	require.Equal(t, expected, ii)
}

var result bool

func BenchmarkAggregateEventRecorderSubscriber(b *testing.B) {
	receivedChan := make(chan bool, 1)
	authHeaderValue := "applesauce"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedChan <- true
	}))
	defer ts.Close()

	ctx := context.Background()
	subscriber := aggregateeventrecorder.NewAggregateEventRecorder(
		ctx,
		aggregateeventrecorder.EventRecorderConfig{
			InstanceID:            "test-instance",
			EndpointURL:           fmt.Sprintf("%s/test-path/here", ts.URL),
			EndpointAuthorization: authHeaderValue,
		},
	).RetrievalEventSubscriber()
	id, _ := types.NewRetrievalID()
	fetchStartTime := time.Now()
	ptime := time.Now().Add(time.Hour * -1)
	spid := peer.ID("A")
	testCid1 := testutil.GenerateCid()
	var success bool
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		subscriber(events.Started(time.Now(), id, fetchStartTime, types.FetchPhase, types.NewRetrievalCandidate(spid, nil, testCid1)))
		subscriber(events.FirstByte(time.Now(), id, ptime, types.NewRetrievalCandidate(spid, nil, testCid1), 2*time.Millisecond))
		subscriber(events.Success(time.Now(), id, ptime, types.NewRetrievalCandidate(spid, nil, testCid1), uint64(2020), 3030, 4*time.Second, big.Zero(), 55, multicodec.TransportGraphsyncFilecoinv1))
		subscriber(events.Finished(time.Now(), id, fetchStartTime, types.RetrievalCandidate{RootCid: testCid1}))
		b.StopTimer()

		select {
		case <-ctx.Done():
			b.Fatal(ctx.Err())
		case result := <-receivedChan:
			success = result
		}
	}
	result = success
}
