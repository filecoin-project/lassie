package aggregateeventrecorder_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"context"

	"github.com/filecoin-project/go-clock"
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
)

func TestAggregateEventRecorder(t *testing.T) {
	type gotReq struct {
		path string
		auth string
		node datamodel.Node
	}
	authHeaderValue := "applesauce"
	testPath := "/test-path/here"

	receivedChan := make(chan gotReq, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		t.Logf("got: %s", string(got))
		req, err := ipld.Decode(got, dagjson.Decode)
		require.NoError(t, err)
		receivedChan <- gotReq{r.URL.Path, r.Header.Get("Authorization"), req}
	}))
	defer ts.Close()

	testCid1 := testutil.GenerateCid()
	bitswapCandidates := testutil.GenerateRetrievalCandidatesForCID(t, 3, testCid1, &metadata.Bitswap{})
	graphsyncCandidates := testutil.GenerateRetrievalCandidatesForCID(t, 3, testCid1, &metadata.GraphsyncFilecoinV1{})

	tests := []struct {
		name string
		exec func(t *testing.T, ctx context.Context, subscriber types.RetrievalEventSubscriber, id types.RetrievalID)
	}{
		{
			name: "Retrieval Success",
			exec: func(t *testing.T, ctx context.Context, subscriber types.RetrievalEventSubscriber, id types.RetrievalID) {
				clock := clock.NewMock()
				fetchStartTime := clock.Now()
				subscriber(events.StartedFetch(clock.Now(), id, testCid1, "/applesauce", multicodec.TransportGraphsyncFilecoinv1, multicodec.TransportBitswap))
				clock.Add(10 * time.Millisecond)
				subscriber(events.StartedFindingCandidates(clock.Now(), id, testCid1))
				subscriber(events.CandidatesFound(clock.Now(), id, testCid1, graphsyncCandidates))
				subscriber(events.CandidatesFiltered(clock.Now(), id, testCid1, graphsyncCandidates[:2]))
				subscriber(events.StartedRetrieval(clock.Now(), id, graphsyncCandidates[0], multicodec.TransportGraphsyncFilecoinv1))
				subscriber(events.StartedRetrieval(clock.Now(), id, graphsyncCandidates[1], multicodec.TransportGraphsyncFilecoinv1))
				clock.Add(10 * time.Millisecond)
				subscriber(events.CandidatesFound(clock.Now(), id, testCid1, bitswapCandidates))
				subscriber(events.CandidatesFiltered(clock.Now(), id, testCid1, bitswapCandidates[:2]))
				bitswapPeer := types.NewRetrievalCandidate(peer.ID(""), nil, testCid1, &metadata.Bitswap{})
				subscriber(events.StartedRetrieval(clock.Now(), id, bitswapPeer, multicodec.TransportBitswap))
				clock.Add(20 * time.Millisecond)
				subscriber(events.FirstByte(clock.Now(), id, bitswapPeer, 20*time.Millisecond, multicodec.TransportBitswap))
				subscriber(events.BlockReceived(clock.Now(), id, bitswapCandidates[0], multicodec.TransportBitswap, 3000))
				subscriber(events.BlockReceived(clock.Now(), id, bitswapCandidates[0], multicodec.TransportBitswap, 2000))
				subscriber(events.FailedRetrieval(clock.Now(), id, graphsyncCandidates[0], multicodec.TransportGraphsyncFilecoinv1, "failed to dial"))
				clock.Add(20 * time.Millisecond)
				subscriber(events.FirstByte(clock.Now(), id, graphsyncCandidates[1], 50*time.Millisecond, multicodec.TransportGraphsyncFilecoinv1))
				clock.Add(30 * time.Millisecond)
				subscriber(events.BlockReceived(clock.Now(), id, graphsyncCandidates[1], multicodec.TransportGraphsyncFilecoinv1, 3000))
				subscriber(events.BlockReceived(clock.Now(), id, graphsyncCandidates[1], multicodec.TransportGraphsyncFilecoinv1, 2000))

				subscriber(events.BlockReceived(clock.Now(), id, bitswapCandidates[1], multicodec.TransportBitswap, 5000))
				subscriber(events.Success(clock.Now(), id, bitswapPeer, uint64(10000), 3030, 4*time.Second, multicodec.TransportBitswap))
				subscriber(events.Finished(clock.Now(), id, bitswapPeer))

				var req gotReq
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case req = <-receivedChan:
				}
				require.Equal(t, testPath, req.path)
				require.Equal(t, "Basic applesauce", req.auth)

				require.Equal(t, int64(1), req.node.Length())
				eventList := verifyListNode(t, req.node, "events", 1)
				event := verifyListElement(t, eventList, 0)
				// require.Equal(t, int64(18), event.Length())
				verifyStringNode(t, event, "instanceId", "test-instance")
				verifyStringNode(t, event, "retrievalId", id.String())
				verifyStringNode(t, event, "rootCid", testCid1.String())
				verifyStringNode(t, event, "urlPath", "/applesauce")
				verifyStringNode(t, event, "storageProviderId", types.BitswapIndentifier)
				verifyStringNode(t, event, "timeToFirstByte", "40ms")
				verifyStringNode(t, event, "timeToFirstIndexerResult", "10ms")
				verifyIntNode(t, event, "bandwidth", 200000)
				verifyBoolNode(t, event, "success", true)
				verifyStringNode(t, event, "startTime", fetchStartTime.Format(time.RFC3339Nano))
				verifyStringNode(t, event, "endTime", fetchStartTime.Add(90*time.Millisecond).Format(time.RFC3339Nano))

				verifyIntNode(t, event, "indexerCandidatesReceived", 6)
				verifyIntNode(t, event, "indexerCandidatesFiltered", 4)
				protocolsAllowed := verifyListNode(t, event, "protocolsAllowed", 2)
				verifyStringListElementsMatch(t, protocolsAllowed, []string{"transport-graphsync-filecoinv1", "transport-bitswap"})
				protocolsAttempted := verifyListNode(t, event, "protocolsAttempted", 2)
				verifyStringListElementsMatch(t, protocolsAttempted, []string{"transport-graphsync-filecoinv1", "transport-bitswap"})
				verifyStringNode(t, event, "protocolSucceeded", "transport-bitswap")
				retrievalAttempts, err := event.LookupByString("retrievalAttempts")
				require.NoError(t, err)
				require.Equal(t, int64(5), retrievalAttempts.Length())
				sp1Attempt, err := retrievalAttempts.LookupByString(graphsyncCandidates[0].MinerPeer.ID.String())
				require.NoError(t, err)
				require.Equal(t, int64(2), sp1Attempt.Length())
				verifyStringNode(t, sp1Attempt, "protocol", multicodec.TransportGraphsyncFilecoinv1.String())
				verifyStringNode(t, sp1Attempt, "error", "failed to dial")
				sp2Attempt, err := retrievalAttempts.LookupByString(graphsyncCandidates[1].MinerPeer.ID.String())
				require.NoError(t, err)
				require.Equal(t, int64(3), sp2Attempt.Length())
				verifyStringNode(t, sp2Attempt, "protocol", multicodec.TransportGraphsyncFilecoinv1.String())
				verifyStringNode(t, sp2Attempt, "timeToFirstByte", "50ms")
				verifyIntNode(t, sp2Attempt, "bytesTransferred", 5000)
				bitswapAttempt, err := retrievalAttempts.LookupByString(types.BitswapIndentifier)
				require.NoError(t, err)
				require.Equal(t, int64(3), bitswapAttempt.Length())
				verifyStringNode(t, bitswapAttempt, "timeToFirstByte", "20ms")
				verifyStringNode(t, bitswapAttempt, "protocol", multicodec.TransportBitswap.String())
				verifyIntNode(t, bitswapAttempt, "bytesTransferred", 10000)
				bitswapPeer1Attempt, err := retrievalAttempts.LookupByString(bitswapCandidates[0].MinerPeer.ID.String())
				require.NoError(t, err)
				require.Equal(t, int64(2), bitswapPeer1Attempt.Length())
				verifyIntNode(t, bitswapPeer1Attempt, "bytesTransferred", 5000)
				verifyStringNode(t, bitswapPeer1Attempt, "protocol", multicodec.TransportBitswap.String())
				bitswapPeer2Attempt, err := retrievalAttempts.LookupByString(bitswapCandidates[1].MinerPeer.ID.String())
				require.NoError(t, err)
				require.Equal(t, int64(2), bitswapPeer2Attempt.Length())
				verifyIntNode(t, bitswapPeer2Attempt, "bytesTransferred", 5000)
				verifyStringNode(t, bitswapPeer2Attempt, "protocol", multicodec.TransportBitswap.String())
			},
		},
		{
			name: "Retrieval Failure, Never Reached First Byte",
			exec: func(t *testing.T, ctx context.Context, subscriber types.RetrievalEventSubscriber, id types.RetrievalID) {
				clock := clock.NewMock()
				fetchStartTime := clock.Now()
				subscriber(events.StartedFetch(clock.Now(), id, testCid1, "/applesauce"))
				subscriber(events.Finished(clock.Now(), id, types.RetrievalCandidate{RootCid: testCid1}))

				var req gotReq
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case req = <-receivedChan:
				}
				require.Equal(t, testPath, req.path)
				require.Equal(t, "Basic applesauce", req.auth)

				require.Equal(t, int64(1), req.node.Length())
				eventList := verifyListNode(t, req.node, "events", 1)
				event := verifyListElement(t, eventList, 0)
				require.Equal(t, int64(9), event.Length())
				verifyStringNode(t, event, "instanceId", "test-instance")
				verifyStringNode(t, event, "retrievalId", id.String())
				verifyStringNode(t, event, "rootCid", testCid1.String())
				verifyStringNode(t, event, "urlPath", "/applesauce")
				verifyBoolNode(t, event, "success", false)
				verifyStringNode(t, event, "startTime", fetchStartTime.Format(time.RFC3339Nano))
				verifyStringNode(t, event, "endTime", fetchStartTime.Format(time.RFC3339Nano))
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
					EndpointURL:           ts.URL + testPath,
					EndpointAuthorization: authHeaderValue,
				},
			).RetrievalEventSubscriber()
			id, err := types.NewRetrievalID()
			require.NoError(t, err)
			test.exec(t, ctx, subscriber, id)
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
