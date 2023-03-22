package aggregateeventrecorder_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/aggregateeventrecorder"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

var testCid1 = mustCid("bafybeihrqe2hmfauph5yfbd6ucv7njqpiy4tvbewlvhzjl4bhnyiu6h7pm")

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

	tests := []struct {
		name string
		exec func(t *testing.T, ctx context.Context, subscriber types.RetrievalEventSubscriber, id types.RetrievalID, etime, ptime time.Time, spid peer.ID)
	}{
		{
			name: "Retrieval Success",
			exec: func(t *testing.T, ctx context.Context, subscriber types.RetrievalEventSubscriber, id types.RetrievalID, etime, ptime time.Time, spid peer.ID) {
				fetchPhaseStartTime := ptime.Add(-10 * time.Second)

				subscriber(events.Started(time.Now(), id, fetchPhaseStartTime, types.FetchPhase, types.RetrievalCandidate{RootCid: testCid1}))
				subscriber(events.FirstByte(time.Now(), id, ptime, types.RetrievalCandidate{RootCid: testCid1}))
				subscriber(events.Success(time.Now().Add(50*time.Millisecond), id, ptime, types.NewRetrievalCandidate(spid, testCid1), uint64(2020), 3030, 4*time.Second, big.Zero(), 55))
				subscriber(events.Finished(time.Now().Add(50*time.Millisecond), id, fetchPhaseStartTime, types.RetrievalCandidate{RootCid: testCid1}))

				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-receivedChan:
				}

				require.Equal(t, int64(1), req.Length())
				eventList := verifyListNode(t, req, "events", 1)
				event := verifyListElement(t, eventList, 0)
				// require.Equal(t, int64(8), event.Length())
				verifyStringNode(t, event, "instanceId", "test-instance")
				verifyStringNode(t, event, "retrievalId", id.String())
				verifyStringNode(t, event, "storageProviderId", spid.String())
				// verifyIntNode(t, event, "timeToFirstByte", 0)
				// verifyIntNode(t, event, "bandwidth", 0)
				verifyBoolNode(t, event, "success", true)
				// verifyStringNode(t, event, "startTime", fetchPhaseStartTime.Format(time.RFC3339Nano))
				// verifyStringNode(t, event, "endTime", fetchPhaseStartTime.Format(time.RFC3339Nano))
			},
		},
		{
			name: "Retrieval Failure, Never Reached First Byte",
			exec: func(t *testing.T, ctx context.Context, subscriber types.RetrievalEventSubscriber, id types.RetrievalID, etime, ptime time.Time, spid peer.ID) {
				fetchPhaseStartTime := ptime.Add(-10 * time.Second)

				subscriber(events.Started(time.Now(), id, fetchPhaseStartTime, types.FetchPhase, types.RetrievalCandidate{RootCid: testCid1}))
				subscriber(events.Finished(time.Now(), id, fetchPhaseStartTime, types.RetrievalCandidate{RootCid: testCid1}))

				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-receivedChan:
				}

				require.Equal(t, int64(1), req.Length())
				eventList := verifyListNode(t, req, "events", 1)
				event := verifyListElement(t, eventList, 0)
				require.Equal(t, int64(5), event.Length())
				verifyStringNode(t, event, "instanceId", "test-instance")
				verifyStringNode(t, event, "retrievalId", id.String())
				verifyBoolNode(t, event, "success", false)
				// verifyStringNode(t, event, "startTime", fetchPhaseStartTime.Format(time.RFC3339Nano))
				// verifyStringNode(t, event, "endTime", fetchPhaseStartTime.Format(time.RFC3339Nano))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			subscriber := aggregateeventrecorder.NewAggregateEventRecorder(
				ctx,
				"test-instance",
				fmt.Sprintf("%s/test-path/here", ts.URL),
				authHeaderValue,
			).RetrievalEventSubscriber()
			id, err := types.NewRetrievalID()
			require.NoError(t, err)
			etime := time.Now()
			ptime := time.Now().Add(time.Hour * -1)
			spid := peer.ID("A")
			test.exec(t, ctx, subscriber, id, etime, ptime, spid)
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

func mustCid(cstr string) cid.Cid {
	c, err := cid.Decode(cstr)
	if err != nil {
		panic(err)
	}
	return c
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
		"test-instance",
		fmt.Sprintf("%s/test-path/here", ts.URL),
		authHeaderValue,
	).RetrievalEventSubscriber()
	id, _ := types.NewRetrievalID()
	fetchStartTime := time.Now()
	ptime := time.Now().Add(time.Hour * -1)
	spid := peer.ID("A")

	var success bool
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		subscriber(events.Started(time.Now(), id, fetchStartTime, types.FetchPhase, types.NewRetrievalCandidate(spid, testCid1)))
		subscriber(events.FirstByte(time.Now(), id, ptime, types.NewRetrievalCandidate(spid, testCid1)))
		subscriber(events.Success(time.Now(), id, ptime, types.NewRetrievalCandidate(spid, testCid1), uint64(2020), 3030, 4*time.Second, big.Zero(), 55))
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
