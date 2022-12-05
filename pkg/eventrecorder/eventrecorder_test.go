package eventrecorder_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lassie/pkg/eventpublisher"
	"github.com/filecoin-project/lassie/pkg/eventrecorder"
	qt "github.com/frankban/quicktest"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/net/context"
)

var testCid1 cid.Cid = mustCid("bafybeihrqe2hmfauph5yfbd6ucv7njqpiy4tvbewlvhzjl4bhnyiu6h7pm")

func TestEventRecorder(t *testing.T) {
	var req datamodel.Node
	var path string
	receivedChan := make(chan bool, 1)
	authHeaderValue := "applesauce"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		req, err = ipld.DecodeStreaming(r.Body, dagjson.Decode)
		qt.Assert(t, err, qt.IsNil)
		path = r.URL.Path
		qt.Assert(t, r.Header.Get("Authorization"), qt.Equals, "Basic applesauce")
		receivedChan <- true
	}))
	defer ts.Close()

	tests := []struct {
		name string
		exec func(t *testing.T, ctx context.Context, er *eventrecorder.EventRecorder, id uuid.UUID, etime, ptime time.Time, spid peer.ID)
	}{
		{
			name: "QuerySuccess",
			exec: func(t *testing.T, ctx context.Context, er *eventrecorder.EventRecorder, id uuid.UUID, etime, ptime time.Time, spid peer.ID) {
				qr := retrievalmarket.QueryResponse{
					Status:                     retrievalmarket.QueryResponseUnavailable,
					Size:                       10101,
					MinPricePerByte:            abi.NewTokenAmount(202020),
					UnsealPrice:                abi.NewTokenAmount(0),
					Message:                    "yo!",
					PieceCIDFound:              1,
					MaxPaymentInterval:         3030,
					MaxPaymentIntervalIncrease: 99,
					PaymentAddress:             address.TestAddress,
				}
				er.QuerySuccess(id, ptime, etime, testCid1, spid, qr)

				select {
				case <-ctx.Done():
				case <-receivedChan:
				}

				qt.Assert(t, req.Length(), qt.Equals, int64(1))
				eventList := verifyListNode(t, req, "events", 1)
				event := verifyListElement(t, eventList, 0)
				qt.Assert(t, event.Length(), qt.Equals, int64(9))
				verifyStringNode(t, event, "retrievalId", id.String())
				verifyStringNode(t, event, "instanceId", "test-instance")
				verifyStringNode(t, event, "cid", testCid1.String())
				verifyStringNode(t, event, "storageProviderId", spid.String())
				verifyStringNode(t, event, "phase", "query")
				verifyStringNode(t, event, "phaseStartTime", ptime.Format(time.RFC3339Nano))
				verifyStringNode(t, event, "eventName", "query-asked")
				verifyStringNode(t, event, "eventTime", etime.Format(time.RFC3339Nano))

				detailsNode, err := event.LookupByString("eventDetails")
				qt.Assert(t, err, qt.IsNil)
				qt.Assert(t, detailsNode.Length(), qt.Equals, int64(9))
				verifyIntNode(t, detailsNode, "Status", 1)
				verifyIntNode(t, detailsNode, "Size", 10101)
				verifyIntNode(t, detailsNode, "MaxPaymentInterval", 3030)
				verifyIntNode(t, detailsNode, "MaxPaymentIntervalIncrease", 99)
				verifyIntNode(t, detailsNode, "PieceCIDFound", 1)
				verifyStringNode(t, detailsNode, "UnsealPrice", "0")
				verifyStringNode(t, detailsNode, "MinPricePerByte", "202020")
				verifyStringNode(t, detailsNode, "PaymentAddress", address.TestAddress.String())
				verifyStringNode(t, detailsNode, "Message", "yo!")
			},
		},
		{
			name: "RetrievalSuccess",
			exec: func(t *testing.T, ctx context.Context, er *eventrecorder.EventRecorder, id uuid.UUID, etime, ptime time.Time, spid peer.ID) {
				er.RetrievalSuccess(id, ptime, etime, testCid1, spid, uint64(2020), 3030, true)

				select {
				case <-ctx.Done():
				case <-receivedChan:
				}

				qt.Assert(t, req.Length(), qt.Equals, int64(1))
				eventList := verifyListNode(t, req, "events", 1)
				event := verifyListElement(t, eventList, 0)
				qt.Assert(t, event.Length(), qt.Equals, int64(9))
				verifyStringNode(t, event, "retrievalId", id.String())
				verifyStringNode(t, event, "instanceId", "test-instance")
				verifyStringNode(t, event, "cid", testCid1.String())
				verifyStringNode(t, event, "storageProviderId", spid.String())
				verifyStringNode(t, event, "phase", "retrieval")
				verifyStringNode(t, event, "phaseStartTime", ptime.Format(time.RFC3339Nano))
				verifyStringNode(t, event, "eventName", "success")
				verifyStringNode(t, event, "eventTime", etime.Format(time.RFC3339Nano))

				detailsNode, err := event.LookupByString("eventDetails")
				qt.Assert(t, err, qt.IsNil)
				qt.Assert(t, detailsNode.Length(), qt.Equals, int64(3))
				verifyIntNode(t, detailsNode, "receivedSize", 2020)
				verifyIntNode(t, detailsNode, "receivedCids", 3030)
				verifyBoolNode(t, detailsNode, "confirmed", true)
			},
		},
		{
			name: "QueryFailure",
			exec: func(t *testing.T, ctx context.Context, er *eventrecorder.EventRecorder, id uuid.UUID, etime, ptime time.Time, spid peer.ID) {
				er.QueryFailure(id, ptime, etime, testCid1, spid, "ha ha no")

				select {
				case <-ctx.Done():
				case <-receivedChan:
				}
				qt.Assert(t, req.Length(), qt.Equals, int64(1))
				eventList := verifyListNode(t, req, "events", 1)
				event := verifyListElement(t, eventList, 0)
				qt.Assert(t, event.Length(), qt.Equals, int64(9))
				verifyStringNode(t, event, "retrievalId", id.String())
				verifyStringNode(t, event, "instanceId", "test-instance")
				verifyStringNode(t, event, "cid", testCid1.String())
				verifyStringNode(t, event, "storageProviderId", spid.String())
				verifyStringNode(t, event, "phase", "query")
				verifyStringNode(t, event, "phaseStartTime", ptime.Format(time.RFC3339Nano))
				verifyStringNode(t, event, "eventName", "failure")
				verifyStringNode(t, event, "eventTime", etime.Format(time.RFC3339Nano))

				detailsNode, err := event.LookupByString("eventDetails")
				qt.Assert(t, err, qt.IsNil)
				qt.Assert(t, detailsNode.Length(), qt.Equals, int64(1))
				verifyStringNode(t, detailsNode, "error", "ha ha no")
			},
		},
		{
			name: "RetrievalFailure",
			exec: func(t *testing.T, ctx context.Context, er *eventrecorder.EventRecorder, id uuid.UUID, etime, ptime time.Time, spid peer.ID) {
				er.RetrievalFailure(id, ptime, etime, testCid1, spid, "ha ha no, silly silly")

				select {
				case <-ctx.Done():
				case <-receivedChan:
				}

				qt.Assert(t, req.Length(), qt.Equals, int64(1))
				eventList := verifyListNode(t, req, "events", 1)
				event := verifyListElement(t, eventList, 0)
				qt.Assert(t, event.Length(), qt.Equals, int64(9))
				verifyStringNode(t, event, "retrievalId", id.String())
				verifyStringNode(t, event, "instanceId", "test-instance")
				verifyStringNode(t, event, "cid", testCid1.String())
				verifyStringNode(t, event, "storageProviderId", spid.String())
				verifyStringNode(t, event, "phase", "retrieval")
				verifyStringNode(t, event, "phaseStartTime", ptime.Format(time.RFC3339Nano))
				verifyStringNode(t, event, "eventName", "failure")
				verifyStringNode(t, event, "eventTime", etime.Format(time.RFC3339Nano))

				detailsNode, err := event.LookupByString("eventDetails")
				qt.Assert(t, err, qt.IsNil)
				qt.Assert(t, detailsNode.Length(), qt.Equals, int64(1))
				verifyStringNode(t, detailsNode, "error", "ha ha no, silly silly")
			},
		},
		{
			name: "QueryProgress",
			exec: func(t *testing.T, ctx context.Context, er *eventrecorder.EventRecorder, id uuid.UUID, etime, ptime time.Time, spid peer.ID) {
				er.QueryProgress(id, ptime, etime, testCid1, spid, eventpublisher.ConnectedCode)

				select {
				case <-ctx.Done():
				case <-receivedChan:
				}

				qt.Assert(t, req.Length(), qt.Equals, int64(1))
				eventList := verifyListNode(t, req, "events", 1)
				event := verifyListElement(t, eventList, 0)
				qt.Assert(t, event.Length(), qt.Equals, int64(8))
				verifyStringNode(t, event, "retrievalId", id.String())
				verifyStringNode(t, event, "instanceId", "test-instance")
				verifyStringNode(t, event, "cid", testCid1.String())
				verifyStringNode(t, event, "storageProviderId", spid.String())
				verifyStringNode(t, event, "phase", "query")
				verifyStringNode(t, event, "phaseStartTime", ptime.Format(time.RFC3339Nano))
				verifyStringNode(t, event, "eventName", "connected")
				verifyStringNode(t, event, "eventTime", etime.Format(time.RFC3339Nano))
			},
		},
		{
			name: "RetrievalProgress",
			exec: func(t *testing.T, ctx context.Context, er *eventrecorder.EventRecorder, id uuid.UUID, etime, ptime time.Time, spid peer.ID) {
				er.RetrievalProgress(id, ptime, etime, testCid1, spid, eventpublisher.FirstByteCode)

				select {
				case <-ctx.Done():
				case <-receivedChan:
				}

				qt.Assert(t, req.Length(), qt.Equals, int64(1))
				eventList := verifyListNode(t, req, "events", 1)
				event := verifyListElement(t, eventList, 0)
				qt.Assert(t, event.Length(), qt.Equals, int64(8))
				verifyStringNode(t, event, "retrievalId", id.String())
				verifyStringNode(t, event, "instanceId", "test-instance")
				verifyStringNode(t, event, "cid", testCid1.String())
				verifyStringNode(t, event, "storageProviderId", spid.String())
				verifyStringNode(t, event, "phase", "retrieval")
				verifyStringNode(t, event, "phaseStartTime", ptime.Format(time.RFC3339Nano))
				verifyStringNode(t, event, "eventName", "first-byte-received")
				verifyStringNode(t, event, "eventTime", etime.Format(time.RFC3339Nano))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			er := eventrecorder.NewEventRecorder(ctx, "test-instance", fmt.Sprintf("%s/test-path/here", ts.URL), authHeaderValue)
			id, err := uuid.NewRandom()
			qt.Assert(t, err, qt.IsNil)
			etime := time.Now()
			ptime := time.Now().Add(time.Hour * -1)
			spid := peer.NewPeerRecord().PeerID
			test.exec(t, ctx, er, id, etime, ptime, spid)
			qt.Assert(t, req, qt.IsNotNil)
			qt.Assert(t, path, qt.Equals, "/test-path/here")
		})
	}
}

func verifyListNode(t *testing.T, node datamodel.Node, key string, expectedLength int64) datamodel.Node {
	subNode, err := node.LookupByString(key)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, subNode.Kind(), qt.Equals, datamodel.Kind_List)
	qt.Assert(t, subNode.Length(), qt.Equals, expectedLength)
	return subNode
}

func verifyListElement(t *testing.T, node datamodel.Node, index int64) datamodel.Node {
	element, err := node.LookupByIndex(index)
	qt.Assert(t, err, qt.IsNil)
	return element
}

func verifyStringNode(t *testing.T, node datamodel.Node, key string, expected string) {
	subNode, err := node.LookupByString(key)
	qt.Assert(t, err, qt.IsNil)
	str, err := subNode.AsString()
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, str, qt.Equals, expected)
}

func verifyIntNode(t *testing.T, node datamodel.Node, key string, expected int64) {
	subNode, err := node.LookupByString(key)
	qt.Assert(t, err, qt.IsNil)
	ii, err := subNode.AsInt()
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, ii, qt.Equals, expected)
}

func verifyBoolNode(t *testing.T, node datamodel.Node, key string, expected bool) {
	subNode, err := node.LookupByString(key)
	qt.Assert(t, err, qt.IsNil)
	bb, err := subNode.AsBool()
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, bb, qt.Equals, expected)
}

func mustCid(cstr string) cid.Cid {
	c, err := cid.Decode(cstr)
	if err != nil {
		panic(err)
	}
	return c
}

func TestEventRecorderSlowPost(t *testing.T) {
	var reqsLk sync.Mutex
	var reqs []datamodel.Node
	awaitResponse := make(chan struct{})
	authHeaderValue := "applesauce"
	var requestWg sync.WaitGroup
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		req, err := ipld.DecodeStreaming(r.Body, dagjson.Decode)
		qt.Assert(t, err, qt.IsNil)
		reqsLk.Lock()
		reqs = append(reqs, req)
		reqsLk.Unlock()
		qt.Assert(t, req.Length(), qt.Equals, int64(1))
		events, err := req.LookupByString("events")
		qt.Assert(t, err, qt.IsNil)
		qt.Assert(t, events.Kind(), qt.Equals, datamodel.Kind_List)
		eventsLen := events.Length()
		qt.Assert(t, r.Header.Get("Authorization"), qt.Equals, "Basic applesauce")
		<-awaitResponse
		for i := 0; i < int(eventsLen); i++ {
			requestWg.Done()
		}
	}))
	defer ts.Close()

	numParallel := 500
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	er := eventrecorder.NewEventRecorder(ctx, "test-instance", fmt.Sprintf("%s/test-path/here", ts.URL), authHeaderValue)
	id, err := uuid.NewRandom()
	qt.Assert(t, err, qt.IsNil)
	etime := time.Now()
	ptime := time.Now().Add(time.Hour * -1)
	spid := peer.NewPeerRecord().PeerID

	var wg sync.WaitGroup
	for i := 0; i < numParallel; i++ {
		wg.Add(1)
		requestWg.Add(1)
		go func() {
			defer wg.Done()
			er.RetrievalProgress(id, ptime, etime, testCid1, spid, eventpublisher.FirstByteCode)
		}()
	}
	if !waitGroupWait(ctx, &wg) {
		close(awaitResponse)
		t.Fatal("did not finish posting events")
	}
	close(awaitResponse)
	if !waitGroupWait(ctx, &requestWg) {
		t.Fatal("did not finish processing events")
	}
	for _, req := range reqs {
		qt.Assert(t, req, qt.IsNotNil)
	}
}

// waitGroupWait calls wg.Wait while respecting context cancellation
func waitGroupWait(ctx context.Context, wg *sync.WaitGroup) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return false
	case <-done:
		return true
	}
}
