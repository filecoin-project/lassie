package eventrecorder_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/application-research/autoretrieve/filecoin/eventrecorder"
	"github.com/application-research/filclient/rep"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/abi"
	qt "github.com/frankban/quicktest"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/libp2p/go-libp2p-core/peer"
)

var testCid1 cid.Cid = mustCid("bafybeihrqe2hmfauph5yfbd6ucv7njqpiy4tvbewlvhzjl4bhnyiu6h7pm")

func TestEventRecorder(t *testing.T) {
	var req datamodel.Node
	var path string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		req, err = ipld.DecodeStreaming(r.Body, dagjson.Decode)
		qt.Assert(t, err, qt.IsNil)
		path = r.URL.Path
	}))
	defer ts.Close()

	tests := []struct {
		name string
		exec func(t *testing.T, er *eventrecorder.EventRecorder, id uuid.UUID, etime, ptime time.Time, spid peer.ID)
	}{
		{
			name: "QuerySuccess",
			exec: func(t *testing.T, er *eventrecorder.EventRecorder, id uuid.UUID, etime, ptime time.Time, spid peer.ID) {
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

				qt.Assert(t, req.Length(), qt.Equals, int64(9))
				verifyStringNode(t, req, "retrievalId", id.String())
				verifyStringNode(t, req, "instanceId", "test-instance")
				verifyStringNode(t, req, "cid", testCid1.String())
				verifyStringNode(t, req, "storageProviderId", spid.String())
				verifyStringNode(t, req, "phase", "query")
				verifyStringNode(t, req, "phaseStartTime", ptime.Format(time.RFC3339Nano))
				verifyStringNode(t, req, "event", "query-ask")
				verifyStringNode(t, req, "eventTime", etime.Format(time.RFC3339Nano))

				detailsNode, err := req.LookupByString("eventDetails")
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
			exec: func(t *testing.T, er *eventrecorder.EventRecorder, id uuid.UUID, etime, ptime time.Time, spid peer.ID) {
				er.RetrievalSuccess(id, ptime, etime, testCid1, spid, uint64(2020))

				qt.Assert(t, req.Length(), qt.Equals, int64(9))
				verifyStringNode(t, req, "retrievalId", id.String())
				verifyStringNode(t, req, "instanceId", "test-instance")
				verifyStringNode(t, req, "cid", testCid1.String())
				verifyStringNode(t, req, "storageProviderId", spid.String())
				verifyStringNode(t, req, "phase", "retrieval")
				verifyStringNode(t, req, "phaseStartTime", ptime.Format(time.RFC3339Nano))
				verifyStringNode(t, req, "event", "success")
				verifyStringNode(t, req, "eventTime", etime.Format(time.RFC3339Nano))

				detailsNode, err := req.LookupByString("eventDetails")
				qt.Assert(t, err, qt.IsNil)
				qt.Assert(t, detailsNode.Length(), qt.Equals, int64(1))
				verifyIntNode(t, detailsNode, "receivedSize", 2020)
			},
		},
		{
			name: "QueryFailure",
			exec: func(t *testing.T, er *eventrecorder.EventRecorder, id uuid.UUID, etime, ptime time.Time, spid peer.ID) {
				er.QueryFailure(id, ptime, etime, testCid1, spid, "ha ha no")

				qt.Assert(t, req.Length(), qt.Equals, int64(9))
				verifyStringNode(t, req, "retrievalId", id.String())
				verifyStringNode(t, req, "instanceId", "test-instance")
				verifyStringNode(t, req, "cid", testCid1.String())
				verifyStringNode(t, req, "storageProviderId", spid.String())
				verifyStringNode(t, req, "phase", "query")
				verifyStringNode(t, req, "phaseStartTime", ptime.Format(time.RFC3339Nano))
				verifyStringNode(t, req, "event", "failure")
				verifyStringNode(t, req, "eventTime", etime.Format(time.RFC3339Nano))

				detailsNode, err := req.LookupByString("eventDetails")
				qt.Assert(t, err, qt.IsNil)
				qt.Assert(t, detailsNode.Length(), qt.Equals, int64(1))
				verifyStringNode(t, detailsNode, "error", "ha ha no")
			},
		},
		{
			name: "RetrievalFailure",
			exec: func(t *testing.T, er *eventrecorder.EventRecorder, id uuid.UUID, etime, ptime time.Time, spid peer.ID) {
				er.RetrievalFailure(id, ptime, etime, testCid1, spid, "ha ha no, silly silly")

				qt.Assert(t, req.Length(), qt.Equals, int64(9))
				verifyStringNode(t, req, "retrievalId", id.String())
				verifyStringNode(t, req, "instanceId", "test-instance")
				verifyStringNode(t, req, "cid", testCid1.String())
				verifyStringNode(t, req, "storageProviderId", spid.String())
				verifyStringNode(t, req, "phase", "retrieval")
				verifyStringNode(t, req, "phaseStartTime", ptime.Format(time.RFC3339Nano))
				verifyStringNode(t, req, "event", "failure")
				verifyStringNode(t, req, "eventTime", etime.Format(time.RFC3339Nano))

				detailsNode, err := req.LookupByString("eventDetails")
				qt.Assert(t, err, qt.IsNil)
				qt.Assert(t, detailsNode.Length(), qt.Equals, int64(1))
				verifyStringNode(t, detailsNode, "error", "ha ha no, silly silly")
			},
		},
		{
			name: "QueryProgress",
			exec: func(t *testing.T, er *eventrecorder.EventRecorder, id uuid.UUID, etime, ptime time.Time, spid peer.ID) {
				er.QueryProgress(id, ptime, etime, testCid1, spid, rep.RetrievalEventConnect)

				qt.Assert(t, req.Length(), qt.Equals, int64(8))
				verifyStringNode(t, req, "retrievalId", id.String())
				verifyStringNode(t, req, "instanceId", "test-instance")
				verifyStringNode(t, req, "cid", testCid1.String())
				verifyStringNode(t, req, "storageProviderId", spid.String())
				verifyStringNode(t, req, "phase", "query")
				verifyStringNode(t, req, "phaseStartTime", ptime.Format(time.RFC3339Nano))
				verifyStringNode(t, req, "event", "connect")
				verifyStringNode(t, req, "eventTime", etime.Format(time.RFC3339Nano))
			},
		},
		{
			name: "RetrievalProgress",
			exec: func(t *testing.T, er *eventrecorder.EventRecorder, id uuid.UUID, etime, ptime time.Time, spid peer.ID) {
				er.RetrievalProgress(id, ptime, etime, testCid1, spid, rep.RetrievalEventFirstByte)

				qt.Assert(t, req.Length(), qt.Equals, int64(8))
				verifyStringNode(t, req, "retrievalId", id.String())
				verifyStringNode(t, req, "instanceId", "test-instance")
				verifyStringNode(t, req, "cid", testCid1.String())
				verifyStringNode(t, req, "storageProviderId", spid.String())
				verifyStringNode(t, req, "phase", "retrieval")
				verifyStringNode(t, req, "phaseStartTime", ptime.Format(time.RFC3339Nano))
				verifyStringNode(t, req, "event", "first-byte-received")
				verifyStringNode(t, req, "eventTime", etime.Format(time.RFC3339Nano))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			er := eventrecorder.NewEventRecorder("test-instance", fmt.Sprintf("%s/test-path/here", ts.URL))
			id, err := uuid.NewRandom()
			qt.Assert(t, err, qt.IsNil)
			etime := time.Now()
			ptime := time.Now().Add(time.Hour * -1)
			spid := peer.NewPeerRecord().PeerID
			test.exec(t, er, id, etime, ptime, spid)
			qt.Assert(t, req, qt.IsNotNil)
			qt.Assert(t, path, qt.Equals, "/test-path/here")
		})
	}
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

func mustCid(cstr string) cid.Cid {
	c, err := cid.Decode(cstr)
	if err != nil {
		panic(err)
	}
	return c
}
