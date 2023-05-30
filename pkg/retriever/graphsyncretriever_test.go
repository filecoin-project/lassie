package retriever_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/session"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestRetrievalRacing(t *testing.T) {
	retrievalID := types.RetrievalID(uuid.New())
	startTime := time.Now().Add(time.Hour)
	initialPause := 10 * time.Millisecond
	peerFoo := peer.ID("foo")
	peerBar := peer.ID("bar")
	peerBaz := peer.ID("baz")
	peerBang := peer.ID("bang")

	testCases := []struct {
		name              string
		customMetadata    map[string]metadata.Protocol
		connectReturns    map[string]testutil.DelayedConnectReturn
		retrievalReturns  map[string]testutil.DelayedClientReturn
		expectedRetrieval string
		expectSequence    []testutil.ExpectedActionsAtTime
	}{
		{
			name: "single fast",
			connectReturns: map[string]testutil.DelayedConnectReturn{
				"foo": {Delay: time.Millisecond * 20},
				"bar": {Delay: time.Millisecond * 500},
				"baz": {Delay: time.Millisecond * 500},
			},
			retrievalReturns: map[string]testutil.DelayedClientReturn{
				"foo": {ResultStats: &types.RetrievalStats{StorageProviderId: peerFoo, Size: 1}, Delay: time.Millisecond * 20},
				"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBar, Size: 2}, Delay: time.Millisecond * 500},
				"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBaz, Size: 3}, Delay: time.Millisecond * 500},
			},
			expectedRetrieval: "foo",
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{peerFoo, peerBar, peerBaz},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
					},
				},
				{
					AfterStart: time.Millisecond * 20,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*20), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerFoo, Duration: time.Millisecond * 20},
					},
				},
				{
					AfterStart:         time.Millisecond*20 + initialPause,
					ReceivedRetrievals: []peer.ID{peerFoo},
				},
				{
					AfterStart: time.Millisecond*40 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.Accepted(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.FirstByte(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}, time.Millisecond*20),
						events.Success(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}, 1, 0, 0, big.Zero(), 0, multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: peerFoo, Duration: time.Millisecond * 20},
						{Type: testutil.SessionMetric_Success, Provider: peerFoo, Value: 1.0},
					},
				},
			},
		},
		{
			name: "all connects finished",
			connectReturns: map[string]testutil.DelayedConnectReturn{
				"foo": {Delay: time.Millisecond * 20},
				"bar": {Delay: time.Millisecond * 50},
				"baz": {Delay: time.Millisecond * 50},
			},
			retrievalReturns: map[string]testutil.DelayedClientReturn{
				"foo": {ResultStats: &types.RetrievalStats{StorageProviderId: peerFoo, Size: 1}, Delay: time.Millisecond * 500},
				"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBar, Size: 2}, Delay: time.Millisecond * 500},
				"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBaz, Size: 3}, Delay: time.Millisecond * 500},
			},
			expectedRetrieval: "foo",
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{peerFoo, peerBar, peerBaz},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
					},
				},
				{
					AfterStart: time.Millisecond * 20,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*20), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerFoo, Duration: time.Millisecond * 20},
					},
				},
				{
					AfterStart:         time.Millisecond*20 + initialPause,
					ReceivedRetrievals: []peer.ID{peerFoo},
				},
				{
					AfterStart: time.Millisecond*50 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
						events.Connected(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerBar, Duration: time.Millisecond * 50},
						{Type: testutil.SessionMetric_Connect, Provider: peerBaz, Duration: time.Millisecond * 50},
					},
				},
				{
					AfterStart: time.Millisecond*520 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*520+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.Accepted(startTime.Add(time.Millisecond*520+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.FirstByte(startTime.Add(time.Millisecond*520+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}, time.Millisecond*500),
						events.Success(startTime.Add(time.Millisecond*520+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}, 1, 0, 0, big.Zero(), 0, multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: peerFoo, Duration: time.Millisecond * 500},
						{Type: testutil.SessionMetric_Success, Provider: peerFoo, Value: 1.0},
					},
				},
			},
		},

		{
			name: "all connects failed",
			connectReturns: map[string]testutil.DelayedConnectReturn{
				"foo": {Err: errors.New("Nope"), Delay: time.Millisecond * 20},
				"bar": {Err: errors.New("Nope"), Delay: time.Millisecond * 20},
				"baz": {Err: errors.New("Nope"), Delay: time.Millisecond * 20},
			},
			retrievalReturns: map[string]testutil.DelayedClientReturn{
				"foo": {ResultStats: &types.RetrievalStats{StorageProviderId: peerFoo, Size: 1}, Delay: time.Millisecond},
				"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBar, Size: 2}, Delay: time.Millisecond},
				"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBaz, Size: 3}, Delay: time.Millisecond},
			},
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{peerFoo, peerBar, peerBaz},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
					},
				},
				{
					AfterStart: time.Millisecond * 20,
					ExpectedEvents: []types.RetrievalEvent{
						events.Failed(startTime.Add(time.Millisecond*20), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}, "unable to connect to provider: Nope"),
						events.Failed(startTime.Add(time.Millisecond*20), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}, "unable to connect to provider: Nope"),
						events.Failed(startTime.Add(time.Millisecond*20), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}, "unable to connect to provider: Nope"),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Failure, Provider: peerFoo},
						{Type: testutil.SessionMetric_Failure, Provider: peerBar},
						{Type: testutil.SessionMetric_Failure, Provider: peerBaz},
					},
				},
			},
		},
		{
			name: "first retrieval failed",
			connectReturns: map[string]testutil.DelayedConnectReturn{
				"foo": {Err: nil, Delay: time.Millisecond * 20},
				"bar": {Err: nil, Delay: time.Millisecond * 60},
				"baz": {Err: nil, Delay: time.Millisecond * 500},
			},
			retrievalReturns: map[string]testutil.DelayedClientReturn{
				"foo": {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 20},
				"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBar, Size: 2}, Delay: time.Millisecond * 20},
				"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBaz, Size: 3}, Delay: time.Millisecond * 20},
			},
			expectedRetrieval: "bar",
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{peerFoo, peerBar, peerBaz},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
					},
				},
				{
					AfterStart: time.Millisecond * 20,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*20), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerFoo, Duration: time.Millisecond * 20},
					},
				},
				{
					AfterStart:         time.Millisecond*20 + initialPause,
					ReceivedRetrievals: []peer.ID{peerFoo},
				},
				{
					AfterStart: time.Millisecond*40 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.Failed(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}, "retrieval failed: Nope"),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Failure, Provider: peerFoo},
					},
				},
				{
					AfterStart:         time.Millisecond * 60,
					ReceivedRetrievals: []peer.ID{peerBar},
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*60), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerBar, Duration: time.Millisecond * 60},
					},
				},
				{
					AfterStart: time.Millisecond * 80,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*80), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
						events.Accepted(startTime.Add(time.Millisecond*80), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
						events.FirstByte(startTime.Add(time.Millisecond*80), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}, time.Millisecond*20),
						events.Success(startTime.Add(time.Millisecond*80), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}, 2, 0, 0, big.Zero(), 0, multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: peerBar, Duration: time.Millisecond * 20},
						{Type: testutil.SessionMetric_Success, Provider: peerBar, Value: 2},
					},
				},
			},
		},

		{
			name: "all retrievals failed",
			connectReturns: map[string]testutil.DelayedConnectReturn{
				"foo": {Err: nil, Delay: time.Millisecond * 20},
				"bar": {Err: nil, Delay: time.Millisecond * 25},
				"baz": {Err: nil, Delay: time.Millisecond * 60},
			},
			retrievalReturns: map[string]testutil.DelayedClientReturn{
				"foo": {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 100},
				"bar": {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 100},
				"baz": {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 100},
			},
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{peerFoo, peerBar, peerBaz},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
					},
				},
				{
					AfterStart: time.Millisecond * 20,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*20), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerFoo, Duration: time.Millisecond * 20},
					},
				},
				{
					AfterStart: time.Millisecond * 25,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*25), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerBar, Duration: time.Millisecond * 25},
					},
				},
				{
					AfterStart:         time.Millisecond*20 + initialPause,
					ReceivedRetrievals: []peer.ID{peerFoo},
				},
				{
					AfterStart: time.Millisecond * 60,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*60), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerBaz, Duration: time.Millisecond * 60},
					},
				},
				{
					AfterStart:         time.Millisecond*120 + initialPause,
					ReceivedRetrievals: []peer.ID{peerBar},
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*120+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.Failed(startTime.Add(time.Millisecond*120+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}, "retrieval failed: Nope"),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Failure, Provider: peerFoo},
					},
				},
				{
					AfterStart:         time.Millisecond*220 + initialPause,
					ReceivedRetrievals: []peer.ID{peerBaz},
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*220+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
						events.Failed(startTime.Add(time.Millisecond*220+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}, "retrieval failed: Nope"),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Failure, Provider: peerBar},
					},
				},
				{
					AfterStart: time.Millisecond*320 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*320+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
						events.Failed(startTime.Add(time.Millisecond*320+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}, "retrieval failed: Nope"),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Failure, Provider: peerBaz},
					},
				},
			},
		},

		// quickest ("foo") fails retrieval, the other 3 line up in the queue,
		// it should choose the "best", which is the one that's fast retrieval/verified deal
		{
			name: "racing chooses best",
			customMetadata: map[string]metadata.Protocol{
				"bar":  &metadata.GraphsyncFilecoinV1{FastRetrieval: true, VerifiedDeal: false},
				"bang": &metadata.GraphsyncFilecoinV1{FastRetrieval: false, VerifiedDeal: true},
			},
			connectReturns: map[string]testutil.DelayedConnectReturn{
				"foo":  {Err: nil, Delay: time.Millisecond},
				"bar":  {Err: nil, Delay: time.Millisecond * 100},
				"baz":  {Err: nil, Delay: time.Millisecond * 100},
				"bang": {Err: nil, Delay: time.Millisecond * 100},
			},
			retrievalReturns: map[string]testutil.DelayedClientReturn{
				"foo":  {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 200},
				"bar":  {ResultStats: &types.RetrievalStats{StorageProviderId: peerBar, Size: 3}, Delay: time.Millisecond * 20},
				"baz":  {ResultStats: &types.RetrievalStats{StorageProviderId: peerBaz, Size: 2}, Delay: time.Millisecond * 20},
				"bang": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBang, Size: 4}, Delay: time.Millisecond * 20},
			},
			expectedRetrieval: "baz",
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{peerFoo, peerBar, peerBaz, peerBang},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBang}}),
					},
				},
				{
					AfterStart: time.Millisecond * 1,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*1), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerFoo, Duration: time.Millisecond * 1},
					},
				},
				{
					AfterStart:         time.Millisecond*1 + initialPause,
					ReceivedRetrievals: []peer.ID{peerFoo},
				},
				{
					AfterStart: time.Millisecond * 100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
						events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
						events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBang}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerBar, Duration: time.Millisecond * 100},
						{Type: testutil.SessionMetric_Connect, Provider: peerBaz, Duration: time.Millisecond * 100},
						{Type: testutil.SessionMetric_Connect, Provider: peerBang, Duration: time.Millisecond * 100},
					},
				},
				{
					AfterStart:         time.Millisecond*201 + initialPause,
					ReceivedRetrievals: []peer.ID{peerBaz},
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*201+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.Failed(startTime.Add(time.Millisecond*201+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}, "retrieval failed: Nope"),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Failure, Provider: peerFoo},
					},
				},
				{
					AfterStart: time.Millisecond*221 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*221+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
						events.Accepted(startTime.Add(time.Millisecond*221+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
						events.FirstByte(startTime.Add(time.Millisecond*221+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}, time.Millisecond*20),
						events.Success(startTime.Add(time.Millisecond*221+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}, 2, 0, 0, big.Zero(), 0, multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: peerBaz, Duration: time.Millisecond * 20},
						{Type: testutil.SessionMetric_Success, Provider: peerBaz, Value: 2},
					},
				},
			},
		},

		// same as above, but we don't have a failing "foo" to act as a gate for the
		// others to line up; tests the prioritywaitqueue initial pause
		{
			name: "racing chooses best (same connect)",
			customMetadata: map[string]metadata.Protocol{
				"bar":  &metadata.GraphsyncFilecoinV1{FastRetrieval: true, VerifiedDeal: false},
				"bang": &metadata.GraphsyncFilecoinV1{FastRetrieval: false, VerifiedDeal: true},
			},
			connectReturns: map[string]testutil.DelayedConnectReturn{
				"bar":  {Err: nil, Delay: time.Millisecond * 100},
				"baz":  {Err: nil, Delay: time.Millisecond * 100},
				"bang": {Err: nil, Delay: time.Millisecond * 100},
			},
			retrievalReturns: map[string]testutil.DelayedClientReturn{
				"bar":  {ResultStats: &types.RetrievalStats{StorageProviderId: peerBar, Size: 3}, Delay: time.Millisecond * 20},
				"baz":  {ResultStats: &types.RetrievalStats{StorageProviderId: peerBaz, Size: 2}, Delay: time.Millisecond * 20},
				"bang": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBang, Size: 4}, Delay: time.Millisecond * 20},
			},
			expectedRetrieval: "baz",
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{peerBar, peerBaz, peerBang},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBang}}),
					},
				},
				{
					AfterStart: time.Millisecond * 100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
						events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
						events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBang}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerBar, Duration: time.Millisecond * 100},
						{Type: testutil.SessionMetric_Connect, Provider: peerBaz, Duration: time.Millisecond * 100},
						{Type: testutil.SessionMetric_Connect, Provider: peerBang, Duration: time.Millisecond * 100},
					},
				},
				{
					AfterStart:         time.Millisecond*100 + initialPause,
					ReceivedRetrievals: []peer.ID{peerBaz},
				},
				{
					AfterStart: time.Millisecond*120 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*120+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
						events.Accepted(startTime.Add(time.Millisecond*120+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
						events.FirstByte(startTime.Add(time.Millisecond*120+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}, time.Millisecond*20),
						events.Success(startTime.Add(time.Millisecond*120+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}, 2, 0, 0, big.Zero(), 0, multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: peerBaz, Duration: time.Millisecond * 20},
						{Type: testutil.SessionMetric_Success, Provider: peerBaz, Value: 2},
					},
				},
			},
		},

		// quickest ("foo") fails retrieval, the other 3 line up in the queue,
		// it should choose the "best", which in this case is the fastest to line
		// up / connect (they are all free and the same size)
		{
			name: "racing chooses fastest connect",
			connectReturns: map[string]testutil.DelayedConnectReturn{
				"foo":  {Err: nil, Delay: time.Millisecond * 1},
				"bar":  {Err: nil, Delay: time.Millisecond * 220},
				"baz":  {Err: nil, Delay: time.Millisecond * 200},
				"bang": {Err: nil, Delay: time.Millisecond * 100},
			},
			retrievalReturns: map[string]testutil.DelayedClientReturn{
				"foo":  {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 400},
				"bar":  {ResultStats: &types.RetrievalStats{StorageProviderId: peerBar, Size: 3}, Delay: time.Millisecond * 20},
				"baz":  {ResultStats: &types.RetrievalStats{StorageProviderId: peerBaz, Size: 2}, Delay: time.Millisecond * 20},
				"bang": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBang, Size: 4}, Delay: time.Millisecond * 20},
			},
			expectedRetrieval: "bang",
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{peerFoo, peerBar, peerBaz, peerBang},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBang}}),
					},
				},
				{
					AfterStart: time.Millisecond * 1,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*1), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerFoo, Duration: time.Millisecond * 1},
					},
				},
				{
					AfterStart:         time.Millisecond*1 + initialPause,
					ReceivedRetrievals: []peer.ID{peerFoo},
				},
				{
					AfterStart: time.Millisecond * 100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBang}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerBang, Duration: time.Millisecond * 100},
					},
				},
				{
					AfterStart: time.Millisecond * 200,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*200), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerBaz, Duration: time.Millisecond * 200},
					},
				},
				{
					AfterStart: time.Millisecond * 220,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*220), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Connect, Provider: peerBar, Duration: time.Millisecond * 220},
					},
				},
				{
					AfterStart:         time.Millisecond*401 + initialPause,
					ReceivedRetrievals: []peer.ID{peerBang},
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*401+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
						events.Failed(startTime.Add(time.Millisecond*401+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}, "retrieval failed: Nope"),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_Failure, Provider: peerFoo},
					},
				},
				{
					AfterStart: time.Millisecond*421 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*421+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBang}}),
						events.Accepted(startTime.Add(time.Millisecond*421+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBang}}),
						events.FirstByte(startTime.Add(time.Millisecond*421+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBang}}, time.Millisecond*20),
						events.Success(startTime.Add(time.Millisecond*421+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBang}}, 4, 0, 0, big.Zero(), 0, multicodec.TransportGraphsyncFilecoinv1),
					},
					ExpectedMetrics: []testutil.SessionMetric{
						{Type: testutil.SessionMetric_FirstByte, Provider: peerBang, Duration: time.Millisecond * 20},
						{Type: testutil.SessionMetric_Success, Provider: peerBang, Value: 4},
					},
				},
			},
		},
	}
	ctx := context.Background()
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			clock := clock.NewMock()
			clock.Set(startTime)
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			mockClient := testutil.NewMockClient(tc.connectReturns, tc.retrievalReturns, clock)
			candidates := []types.RetrievalCandidate{}
			for p := range tc.connectReturns {
				var protocol metadata.Protocol
				if custom, ok := tc.customMetadata[p]; ok {
					protocol = custom
				} else {
					protocol = &metadata.GraphsyncFilecoinV1{VerifiedDeal: true, FastRetrieval: true}
				}
				candidates = append(candidates, types.NewRetrievalCandidate(peer.ID(p), nil, cid.Undef, protocol))
			}

			// we're testing the actual Session implementation, but we also want
			// to observe, so MockSession WithActual lets us do that.
			scfg := session.DefaultConfig().
				WithDefaultProviderConfig(session.ProviderConfig{
					RetrievalTimeout: time.Second,
				}).
				WithConnectTimeAlpha(0.0). // only use the last connect time
				WithoutRandomness()
			_session := session.NewSession(scfg, true)
			session := testutil.NewMockSession(ctx)
			session.WithActual(_session)
			// register the retrieval so we don't get any "not found" errors out
			// of the session
			session.RegisterRetrieval(retrievalID, cid.Undef, basicnode.NewString("r"))
			session.AddToRetrieval(retrievalID, []peer.ID{peerFoo, peerBar, peerBaz, peerBang})

			cfg := retriever.NewGraphsyncRetrieverWithConfig(session, mockClient, clock, initialPause)

			rv := testutil.RetrievalVerifier{
				ExpectedSequence: tc.expectSequence,
			}
			// perform retrieval and make sure we got a result
			results := rv.RunWithVerification(ctx, t, clock, mockClient, nil, session, []testutil.RunRetrieval{func(cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
				return cfg.Retrieve(ctx, types.RetrievalRequest{
					Cid:         cid.Undef,
					RetrievalID: retrievalID,
					LinkSystem:  cidlink.DefaultLinkSystem(),
				}, cb).RetrieveFromAsyncCandidates(makeAsyncCandidates(t, candidates))
			}})
			require.Len(t, results, 1)
			stats, err := results[0].Stats, results[0].Err
			if tc.expectedRetrieval != "" {
				require.NotNil(t, stats)
				require.NoError(t, err)
				// make sure we got the final retrieval we wanted
				require.Equal(t, tc.retrievalReturns[tc.expectedRetrieval].ResultStats, stats)
			} else {
				require.Nil(t, stats)
				require.Error(t, err)
			}

		})
	}
}

// run two retrievals simultaneously
func TestMultipleRetrievals(t *testing.T) {
	retrievalID1 := types.RetrievalID(uuid.New())
	retrievalID2 := types.RetrievalID(uuid.New())
	peerFoo := peer.ID("foo")
	peerBar := peer.ID("bar")
	peerBaz := peer.ID("baz")
	peerBang := peer.ID("bang")
	peerBoom := peer.ID("boom")
	peerBing := peer.ID("bing")
	cid1 := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	cid2 := cid.MustParse("bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk")
	startTime := time.Now().Add(time.Hour)
	clock := clock.NewMock()
	initialPause := 10 * time.Millisecond
	clock.Set(startTime)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	mockClient := testutil.NewMockClient(
		map[string]testutil.DelayedConnectReturn{
			// group a
			"foo": {Err: nil, Delay: time.Millisecond},
			"bar": {Err: nil, Delay: time.Millisecond * 100},
			"baz": {Err: nil, Delay: time.Millisecond * 500}, // should not finish this
			// group b
			"bang": {Err: nil, Delay: time.Millisecond * 500}, // should not finish this
			"boom": {Err: errors.New("Nope"), Delay: time.Millisecond},
			"bing": {Err: nil, Delay: time.Millisecond * 100},
		},
		map[string]testutil.DelayedClientReturn{
			// group a
			"foo": {ResultErr: errors.New("Nope"), Delay: time.Millisecond},
			"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBar, Size: 2}, Delay: time.Millisecond * 200},
			"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBaz, Size: 3}, Delay: time.Millisecond * 200},
			// group b
			"bang": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBang, Size: 3}, Delay: time.Millisecond * 201},
			"boom": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBoom, Size: 3}, Delay: time.Millisecond * 201},
			"bing": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBing, Size: 3}, Delay: time.Millisecond * 201},
		},
		clock,
	)

	expectedSequence := []testutil.ExpectedActionsAtTime{
		{
			AfterStart:          0,
			ReceivedConnections: []peer.ID{peerFoo, peerBar, peerBaz, peerBang, peerBoom, peerBing},
			ExpectedEvents: []types.RetrievalEvent{
				events.Started(startTime, retrievalID1, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
				events.Started(startTime, retrievalID1, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
				events.Started(startTime, retrievalID1, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
				events.Started(startTime, retrievalID2, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBang}}),
				events.Started(startTime, retrievalID2, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBoom}}),
				events.Started(startTime, retrievalID2, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBing}}),
			},
		},
		{
			AfterStart: time.Millisecond * 1,
			ExpectedEvents: []types.RetrievalEvent{
				events.Failed(startTime.Add(time.Millisecond*1), retrievalID2, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBoom}}, "unable to connect to provider: Nope"),
				events.Connected(startTime.Add(time.Millisecond*1), retrievalID1, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
			},
			ExpectedMetrics: []testutil.SessionMetric{
				{Type: testutil.SessionMetric_Failure, Provider: peerBoom},
				{Type: testutil.SessionMetric_Connect, Provider: peerFoo, Duration: time.Millisecond},
			},
		},
		{
			AfterStart:         time.Millisecond*1 + initialPause,
			ReceivedRetrievals: []peer.ID{peerFoo},
		},
		{
			AfterStart: time.Millisecond*2 + initialPause,
			ExpectedEvents: []types.RetrievalEvent{
				events.Proposed(startTime.Add(time.Millisecond*2+initialPause), retrievalID1, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
				events.Failed(startTime.Add(time.Millisecond*2+initialPause), retrievalID1, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}, "retrieval failed: Nope"),
			},
			ExpectedMetrics: []testutil.SessionMetric{
				{Type: testutil.SessionMetric_Failure, Provider: peerFoo},
			},
		},
		{
			AfterStart: time.Millisecond * 100,
			ExpectedEvents: []types.RetrievalEvent{
				events.Connected(startTime.Add(time.Millisecond*100), retrievalID1, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
				events.Connected(startTime.Add(time.Millisecond*100), retrievalID2, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBing}}),
			},
			ExpectedMetrics: []testutil.SessionMetric{
				{Type: testutil.SessionMetric_Connect, Provider: peerBar, Duration: time.Millisecond * 100},
				{Type: testutil.SessionMetric_Connect, Provider: peerBing, Duration: time.Millisecond * 100},
			},
		},
		{
			AfterStart:         time.Millisecond * 100,
			ReceivedRetrievals: []peer.ID{peerBar},
		},
		{
			AfterStart:         time.Millisecond*100 + initialPause,
			ReceivedRetrievals: []peer.ID{peerBing},
		},
		{
			AfterStart: time.Millisecond * 300,
			ExpectedEvents: []types.RetrievalEvent{
				events.Proposed(startTime.Add(time.Millisecond*300), retrievalID1, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
				events.Accepted(startTime.Add(time.Millisecond*300), retrievalID1, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
				events.FirstByte(startTime.Add(time.Millisecond*300), retrievalID1, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}, time.Millisecond*200),
				events.Success(startTime.Add(time.Millisecond*300), retrievalID1, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}, 2, 0, 0, big.Zero(), 0, multicodec.TransportGraphsyncFilecoinv1),
			},
			ExpectedMetrics: []testutil.SessionMetric{
				{Type: testutil.SessionMetric_FirstByte, Provider: peerBar, Duration: time.Millisecond * 200},
				{Type: testutil.SessionMetric_Success, Provider: peerBar, Value: 2},
			},
		},
		{
			AfterStart: time.Millisecond*301 + initialPause,
			ExpectedEvents: []types.RetrievalEvent{
				events.Proposed(startTime.Add(time.Millisecond*301+initialPause), retrievalID2, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBing}}),
				events.Accepted(startTime.Add(time.Millisecond*301+initialPause), retrievalID2, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBing}}),
				events.FirstByte(startTime.Add(time.Millisecond*301+initialPause), retrievalID2, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBing}}, time.Millisecond*201),
				events.Success(startTime.Add(time.Millisecond*301+initialPause), retrievalID2, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBing}}, 3, 0, 0, big.Zero(), 0, multicodec.TransportGraphsyncFilecoinv1),
			},
			ExpectedMetrics: []testutil.SessionMetric{
				{Type: testutil.SessionMetric_FirstByte, Provider: peerBing, Duration: time.Millisecond * 201},
				{Type: testutil.SessionMetric_Success, Provider: peerBing, Value: 3},
			},
		},
	}

	// we're testing the actual Session implementation, but we also want
	// to observe, so MockSession WithActual lets us do that.
	scfg := session.DefaultConfig().
		WithDefaultProviderConfig(session.ProviderConfig{
			RetrievalTimeout: time.Second,
		}).
		WithConnectTimeAlpha(0.0). // only use the last connect time
		WithoutRandomness()
	_session := session.NewSession(scfg, true)
	session := testutil.NewMockSession(ctx)
	session.WithActual(_session)
	// register the retrieval so we don't get any "not found" errors out
	// of the session
	session.RegisterRetrieval(retrievalID1, cid.MustParse("bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk"), basicnode.NewString("r"))
	session.AddToRetrieval(retrievalID1, []peer.ID{peerFoo, peerBar, peerBaz})
	session.RegisterRetrieval(retrievalID2, cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"), basicnode.NewString("r"))
	session.AddToRetrieval(retrievalID2, []peer.ID{peerBang, peerBoom, peerBing})

	cfg := retriever.NewGraphsyncRetrieverWithConfig(session, mockClient, clock, initialPause)

	results := testutil.RetrievalVerifier{
		ExpectedSequence: expectedSequence,
	}.RunWithVerification(ctx, t, clock, mockClient, nil, session, []testutil.RunRetrieval{
		func(cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
			return cfg.Retrieve(context.Background(), types.RetrievalRequest{
				Cid:         cid1,
				RetrievalID: retrievalID1,
				LinkSystem:  cidlink.DefaultLinkSystem(),
			}, cb).RetrieveFromAsyncCandidates(makeAsyncCandidates(t, []types.RetrievalCandidate{
				types.NewRetrievalCandidate(peerFoo, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{}),
				types.NewRetrievalCandidate(peerBar, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{}),
				types.NewRetrievalCandidate(peerBaz, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{}),
			}))
		},
		func(cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
			return cfg.Retrieve(context.Background(), types.RetrievalRequest{
				Cid:         cid2,
				RetrievalID: retrievalID2,
				LinkSystem:  cidlink.DefaultLinkSystem(),
			}, cb).RetrieveFromAsyncCandidates(makeAsyncCandidates(t, []types.RetrievalCandidate{
				types.NewRetrievalCandidate(peerBang, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{}),
				types.NewRetrievalCandidate(peerBoom, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{}),
				types.NewRetrievalCandidate(peerBing, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{}),
			}))
		}})
	require.Len(t, results, 2)
	stats, err := results[0].Stats, results[0].Err
	require.NoError(t, err)
	require.NotNil(t, stats)
	// make sure we got the final retrieval we wanted
	require.Equal(t, mockClient.GetRetrievalReturns()["bar"].ResultStats, stats)

	stats, err = results[1].Stats, results[1].Err
	require.NoError(t, err)
	require.NotNil(t, stats)
	// make sure we got the final retrieval we wanted
	require.Equal(t, mockClient.GetRetrievalReturns()["bing"].ResultStats, stats)
}

func TestRetrievalSelector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	retrievalID := types.RetrievalID(uuid.New())
	peerFoo := peer.ID("foo")
	peerBar := peer.ID("bar")
	cid1 := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	mockClient := testutil.NewMockClient(
		map[string]testutil.DelayedConnectReturn{"foo": {Err: nil, Delay: 0}},
		map[string]testutil.DelayedClientReturn{"foo": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBar, Size: 2}, Delay: 0}},
		clock.New(),
	)

	scfg := session.DefaultConfig().
		WithDefaultProviderConfig(session.ProviderConfig{
			RetrievalTimeout: time.Second,
		}).
		WithConnectTimeAlpha(0.0). // only use the last connect time
		WithoutRandomness()
	session := session.NewSession(scfg, true)
	cfg := retriever.NewGraphsyncRetriever(session, mockClient)

	selector := selectorparse.CommonSelector_MatchPoint

	retrieval := cfg.Retrieve(context.Background(), types.RetrievalRequest{
		Cid:         cid1,
		RetrievalID: retrievalID,
		LinkSystem:  cidlink.DefaultLinkSystem(),
		Selector:    selector,
	}, nil)
	stats, err := retrieval.RetrieveFromAsyncCandidates(makeAsyncCandidates(t, []types.RetrievalCandidate{types.NewRetrievalCandidate(peerFoo, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{})}))
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, mockClient.GetRetrievalReturns()["foo"].ResultStats, stats)

	// make sure we performed the retrievals we expected
	rr := mockClient.VerifyReceivedRetrievalFrom(ctx, t, peerFoo)
	require.NotNil(t, rr)
	require.Same(t, selector, rr.Selector)
}

func TestDuplicateRetreivals(t *testing.T) {
	retrievalID := types.RetrievalID(uuid.New())
	peerFoo := peer.ID("foo")
	peerBar := peer.ID("bar")
	peerBaz := peer.ID("baz")
	cid1 := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	startTime := time.Now().Add(time.Hour)
	clock := clock.NewMock()
	clock.Set(startTime)
	initialPause := 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	mockClient := testutil.NewMockClient(
		map[string]testutil.DelayedConnectReturn{
			"foo": {Err: nil, Delay: time.Millisecond * 50},
			"baz": {Err: nil, Delay: time.Millisecond * 75},
			"bar": {Err: nil, Delay: time.Millisecond * 100},
		},
		map[string]testutil.DelayedClientReturn{
			"foo": {ResultErr: errors.New("Nope"), Delay: time.Millisecond * 150},
			"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBar, Size: 2}, Delay: time.Millisecond * 200},
			"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peerBaz, Size: 2}, Delay: time.Millisecond * 200},
		},
		clock,
	)

	expectedSequence := []testutil.ExpectedActionsAtTime{
		{
			AfterStart:          0,
			ReceivedConnections: []peer.ID{peerFoo, peerBar, peerBaz},
			ExpectedEvents: []types.RetrievalEvent{
				events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
				events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
				events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
			},
		},
		{
			AfterStart: time.Millisecond * 50,
			ExpectedEvents: []types.RetrievalEvent{
				events.Connected(startTime.Add(time.Millisecond*50), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
			},
			ExpectedMetrics: []testutil.SessionMetric{
				{Type: testutil.SessionMetric_Connect, Provider: peerFoo, Duration: time.Millisecond * 50},
			},
		},
		{
			AfterStart:         time.Millisecond*50 + initialPause,
			ReceivedRetrievals: []peer.ID{peerFoo},
		},
		{
			AfterStart: time.Millisecond * 75,
			ExpectedEvents: []types.RetrievalEvent{
				events.Connected(startTime.Add(time.Millisecond*75), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBaz}}),
			},
			ExpectedMetrics: []testutil.SessionMetric{
				{Type: testutil.SessionMetric_Connect, Provider: peerBaz, Duration: time.Millisecond * 75},
			},
		},
		{
			AfterStart: time.Millisecond * 100,
			ExpectedEvents: []types.RetrievalEvent{
				events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerBar}}),
			},
			ExpectedMetrics: []testutil.SessionMetric{
				{Type: testutil.SessionMetric_Connect, Provider: peerBar, Duration: time.Millisecond * 100},
			},
		},
		{
			AfterStart:         time.Millisecond*200 + initialPause,
			ReceivedRetrievals: []peer.ID{peerBar},
			ExpectedEvents: []types.RetrievalEvent{
				events.Proposed(startTime.Add(time.Millisecond*200+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}),
				events.Failed(startTime.Add(time.Millisecond*200+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peerFoo}}, "retrieval failed: Nope"),
			},
			ExpectedMetrics: []testutil.SessionMetric{
				{Type: testutil.SessionMetric_Failure, Provider: peerFoo},
			},
		},
		{
			AfterStart: time.Millisecond*400 + initialPause,
			ExpectedEvents: []types.RetrievalEvent{
				events.Proposed(startTime.Add(time.Millisecond*400+initialPause), retrievalID, startTime, types.NewRetrievalCandidate(peerBar, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: true, FastRetrieval: false})),
				events.Accepted(startTime.Add(time.Millisecond*400+initialPause), retrievalID, startTime, types.NewRetrievalCandidate(peerBar, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: true, FastRetrieval: false})),
				events.FirstByte(startTime.Add(time.Millisecond*400+initialPause), retrievalID, startTime, types.NewRetrievalCandidate(peerBar, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: true, FastRetrieval: false}), time.Millisecond*200),
				events.Success(startTime.Add(time.Millisecond*400+initialPause), retrievalID, startTime, types.NewRetrievalCandidate(peerBar, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: true, FastRetrieval: false}), 2, 0, 0, big.Zero(), 0, multicodec.TransportGraphsyncFilecoinv1),
			},
			ExpectedMetrics: []testutil.SessionMetric{
				{Type: testutil.SessionMetric_FirstByte, Provider: peerBar, Duration: time.Millisecond * 200},
				{Type: testutil.SessionMetric_Success, Provider: peerBar, Value: 2},
			},
		},
	}

	// we're testing the actual Session implementation, but we also want
	// to observe, so MockSession WithActual lets us do that.
	scfg := session.DefaultConfig().
		WithDefaultProviderConfig(session.ProviderConfig{
			RetrievalTimeout: time.Second,
		}).
		WithConnectTimeAlpha(0.0). // only use the last connect time
		WithoutRandomness()
	_session := session.NewSession(scfg, true)
	session := testutil.NewMockSession(ctx)
	session.WithActual(_session)
	session.RegisterRetrieval(retrievalID, cid.Undef, basicnode.NewBool(true))
	session.AddToRetrieval(retrievalID, []peer.ID{peerFoo, peerBar, peerBaz})

	cfg := retriever.NewGraphsyncRetrieverWithConfig(session, mockClient, clock, initialPause)

	results := testutil.RetrievalVerifier{
		ExpectedSequence: expectedSequence,
	}.RunWithVerification(ctx, t, clock, mockClient, nil, session, []testutil.RunRetrieval{
		func(cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
			return cfg.Retrieve(context.Background(), types.RetrievalRequest{
				Cid:         cid1,
				RetrievalID: retrievalID,
				LinkSystem:  cidlink.DefaultLinkSystem(),
			}, cb).RetrieveFromAsyncCandidates(makeAsyncCandidates(t, []types.RetrievalCandidate{
				types.NewRetrievalCandidate(peerFoo, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: false, FastRetrieval: false}),
				types.NewRetrievalCandidate(peerBaz, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: false, FastRetrieval: false}),
				types.NewRetrievalCandidate(peerBar, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: false, FastRetrieval: false}),
				types.NewRetrievalCandidate(peerBar, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: false, FastRetrieval: true}),
				types.NewRetrievalCandidate(peerBar, nil, cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: true, FastRetrieval: false}),
			}))
		},
	})
	require.Len(t, results, 1)
	stats, err := results[0].Stats, results[0].Err
	require.NoError(t, err)
	require.NotNil(t, stats)
	// make sure we got the final retrieval we wanted
	require.Equal(t, mockClient.GetRetrievalReturns()["bar"].ResultStats, stats)
}

func makeAsyncCandidates(t *testing.T, candidates []types.RetrievalCandidate) types.InboundAsyncCandidates {
	incoming, outgoing := types.MakeAsyncCandidates(len(candidates))
	for _, candidate := range candidates {
		err := outgoing.SendNext(context.Background(), []types.RetrievalCandidate{candidate})
		require.NoError(t, err)
	}
	close(outgoing)
	return incoming
}
