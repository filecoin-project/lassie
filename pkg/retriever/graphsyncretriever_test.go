package retriever

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestRetrievalRacing(t *testing.T) {
	retrievalID := types.RetrievalID(uuid.New())
	startTime := time.Now().Add(time.Hour)
	initialPause := 10 * time.Millisecond

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
				"foo": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("foo"), Size: 1}, Delay: time.Millisecond * 20},
				"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: time.Millisecond * 500},
				"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}, Delay: time.Millisecond * 500},
			},
			expectedRetrieval: "foo",
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{"foo", "bar", "baz"},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
					},
				},
				{
					AfterStart: time.Millisecond * 20,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*20), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
					},
				},
				{
					AfterStart:         time.Millisecond*20 + initialPause,
					ReceivedRetrievals: []peer.ID{"foo"},
				},
				{
					AfterStart: time.Millisecond*40 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Accepted(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.FirstByte(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Success(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}, 1, 0, 0, big.Zero(), 0),
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
				"foo": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("foo"), Size: 1}, Delay: time.Millisecond * 500},
				"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: time.Millisecond * 500},
				"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}, Delay: time.Millisecond * 500},
			},
			expectedRetrieval: "foo",
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{"foo", "bar", "baz"},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
					},
				},
				{
					AfterStart: time.Millisecond * 20,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*20), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
					},
				},
				{
					AfterStart:         time.Millisecond*20 + initialPause,
					ReceivedRetrievals: []peer.ID{"foo"},
				},
				{
					AfterStart: time.Millisecond*40 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.Connected(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
					},
				},
				{
					AfterStart: time.Millisecond*520 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*520+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Accepted(startTime.Add(time.Millisecond*520+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.FirstByte(startTime.Add(time.Millisecond*520+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Success(startTime.Add(time.Millisecond*520+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}, 1, 0, 0, big.Zero(), 0),
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
				"foo": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("foo"), Size: 1}, Delay: time.Millisecond},
				"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: time.Millisecond},
				"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}, Delay: time.Millisecond},
			},
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{"foo", "bar", "baz"},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
					},
				},
				{
					AfterStart: time.Millisecond * 20,
					ExpectedEvents: []types.RetrievalEvent{
						events.Failed(startTime.Add(time.Millisecond*20), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}, "unable to connect to provider: Nope"),
						events.Failed(startTime.Add(time.Millisecond*20), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}, "unable to connect to provider: Nope"),
						events.Failed(startTime.Add(time.Millisecond*20), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}, "unable to connect to provider: Nope"),
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
				"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: time.Millisecond * 20},
				"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}, Delay: time.Millisecond * 20},
			},
			expectedRetrieval: "bar",
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{"foo", "bar", "baz"},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
					},
				},
				{
					AfterStart: time.Millisecond * 20,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*20), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
					},
				},
				{
					AfterStart:         time.Millisecond*20 + initialPause,
					ReceivedRetrievals: []peer.ID{"foo"},
				},
				{
					AfterStart: time.Millisecond*40 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Failed(startTime.Add(time.Millisecond*40+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}, "retrieval failed: Nope"),
					},
				},
				{
					AfterStart:         time.Millisecond * 60,
					ReceivedRetrievals: []peer.ID{"bar"},
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*60), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
					},
				},
				{
					AfterStart: time.Millisecond * 80,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*80), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.Accepted(startTime.Add(time.Millisecond*80), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.FirstByte(startTime.Add(time.Millisecond*80), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.Success(startTime.Add(time.Millisecond*80), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}, 2, 0, 0, big.Zero(), 0),
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
					ReceivedConnections: []peer.ID{"foo", "bar", "baz"},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
					},
				},
				{
					AfterStart: time.Millisecond * 20,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*20), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
					},
				},
				{
					AfterStart: time.Millisecond * 25,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*25), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
					},
				},
				{
					AfterStart:         time.Millisecond*20 + initialPause,
					ReceivedRetrievals: []peer.ID{"foo"},
				},
				{
					AfterStart: time.Millisecond * 60,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*60), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
					},
				},
				{
					AfterStart:         time.Millisecond*120 + initialPause,
					ReceivedRetrievals: []peer.ID{"bar"},
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*120+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Failed(startTime.Add(time.Millisecond*120+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}, "retrieval failed: Nope"),
					},
				},
				{
					AfterStart:         time.Millisecond*220 + initialPause,
					ReceivedRetrievals: []peer.ID{"baz"},
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*220+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.Failed(startTime.Add(time.Millisecond*220+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}, "retrieval failed: Nope"),
					},
				},
				{
					AfterStart: time.Millisecond*320 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*320+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
						events.Failed(startTime.Add(time.Millisecond*320+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}, "retrieval failed: Nope"),
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
				"bar":  {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 3}, Delay: time.Millisecond * 20},
				"baz":  {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 2}, Delay: time.Millisecond * 20},
				"bang": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bang"), Size: 4}, Delay: time.Millisecond * 20},
			},
			expectedRetrieval: "baz",
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{"foo", "bar", "baz", "bang"},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}}),
					},
				},
				{
					AfterStart: time.Millisecond * 1,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*1), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
					},
				},
				{
					AfterStart:         time.Millisecond*1 + initialPause,
					ReceivedRetrievals: []peer.ID{"foo"},
				},
				{
					AfterStart: time.Millisecond * 100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
						events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}}),
					},
				},
				{
					AfterStart:         time.Millisecond*201 + initialPause,
					ReceivedRetrievals: []peer.ID{"baz"},
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*201+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Failed(startTime.Add(time.Millisecond*201+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}, "retrieval failed: Nope"),
					},
				},
				{
					AfterStart: time.Millisecond*221 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*221+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
						events.Accepted(startTime.Add(time.Millisecond*221+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
						events.FirstByte(startTime.Add(time.Millisecond*221+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
						events.Success(startTime.Add(time.Millisecond*221+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}, 2, 0, 0, big.Zero(), 0),
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
				"bar":  {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 3}, Delay: time.Millisecond * 20},
				"baz":  {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 2}, Delay: time.Millisecond * 20},
				"bang": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bang"), Size: 4}, Delay: time.Millisecond * 20},
			},
			expectedRetrieval: "baz",
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{"bar", "baz", "bang"},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}}),
					},
				},
				{
					AfterStart: time.Millisecond * 100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
						events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}}),
					},
				},
				{
					AfterStart:         time.Millisecond*100 + initialPause,
					ReceivedRetrievals: []peer.ID{"baz"},
				},
				{
					AfterStart: time.Millisecond*120 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*120+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
						events.Accepted(startTime.Add(time.Millisecond*120+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
						events.FirstByte(startTime.Add(time.Millisecond*120+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
						events.Success(startTime.Add(time.Millisecond*120+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}, 2, 0, 0, big.Zero(), 0),
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
				"bar":  {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 3}, Delay: time.Millisecond * 20},
				"baz":  {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 2}, Delay: time.Millisecond * 20},
				"bang": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bang"), Size: 4}, Delay: time.Millisecond * 20},
			},
			expectedRetrieval: "bang",
			expectSequence: []testutil.ExpectedActionsAtTime{
				{
					AfterStart:          0,
					ReceivedConnections: []peer.ID{"foo", "bar", "baz", "bang"},
					ExpectedEvents: []types.RetrievalEvent{
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
						events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}}),
					},
				},
				{
					AfterStart: time.Millisecond * 1,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*1), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
					},
				},
				{
					AfterStart:         time.Millisecond*1 + initialPause,
					ReceivedRetrievals: []peer.ID{"foo"},
				},
				{
					AfterStart: time.Millisecond * 100,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}}),
					},
				},
				{
					AfterStart: time.Millisecond * 200,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*200), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
					},
				},
				{
					AfterStart: time.Millisecond * 220,
					ExpectedEvents: []types.RetrievalEvent{
						events.Connected(startTime.Add(time.Millisecond*220), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
					},
				},
				{
					AfterStart:         time.Millisecond*401 + initialPause,
					ReceivedRetrievals: []peer.ID{"bang"},
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*401+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
						events.Failed(startTime.Add(time.Millisecond*401+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}, "retrieval failed: Nope"),
					},
				},
				{
					AfterStart: time.Millisecond*421 + initialPause,
					ExpectedEvents: []types.RetrievalEvent{
						events.Proposed(startTime.Add(time.Millisecond*421+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}}),
						events.Accepted(startTime.Add(time.Millisecond*421+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}}),
						events.FirstByte(startTime.Add(time.Millisecond*421+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}}),
						events.Success(startTime.Add(time.Millisecond*421+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}}, 4, 0, 0, big.Zero(), 0),
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
				candidates = append(candidates, types.NewRetrievalCandidate(peer.ID(p), cid.Undef, protocol))
			}
			cfg := NewGraphsyncRetriever(func(peer peer.ID) time.Duration { return time.Second }, mockClient)
			cfg.Clock = clock
			cfg.QueueInitialPause = initialPause

			rv := testutil.RetrievalVerifier{
				ExpectedSequence: tc.expectSequence,
			}
			// perform retrieval and make sure we got a result
			results := rv.RunWithVerification(ctx, t, clock, mockClient, nil, []testutil.RunRetrieval{func(cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
				return cfg.Retrieve(ctx, types.RetrievalRequest{
					Cid:         cid.Undef,
					RetrievalID: retrievalID,
					LinkSystem:  cidlink.DefaultLinkSystem(),
				}, cb).RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, candidates))
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

// run two retrievals simultaneously on a single CidRetrieval
func TestMultipleRetrievals(t *testing.T) {
	retrievalID := types.RetrievalID(uuid.New())
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
			"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: time.Millisecond * 200},
			"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 3}, Delay: time.Millisecond * 200},
			// group b
			"bang": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bang"), Size: 3}, Delay: time.Millisecond * 201},
			"boom": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("boom"), Size: 3}, Delay: time.Millisecond * 201},
			"bing": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bing"), Size: 3}, Delay: time.Millisecond * 201},
		},
		clock,
	)

	cfg := NewGraphsyncRetriever(func(peer peer.ID) time.Duration { return time.Second }, mockClient)
	cfg.Clock = clock
	cfg.QueueInitialPause = initialPause

	expectedSequence := []testutil.ExpectedActionsAtTime{
		{
			AfterStart:          0,
			ReceivedConnections: []peer.ID{"foo", "bar", "baz", "bang", "boom", "bing"},
			ExpectedEvents: []types.RetrievalEvent{
				events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
				events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
				events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
				events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bang")}}),
				events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("boom")}}),
				events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bing")}}),
			},
		},
		{
			AfterStart: time.Millisecond * 1,
			ExpectedEvents: []types.RetrievalEvent{
				events.Failed(startTime.Add(time.Millisecond*1), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("boom")}}, "unable to connect to provider: Nope"),
				events.Connected(startTime.Add(time.Millisecond*1), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
			},
		},
		{
			AfterStart:         time.Millisecond*1 + initialPause,
			ReceivedRetrievals: []peer.ID{"foo"},
		},
		{
			AfterStart: time.Millisecond*2 + initialPause,
			ExpectedEvents: []types.RetrievalEvent{
				events.Proposed(startTime.Add(time.Millisecond*2+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
				events.Failed(startTime.Add(time.Millisecond*2+initialPause), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}, "retrieval failed: Nope"),
			},
		},
		{
			AfterStart: time.Millisecond * 100,
			ExpectedEvents: []types.RetrievalEvent{
				events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
				events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bing")}}),
			},
		},
		{
			AfterStart:         time.Millisecond * 100,
			ReceivedRetrievals: []peer.ID{"bar"},
		},
		{
			AfterStart:         time.Millisecond*100 + initialPause,
			ReceivedRetrievals: []peer.ID{"bing"},
		},
		{
			AfterStart: time.Millisecond * 300,
			ExpectedEvents: []types.RetrievalEvent{
				events.Proposed(startTime.Add(time.Millisecond*300), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
				events.Accepted(startTime.Add(time.Millisecond*300), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
				events.FirstByte(startTime.Add(time.Millisecond*300), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
				events.Success(startTime.Add(time.Millisecond*300), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}, 2, 0, 0, big.Zero(), 0),
			},
		},
		{
			AfterStart: time.Millisecond*301 + initialPause,
			ExpectedEvents: []types.RetrievalEvent{
				events.Proposed(startTime.Add(time.Millisecond*301+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bing")}}),
				events.Accepted(startTime.Add(time.Millisecond*301+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bing")}}),
				events.FirstByte(startTime.Add(time.Millisecond*301+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bing")}}),
				events.Success(startTime.Add(time.Millisecond*301+initialPause), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bing")}}, 3, 0, 0, big.Zero(), 0),
			},
		},
	}
	results := testutil.RetrievalVerifier{
		ExpectedSequence: expectedSequence,
	}.RunWithVerification(ctx, t, clock, mockClient, nil, []testutil.RunRetrieval{
		func(cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
			return cfg.Retrieve(context.Background(), types.RetrievalRequest{
				Cid:         cid1,
				RetrievalID: retrievalID,
				LinkSystem:  cidlink.DefaultLinkSystem(),
			}, cb).RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, []types.RetrievalCandidate{
				types.NewRetrievalCandidate(peer.ID("foo"), cid.Undef, &metadata.GraphsyncFilecoinV1{}),
				types.NewRetrievalCandidate(peer.ID("bar"), cid.Undef, &metadata.GraphsyncFilecoinV1{}),
				types.NewRetrievalCandidate(peer.ID("baz"), cid.Undef, &metadata.GraphsyncFilecoinV1{}),
			}))
		},
		func(cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
			return cfg.Retrieve(context.Background(), types.RetrievalRequest{
				Cid:         cid2,
				RetrievalID: retrievalID,
				LinkSystem:  cidlink.DefaultLinkSystem(),
			}, cb).RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, []types.RetrievalCandidate{
				types.NewRetrievalCandidate(peer.ID("bang"), cid.Undef, &metadata.GraphsyncFilecoinV1{}),
				types.NewRetrievalCandidate(peer.ID("boom"), cid.Undef, &metadata.GraphsyncFilecoinV1{}),
				types.NewRetrievalCandidate(peer.ID("bing"), cid.Undef, &metadata.GraphsyncFilecoinV1{}),
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
	cid1 := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	clock := clock.New()
	mockClient := testutil.NewMockClient(
		map[string]testutil.DelayedConnectReturn{"foo": {Err: nil, Delay: 0}},
		map[string]testutil.DelayedClientReturn{"foo": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: 0}},
		clock,
	)

	cfg := NewGraphsyncRetriever(func(peer peer.ID) time.Duration { return time.Second }, mockClient)

	selector := selectorparse.CommonSelector_MatchPoint

	retrieval := cfg.Retrieve(context.Background(), types.RetrievalRequest{
		Cid:         cid1,
		RetrievalID: retrievalID,
		LinkSystem:  cidlink.DefaultLinkSystem(),
		Selector:    selector,
	}, nil)
	stats, err := retrieval.RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, []types.RetrievalCandidate{types.NewRetrievalCandidate(peer.ID("foo"), cid.Undef, &metadata.GraphsyncFilecoinV1{})}))
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, mockClient.GetRetrievalReturns()["foo"].ResultStats, stats)

	// make sure we performed the retrievals we expected
	rr := mockClient.VerifyReceivedRetrievalFrom(ctx, t, peer.ID("foo"))
	require.NotNil(t, rr)
	require.Same(t, selector, rr.Selector)
}

func TestDuplicateRetreivals(t *testing.T) {
	retrievalID := types.RetrievalID(uuid.New())
	cid1 := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	startTime := time.Now().Add(time.Hour)
	clock := clock.NewMock()
	clock.Set(startTime)
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
			"bar": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("bar"), Size: 2}, Delay: time.Millisecond * 200},
			"baz": {ResultStats: &types.RetrievalStats{StorageProviderId: peer.ID("baz"), Size: 2}, Delay: time.Millisecond * 200},
		},
		clock,
	)

	cfg := NewGraphsyncRetrieverWithClock(func(peer peer.ID) time.Duration { return time.Second }, mockClient, clock)

	expectedSequence := []testutil.ExpectedActionsAtTime{
		{
			AfterStart:          0,
			ReceivedConnections: []peer.ID{"foo", "bar", "baz"},
			ExpectedEvents: []types.RetrievalEvent{
				events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
				events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
				events.Started(startTime, retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
			},
		},
		{
			AfterStart:         time.Millisecond * 50,
			ReceivedRetrievals: []peer.ID{"foo"},
			ExpectedEvents: []types.RetrievalEvent{
				events.Connected(startTime.Add(time.Millisecond*50), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
			},
		},
		{
			AfterStart: time.Millisecond * 75,
			ExpectedEvents: []types.RetrievalEvent{
				events.Connected(startTime.Add(time.Millisecond*75), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("baz")}}),
			},
		},
		{
			AfterStart: time.Millisecond * 100,
			ExpectedEvents: []types.RetrievalEvent{
				events.Connected(startTime.Add(time.Millisecond*100), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("bar")}}),
			},
		},
		{
			AfterStart:         time.Millisecond * 200,
			ReceivedRetrievals: []peer.ID{"bar"},
			ExpectedEvents: []types.RetrievalEvent{
				events.Proposed(startTime.Add(time.Millisecond*200), retrievalID, startTime, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}),
				events.Failed(startTime.Add(time.Millisecond*200), retrievalID, startTime, types.RetrievalPhase, types.RetrievalCandidate{MinerPeer: peer.AddrInfo{ID: peer.ID("foo")}}, "retrieval failed: Nope"),
			},
		},
		{
			AfterStart: time.Millisecond * 400,
			ExpectedEvents: []types.RetrievalEvent{
				events.Proposed(startTime.Add(time.Millisecond*400), retrievalID, startTime, types.NewRetrievalCandidate(peer.ID("bar"), cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: true, FastRetrieval: false})),
				events.Accepted(startTime.Add(time.Millisecond*400), retrievalID, startTime, types.NewRetrievalCandidate(peer.ID("bar"), cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: true, FastRetrieval: false})),
				events.FirstByte(startTime.Add(time.Millisecond*400), retrievalID, startTime, types.NewRetrievalCandidate(peer.ID("bar"), cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: true, FastRetrieval: false})),
				events.Success(startTime.Add(time.Millisecond*400), retrievalID, startTime, types.NewRetrievalCandidate(peer.ID("bar"), cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: true, FastRetrieval: false}), 2, 0, 0, big.Zero(), 0),
			},
		},
	}
	results := testutil.RetrievalVerifier{
		ExpectedSequence: expectedSequence,
	}.RunWithVerification(ctx, t, clock, mockClient, nil, []testutil.RunRetrieval{
		func(cb func(types.RetrievalEvent)) (*types.RetrievalStats, error) {
			return cfg.Retrieve(context.Background(), types.RetrievalRequest{
				Cid:         cid1,
				RetrievalID: retrievalID,
				LinkSystem:  cidlink.DefaultLinkSystem(),
			}, cb).RetrieveFromAsyncCandidates(MakeAsyncCandidates(t, []types.RetrievalCandidate{
				types.NewRetrievalCandidate(peer.ID("foo"), cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: false, FastRetrieval: false}),
				types.NewRetrievalCandidate(peer.ID("baz"), cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: false, FastRetrieval: false}),
				types.NewRetrievalCandidate(peer.ID("bar"), cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: false, FastRetrieval: false}),
				types.NewRetrievalCandidate(peer.ID("bar"), cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: false, FastRetrieval: true}),
				types.NewRetrievalCandidate(peer.ID("bar"), cid.Undef, &metadata.GraphsyncFilecoinV1{PieceCID: cid.Cid{}, VerifiedDeal: true, FastRetrieval: false}),
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

func MakeAsyncCandidates(t *testing.T, candidates []types.RetrievalCandidate) types.InboundAsyncCandidates {
	incoming, outgoing := types.MakeAsyncCandidates(len(candidates))
	for _, candidate := range candidates {
		err := outgoing.SendNext(context.Background(), []types.RetrievalCandidate{candidate})
		require.NoError(t, err)
	}
	close(outgoing)
	return incoming
}
