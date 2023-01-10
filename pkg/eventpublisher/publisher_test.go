package eventpublisher_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lassie/pkg/eventpublisher"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

var testCid1 cid.Cid = mustCid("bafybeihrqe2hmfauph5yfbd6ucv7njqpiy4tvbewlvhzjl4bhnyiu6h7pm")

type sub struct {
	ping chan eventpublisher.RetrievalEvent
}

func (us *sub) OnRetrievalEvent(evt eventpublisher.RetrievalEvent) {
	if us.ping != nil {
		us.ping <- evt
	}
}

func TestSubscribeAndUnsubscribe(t *testing.T) {
	pub := eventpublisher.NewEventPublisher(context.Background())
	sub := &sub{}
	require.Equal(t, 0, pub.SubscriberCount(), "has no subscribers")

	unsub := pub.Subscribe(sub)
	unsub2 := pub.Subscribe(sub)

	require.Equal(t, 2, pub.SubscriberCount(), "registered both subscribers")
	unsub()
	require.Equal(t, 1, pub.SubscriberCount(), "unregistered first subscriber")
	unsub2()
	require.Equal(t, 0, pub.SubscriberCount(), "unregistered second subscriber")
}

func TestEventing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	pub := eventpublisher.NewEventPublisher(ctx)
	sub := &sub{ping: make(chan eventpublisher.RetrievalEvent)}

	pub.Subscribe(sub)
	pid := peer.NewPeerRecord().PeerID
	pub.Publish(eventpublisher.Connect(time.Now(), eventpublisher.QueryPhase, testCid1, pid))
	pub.Publish(eventpublisher.Success(time.Now(), testCid1, "", 101, 202, time.Millisecond*303, abi.NewTokenAmount(404)))

	evt := <-sub.ping
	require.Equal(t, eventpublisher.QueryPhase, evt.Phase())
	require.Equal(t, testCid1, evt.PayloadCid())
	require.Equal(t, pid, evt.StorageProviderId())
	require.Equal(t, eventpublisher.ConnectedCode, evt.Code())
	_, ok := evt.(eventpublisher.RetrievalEventConnect)
	require.True(t, ok)

	evt = <-sub.ping
	require.Equal(t, eventpublisher.RetrievalPhase, evt.Phase())
	require.Equal(t, testCid1, evt.PayloadCid())
	require.Equal(t, peer.ID(""), evt.StorageProviderId())
	require.Equal(t, eventpublisher.SuccessCode, evt.Code())
	res, ok := evt.(eventpublisher.RetrievalEventSuccess)
	require.True(t, ok)
	require.Equal(t, uint64(101), res.ReceivedSize())
	require.Equal(t, uint64(202), res.ReceivedCids())
	require.Equal(t, time.Millisecond*303, res.Duration())
	require.Equal(t, abi.NewTokenAmount(404), res.TotalPayment())
}

func mustCid(cstr string) cid.Cid {
	c, err := cid.Decode(cstr)
	if err != nil {
		panic(err)
	}
	return c
}
