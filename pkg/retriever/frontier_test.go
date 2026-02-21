package retriever

import (
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestFrontier(t *testing.T) {
	c1 := makeCid("block1")
	c2 := makeCid("block2")
	c3 := makeCid("block3")

	f := NewFrontier(c1)

	// Initially has root
	require.False(t, f.Empty())
	require.Equal(t, 1, f.Size())
	require.Equal(t, 0, f.SeenCount())

	// Pop returns root
	got := f.Pop()
	require.Equal(t, c1, got)
	require.True(t, f.Empty())

	// Mark as seen
	f.MarkSeen(c1)
	require.True(t, f.Seen(c1))
	require.False(t, f.Seen(c2))
	require.Equal(t, 1, f.SeenCount())

	// PushAll adds children (in reverse for DFS)
	f.PushAll([]cid.Cid{c2, c3})
	require.Equal(t, 2, f.Size())

	// DFS order: c2 first (was added last due to reverse)
	got = f.Pop()
	require.Equal(t, c2, got)

	got = f.Pop()
	require.Equal(t, c3, got)
	require.True(t, f.Empty())

	// PushAll skips seen CIDs
	f.MarkSeen(c2)
	f.PushAll([]cid.Cid{c1, c2, c3}) // c1 already seen
	require.Equal(t, 1, f.Size())    // Only c3 added
	got = f.Pop()
	require.Equal(t, c3, got)
}

func makeCid(s string) cid.Cid {
	h, _ := mh.Sum([]byte(s), mh.SHA2_256, -1)
	return cid.NewCidV1(cid.Raw, h)
}
