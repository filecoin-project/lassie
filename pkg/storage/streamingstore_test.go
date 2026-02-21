package storage

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

func TestStreamingStore(t *testing.T) {
	ctx := context.Background()
	testCid1, testData1 := randBlock()
	testCid2, testData2 := randBlock()
	testCid3, _ := randBlock()

	written := make(map[cid.Cid][]byte)
	bwo := func(ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		var buf bytes.Buffer
		return &buf, func(lnk ipld.Link) error {
			written[lnk.(cidlink.Link).Cid] = buf.Bytes()
			return nil
		}, nil
	}

	ss := NewStreamingStore(bwo)
	defer ss.Close()

	has, err := ss.Has(ctx, testCid1.KeyString())
	require.NoError(t, err)
	require.False(t, has)
	require.Equal(t, 0, ss.Seen())

	// Get/GetStream always return NotFound — streaming doesn't retain data
	_, err = ss.Get(ctx, testCid1.KeyString())
	require.Error(t, err)
	nf, ok := err.(interface{ NotFound() bool })
	require.True(t, ok)
	require.True(t, nf.NotFound())

	_, err = ss.GetStream(ctx, testCid1.KeyString())
	require.Error(t, err)
	nf, ok = err.(interface{ NotFound() bool })
	require.True(t, ok)
	require.True(t, nf.NotFound())

	require.NoError(t, ss.Put(ctx, testCid1.KeyString(), testData1))
	require.Len(t, written, 1)
	require.Equal(t, testData1, written[testCid1])
	require.Equal(t, 1, ss.Seen())

	has, err = ss.Has(ctx, testCid1.KeyString())
	require.NoError(t, err)
	require.True(t, has)

	// Get still returns NotFound after Put — data not retained
	_, err = ss.Get(ctx, testCid1.KeyString())
	require.Error(t, err)
	nf, ok = err.(interface{ NotFound() bool })
	require.True(t, ok)
	require.True(t, nf.NotFound())

	require.NoError(t, ss.Put(ctx, testCid2.KeyString(), testData2))
	require.Len(t, written, 2)
	require.Equal(t, testData2, written[testCid2])
	require.Equal(t, 2, ss.Seen())

	// duplicate Put is a no-op
	require.NoError(t, ss.Put(ctx, testCid1.KeyString(), testData1))
	require.Len(t, written, 2) // Still 2
	require.Equal(t, 2, ss.Seen())

	has, err = ss.Has(ctx, testCid3.KeyString())
	require.NoError(t, err)
	require.False(t, has)
}

func TestStreamingStoreClose(t *testing.T) {
	ctx := context.Background()
	testCid1, testData1 := randBlock()

	bwo := func(ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		var buf bytes.Buffer
		return &buf, func(lnk ipld.Link) error { return nil }, nil
	}

	ss := NewStreamingStore(bwo)
	require.NoError(t, ss.Close())

	// All operations return errClosed after close
	_, err := ss.Has(ctx, testCid1.KeyString())
	require.Equal(t, errClosed, err)

	_, err = ss.Get(ctx, testCid1.KeyString())
	require.Equal(t, errClosed, err)

	_, err = ss.GetStream(ctx, testCid1.KeyString())
	require.Equal(t, errClosed, err)

	err = ss.Put(ctx, testCid1.KeyString(), testData1)
	require.Equal(t, errClosed, err)
}
