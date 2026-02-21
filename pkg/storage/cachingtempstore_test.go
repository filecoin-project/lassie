package storage

import (
	"bytes"
	"context"
	"io"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage/deferred"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var rng = rand.NewChaCha8([32]byte{33, 33, 33, 33})
var rngLk sync.Mutex

func TestDeferredCarWriterWritesCARv1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tc := []struct {
		name            string
		readBeforeWrite bool
		readDuringWrite bool
		readAfterClose  bool
	}{
		{
			name:            "read before write",
			readBeforeWrite: true,
		},
		{
			name:            "read during write",
			readDuringWrite: true,
		},
		{
			name:           "read after close",
			readAfterClose: true,
		},
		{
			name:            "read before, during and after close",
			readBeforeWrite: true,
			readDuringWrite: true,
			readAfterClose:  true,
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			testCid1, testData1 := randBlock()
			testCid2, testData2 := randBlock()

			var buf bytes.Buffer
			cw := deferred.NewDeferredCarWriterForStream(&buf, []cid.Cid{testCid1})
			ss := NewCachingTempStore(cw.BlockWriteOpener(), NewDeferredStorageCar("", testCid1))
			t.Cleanup(func() { ss.Close() })

			if tt.readBeforeWrite {
				has, err := ss.Has(ctx, randCid().KeyString())
				require.NoError(t, err)
				require.False(t, has)
				got, err := ss.Get(ctx, randCid().KeyString())
				require.Error(t, err)
				nf, ok := err.(interface{ NotFound() bool })
				require.True(t, ok)
				require.True(t, nf.NotFound())
				require.Nil(t, got)
				gotStream, err := ss.GetStream(ctx, randCid().KeyString())
				require.Error(t, err)
				nf, ok = err.(interface{ NotFound() bool })
				require.True(t, ok)
				require.True(t, nf.NotFound())
				require.Nil(t, gotStream)
			}

			require.NoError(t, ss.Put(ctx, testCid1.KeyString(), testData1))

			if tt.readDuringWrite {
				got, err := ss.Get(ctx, testCid1.KeyString())
				require.NoError(t, err)
				require.Equal(t, testData1, got)
				gotStream, err := ss.GetStream(ctx, testCid1.KeyString())
				require.NoError(t, err)
				got, err = io.ReadAll(gotStream)
				require.NoError(t, err)
				require.Equal(t, testData1, got)

				has, err := ss.Has(ctx, randCid().KeyString())
				require.NoError(t, err)
				require.False(t, has)
				got, err = ss.Get(ctx, randCid().KeyString())
				require.Error(t, err)
				nf, ok := err.(interface{ NotFound() bool })
				require.True(t, ok)
				require.True(t, nf.NotFound())
				require.Nil(t, got)
				gotStream, err = ss.GetStream(ctx, randCid().KeyString())
				require.Error(t, err)
				nf, ok = err.(interface{ NotFound() bool })
				require.True(t, ok)
				require.True(t, nf.NotFound())
				require.Nil(t, gotStream)
			}

			require.NoError(t, ss.Put(ctx, testCid2.KeyString(), testData2))

			if tt.readDuringWrite {
				got, err := ss.Get(ctx, testCid2.KeyString())
				require.NoError(t, err)
				require.Equal(t, testData2, got)
				gotStream, err := ss.GetStream(ctx, testCid2.KeyString())
				require.NoError(t, err)
				got, err = io.ReadAll(gotStream)
				require.NoError(t, err)
				require.Equal(t, testData2, got)
			}

			require.NoError(t, ss.Close())

			if tt.readAfterClose {
				require.EqualError(t, ss.Put(ctx, randCid().KeyString(), testData1), "store closed")
				has, err := ss.Has(ctx, randCid().KeyString())
				require.EqualError(t, err, "store closed")
				require.False(t, has)
				got, err := ss.Get(ctx, randCid().KeyString())
				require.EqualError(t, err, "store closed")
				require.Nil(t, got)
				gotStream, err := ss.GetStream(ctx, randCid().KeyString())
				require.EqualError(t, err, "store closed")
				require.Nil(t, gotStream)

				has, err = ss.Has(ctx, testCid1.KeyString())
				require.EqualError(t, err, "store closed")
				require.False(t, has)
				got, err = ss.Get(ctx, testCid1.KeyString())
				require.EqualError(t, err, "store closed")
				require.Nil(t, got)
				gotStream, err = ss.GetStream(ctx, testCid1.KeyString())
				require.EqualError(t, err, "store closed")
				require.Nil(t, gotStream)
			}

			reader, err := carv2.NewBlockReader(&buf)
			require.NoError(t, err)

			require.Equal(t, []cid.Cid{testCid1}, reader.Roots)
			require.Equal(t, uint64(1), reader.Version)

			blk, err := reader.Next()
			require.NoError(t, err)
			require.Equal(t, testCid1, blk.Cid())
			require.Equal(t, testData1, blk.RawData())

			blk, err = reader.Next()
			require.NoError(t, err)
			require.Equal(t, testCid2, blk.Cid())
			require.Equal(t, testData2, blk.RawData())
		})
	}
}

func randBlock() (cid.Cid, []byte) {
	data := make([]byte, 1024)
	rngLk.Lock()
	rng.Read(data)
	rngLk.Unlock()
	h, err := mh.Sum(data, mh.SHA2_512, -1)
	if err != nil {
		panic(err)
	}
	return cid.NewCidV1(cid.Raw, h), data
}

func randCid() cid.Cid {
	c, _ := randBlock()
	return c
}
