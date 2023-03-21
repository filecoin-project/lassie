package storage

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var rng = rand.New(rand.NewSource(3333))
var rngLk sync.Mutex

func TestDeferredCarWriterForPath(t *testing.T) {
	ctx := context.Background()
	testCid1, testData1 := randBlock()
	testCid2, testData2 := randBlock()

	tmpFile := t.TempDir() + "/test.car"

	cw := NewDeferredCarWriterForPath(testCid1, tmpFile)

	_, err := os.Stat(tmpFile)
	require.True(t, os.IsNotExist(err))

	require.NoError(t, cw.Put(ctx, testCid1.KeyString(), testData1))
	require.NoError(t, cw.Put(ctx, testCid2.KeyString(), testData2))

	stat, err := os.Stat(tmpFile)
	require.NoError(t, err)
	require.True(t, stat.Size() > int64(len(testData1)+len(testData2)))

	require.NoError(t, cw.Close())

	// shouldn't be deleted
	_, err = os.Stat(tmpFile)
	require.NoError(t, err)

	r, err := os.Open(tmpFile)
	require.NoError(t, err)
	carv2, err := carv2.NewBlockReader(r)
	require.NoError(t, err)

	// compare CAR contents to what we wrote
	require.Equal(t, carv2.Roots, []cid.Cid{testCid1})
	require.Equal(t, carv2.Version, uint64(1))

	blk, err := carv2.Next()
	require.NoError(t, err)
	require.Equal(t, blk.Cid(), testCid1)
	require.Equal(t, blk.RawData(), testData1)

	blk, err = carv2.Next()
	require.NoError(t, err)
	require.Equal(t, blk.Cid(), testCid2)
	require.Equal(t, blk.RawData(), testData2)

	_, err = carv2.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestDeferredCarWriterForStream(t *testing.T) {
	for _, tc := range []string{"path", "stream"} {
		tc := tc
		t.Run(tc, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			testCid1, testData1 := randBlock()
			testCid2, testData2 := randBlock()
			testCid3, _ := randBlock()

			var cw *DeferredCarWriter
			var buf bytes.Buffer
			tmpFile := t.TempDir() + "/test.car"

			if tc == "path" {
				cw = NewDeferredCarWriterForPath(testCid1, tmpFile)
				_, err := os.Stat(tmpFile)
				require.True(t, os.IsNotExist(err))
			} else {
				cw = NewDeferredCarWriterForStream(testCid1, &buf)
				require.Equal(t, buf.Len(), 0)
			}

			has, err := cw.Has(ctx, testCid3.KeyString())
			require.NoError(t, err)
			require.False(t, has)

			require.NoError(t, cw.Put(ctx, testCid1.KeyString(), testData1))
			has, err = cw.Has(ctx, testCid1.KeyString())
			require.NoError(t, err)
			require.True(t, has)
			require.NoError(t, cw.Put(ctx, testCid2.KeyString(), testData2))
			has, err = cw.Has(ctx, testCid1.KeyString())
			require.NoError(t, err)
			require.True(t, has)
			has, err = cw.Has(ctx, testCid2.KeyString())
			require.NoError(t, err)
			require.True(t, has)
			has, err = cw.Has(ctx, testCid3.KeyString())
			require.NoError(t, err)
			require.False(t, has)

			if tc == "path" {
				stat, err := os.Stat(tmpFile)
				require.NoError(t, err)
				require.True(t, stat.Size() > int64(len(testData1)+len(testData2)))
			} else {
				require.True(t, buf.Len() > len(testData1)+len(testData2))
			}

			require.NoError(t, cw.Close())

			var rdr *carv2.BlockReader
			if tc == "path" {
				r, err := os.Open(tmpFile)
				require.NoError(t, err)
				rdr, err = carv2.NewBlockReader(r)
				require.NoError(t, err)
			} else {
				rdr, err = carv2.NewBlockReader(&buf)
				require.NoError(t, err)
			}

			// compare CAR contents to what we wrote
			require.Equal(t, rdr.Roots, []cid.Cid{testCid1})
			require.Equal(t, rdr.Version, uint64(1))

			blk, err := rdr.Next()
			require.NoError(t, err)
			require.Equal(t, blk.Cid(), testCid1)
			require.Equal(t, blk.RawData(), testData1)

			blk, err = rdr.Next()
			require.NoError(t, err)
			require.Equal(t, blk.Cid(), testCid2)
			require.Equal(t, blk.RawData(), testData2)

			_, err = rdr.Next()
			require.ErrorIs(t, err, io.EOF)
		})
	}
}

func TestDeferredCarWriterPutCb(t *testing.T) {
	ctx := context.Background()
	testCid1, testData1 := randBlock()
	testCid2, testData2 := randBlock()

	var buf bytes.Buffer
	cw := NewDeferredCarWriterForStream(testCid1, &buf)

	var pc1 int
	cw.OnPut(func(ii int) {
		switch pc1 {
		case 0:
			require.Equal(t, len(testData1), ii)
		case 1:
			require.Equal(t, len(testData2), ii)
		default:
			require.Fail(t, "unexpected put callback")
		}
		pc1++
	}, false)
	var pc2 int
	cw.OnPut(func(ii int) {
		switch pc2 {
		case 0:
			require.Equal(t, len(testData1), ii)
		case 1:
			require.Equal(t, len(testData2), ii)
		default:
			require.Fail(t, "unexpected put callback")
		}
		pc2++
	}, false)
	var pc3 int
	cw.OnPut(func(ii int) {
		switch pc3 {
		case 0:
			require.Equal(t, len(testData1), ii)
		default:
			require.Fail(t, "unexpected put callback")
		}
		pc3++
	}, true)

	require.NoError(t, cw.Put(ctx, testCid1.KeyString(), testData1))
	require.NoError(t, cw.Put(ctx, testCid2.KeyString(), testData2))
	require.NoError(t, cw.Close())

	require.Equal(t, 2, pc1)
	require.Equal(t, 2, pc2)
	require.Equal(t, 1, pc3)
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
