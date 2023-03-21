package limitstore_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/lassie/pkg/internal/testutil"
	"github.com/filecoin-project/lassie/pkg/storage/limitstore"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/stretchr/testify/require"
)

func TestLimitStore(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	ms := &memstore.Store{Bag: make(map[string][]byte)}

	err := ms.Put(ctx, "apples", testutil.RandomBytes(1000))
	req.NoError(err)

	limitStore := limitstore.NewLimitStore(ms, 5)
	has, err := limitStore.Has(ctx, "apples")
	req.NoError(err)
	req.True(has)

	fruits := map[string][]byte{
		"oranges": testutil.RandomBytes(1000),
		"bananas": testutil.RandomBytes(1000),
		"plums":   testutil.RandomBytes(1000),
		"grapes":  testutil.RandomBytes(1000),
	}
	for fruit, data := range fruits {
		err := limitStore.Put(ctx, fruit, data)
		req.NoError(err)
	}

	for fruit := range fruits {
		has, err := limitStore.Has(ctx, fruit)
		req.NoError(err)
		req.True(has)
		data, err := limitStore.Get(ctx, fruit)
		req.NoError(err)
		req.Equal(fruits[fruit], data)
	}

	// writing the fruits again is ok cause of the has check
	for fruit, data := range fruits {
		err := limitStore.Put(ctx, fruit, data)
		req.NoError(err)
	}

	// put last block (the first block to the underlying store does not count)
	err = limitStore.Put(ctx, "cheese", testutil.RandomBytes(1000))
	req.NoError(err)

	// put block over limit
	err = limitStore.Put(ctx, "yogurt", testutil.RandomBytes(1000))
	req.EqualError(err, limitstore.ErrExceededLimit{Limit: 5}.Error())
}
