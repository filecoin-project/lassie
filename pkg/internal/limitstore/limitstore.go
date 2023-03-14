package limitstore

import (
	"context"
	"fmt"
	"io"

	"github.com/filecoin-project/lassie/pkg/types"
)

type ErrExceededLimit struct {
	Limit uint64
}

func (e ErrExceededLimit) Error() string {
	return fmt.Sprintf("cannot write - exceeded block limit: %d", e.Limit)
}

var _ io.Closer = (*LimitStore)(nil)

type LimitStore struct {
	types.ReadableWritableStorage
	counter uint64
	limit   uint64
}

func NewLimitStore(storage types.ReadableWritableStorage, limit uint64) *LimitStore {
	return &LimitStore{
		ReadableWritableStorage: storage,
		limit:                   limit,
	}
}

func (ls *LimitStore) Put(ctx context.Context, key string, data []byte) error {
	if ls.counter >= ls.limit {
		return ErrExceededLimit{ls.limit}
	}
	has, err := ls.ReadableWritableStorage.Has(ctx, key)
	if err != nil {
		return err
	}
	if has {
		return nil
	}
	ls.counter++
	return ls.ReadableWritableStorage.Put(ctx, key, data)
}

func (ls *LimitStore) Close() error {
	if closer, ok := ls.ReadableWritableStorage.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
