package limitstore

import (
	"context"
	"fmt"

	"github.com/ipld/go-ipld-prime/storage"
)

type ErrExceededLimit struct {
	Limit uint64
}

func (e ErrExceededLimit) Error() string {
	return fmt.Sprintf("cannot write - exceeded block limit: %d", e.Limit)
}

type Storage interface {
	storage.ReadableStorage
	storage.WritableStorage
	storage.StreamingReadableStorage
}

type LimitStore struct {
	Storage
	counter uint64
	limit   uint64
}

func NewLimitStore(storage Storage, limit uint64) *LimitStore {
	return &LimitStore{
		Storage: storage,
		limit:   limit,
	}
}

func (ls *LimitStore) Put(ctx context.Context, key string, data []byte) error {
	if ls.counter >= ls.limit {
		return ErrExceededLimit{ls.limit}
	}
	has, err := ls.Storage.Has(ctx, key)
	if err != nil {
		return err
	}
	if has {
		return nil
	}
	ls.counter++
	return ls.Storage.Put(ctx, key, data)
}
