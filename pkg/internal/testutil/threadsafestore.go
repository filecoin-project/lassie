package testutil

import (
	"context"
	"io"
	"sync"

	"github.com/ipld/go-ipld-prime/storage"
)

type ParentStore interface {
	storage.ReadableStorage
	storage.StreamingReadableStorage
	storage.WritableStorage
}

var _ ParentStore = &ThreadsafeStore{}

type ThreadsafeStore struct {
	ParentStore
	lk sync.RWMutex
}

func (tss *ThreadsafeStore) Get(ctx context.Context, key string) ([]byte, error) {
	tss.lk.RLock()
	defer tss.lk.RUnlock()
	return tss.ParentStore.Get(ctx, key)
}

func (tss *ThreadsafeStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	tss.lk.RLock()
	defer tss.lk.RUnlock()
	return tss.ParentStore.GetStream(ctx, key)
}

func (tss *ThreadsafeStore) Put(ctx context.Context, key string, content []byte) error {
	tss.lk.Lock()
	defer tss.lk.Unlock()

	return tss.ParentStore.Put(ctx, key, content)
}
