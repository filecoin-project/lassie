package storage

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

var _ types.ReadableWritableStorage = (*StreamingStore)(nil)

// StreamingStore writes blocks directly to output without retaining data.
// Get() always returns NotFound; Has() checks the seen set; Put() writes
// to output and marks the CID as seen.
type StreamingStore struct {
	seen      map[cid.Cid]struct{}
	outWriter linking.BlockWriteOpener
	mu        sync.RWMutex
	closed    bool
}

func NewStreamingStore(outWriter linking.BlockWriteOpener) *StreamingStore {
	return &StreamingStore{
		seen:      make(map[cid.Cid]struct{}),
		outWriter: outWriter,
	}
}

func (s *StreamingStore) Has(ctx context.Context, key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false, errClosed
	}

	c, err := cid.Cast([]byte(key))
	if err != nil {
		return false, err
	}

	_, ok := s.seen[c]
	return ok, nil
}

func (s *StreamingStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, errClosed
	}

	c, err := cid.Cast([]byte(key))
	if err != nil {
		return nil, err
	}

	return nil, carstorage.ErrNotFound{Cid: c}
}

func (s *StreamingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, errClosed
	}

	c, err := cid.Cast([]byte(key))
	if err != nil {
		return nil, err
	}

	return nil, carstorage.ErrNotFound{Cid: c}
}

func (s *StreamingStore) Put(ctx context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errClosed
	}

	c, err := cid.Cast([]byte(key))
	if err != nil {
		return err
	}

	if _, ok := s.seen[c]; ok {
		return nil
	}

	if err := s.writeToOutput(ctx, c, data); err != nil {
		return err
	}

	s.seen[c] = struct{}{}
	return nil
}

func (s *StreamingStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *StreamingStore) Seen() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.seen)
}

func (s *StreamingStore) writeToOutput(ctx context.Context, c cid.Cid, data []byte) error {
	w, commit, err := s.outWriter(linking.LinkContext{Ctx: ctx})
	if err != nil {
		return err
	}

	n, err := bytes.NewBuffer(data).WriteTo(w)
	if err != nil {
		return err
	}
	if n != int64(len(data)) {
		return io.ErrShortWrite
	}

	return commit(cidlink.Link{Cid: c})
}
