package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
)

type TeeingFile struct {
	ctx     context.Context
	lk      sync.Mutex
	file    *os.File
	tee     bytes.Buffer
	closeCh chan struct{}

	offset int64
}

func NewTeeingFile(ctx context.Context, file *os.File) *TeeingFile {
	return &TeeingFile{
		ctx:     ctx,
		file:    file,
		closeCh: make(chan struct{}),
	}
}

func (t *TeeingFile) WriteTo(out io.Writer) error {
	errCh := make(chan error)
	go func() {
		for {
			select {
			case <-t.ctx.Done():
				errCh <- t.ctx.Err()
				return
			case <-t.closeCh:
				errCh <- nil
				return
			default:
			}
			t.lk.Lock()
			t.tee.WriteTo(out)
			t.lk.Unlock()
		}
	}()
	return <-errCh
}

func (t *TeeingFile) WriteAt(p []byte, off int64) (n int, err error) {
	if off < t.offset {
		return 0, fmt.Errorf("rewind seek to %d not supported by TeeingFile, expected >= %d", off, t.offset)
	}
	t.lk.Lock()
	defer t.lk.Unlock()
	for t.offset < off {
		// Write zeros to the tee until we reach the offset.
		len := off - t.offset
		if len > 1024 {
			len = 1024
		}
		if _, err := t.tee.Write(make([]byte, len)); err != nil {
			return 0, err
		}
		t.offset += len
	}
	if _, err := t.tee.Write(p); err != nil {
		return 0, err
	}
	t.offset += int64(len(p))
	return t.file.WriteAt(p, off)
}

func (t *TeeingFile) Close() error {
	t.closeCh <- struct{}{}
	return t.file.Close()
}

func (t *TeeingFile) ReadAt(p []byte, off int64) (n int, err error) {
	return t.file.ReadAt(p, off)
}

func (t *TeeingFile) Read(p []byte) (n int, err error) {
	return t.file.Read(p)
}

func (t *TeeingFile) Truncate(size int64) error {
	return t.file.Truncate(size)
}

func (t *TeeingFile) Stat() (os.FileInfo, error) {
	return t.file.Stat()
}
