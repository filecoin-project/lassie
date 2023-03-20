package storage

import ipldstorage "github.com/ipld/go-ipld-prime/storage"

type ReadableWritableStorage interface {
	ipldstorage.ReadableStorage
	ipldstorage.WritableStorage
	ipldstorage.StreamingReadableStorage
}
