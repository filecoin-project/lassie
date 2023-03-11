package bitswaphelpers

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	carv2 "github.com/ipld/go-car/v2"
	carstore "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/linking/preload"
	"github.com/ipld/go-ipld-prime/storage"
)

type ReadableWritable interface {
	storage.ReadableStorage
	storage.WritableStorage
	storage.StreamingReadableStorage
}

type preloadingLink struct {
	refCnt     uint64
	loadSyncer sync.Once
	loaded     chan struct{}
	err        error
}

type request struct {
	linkCtx linking.LinkContext
	link    datamodel.Link
}

var _ ReadableWritable = (*PreloadStorage)(nil)

type PreloadStorage struct {
	originalLoader linking.BlockReadOpener
	parallelism    int

	f       *os.File
	storage ReadableWritable
	cancel  context.CancelFunc

	preloadsLk sync.RWMutex
	preloads   map[ipld.Link]*preloadingLink
	requests   chan request

	preloadedHits  int
	preloadingHits int
	preloadMisses  int
}

func NewPreloadStorage(originalLoader linking.BlockReadOpener, tempDir string, parallelism int) (*PreloadStorage, error) {
	// TODO: consider a non-CAR storage that we can delete blocks as they are used
	// TODO: lazily initialise like streamingstore since bitswap may not even start?
	file, err := os.CreateTemp(tempDir, "lassie_preloadstore")
	if err != nil {
		return nil, err
	}
	storage, err := carstore.NewReadableWritable(file, nil, carv2.WriteAsCarV1(true))
	if err != nil {
		return nil, err
	}

	return &PreloadStorage{
		originalLoader: originalLoader,
		parallelism:    parallelism,

		f:        file,
		storage:  storage,
		preloads: make(map[datamodel.Link]*preloadingLink),
		requests: make(chan request),
	}, nil
}

func (ps *PreloadStorage) Preloader(preloadCtx preload.PreloadContext, links []preload.Link) {
	ps.preloadsLk.Lock()
	defer ps.preloadsLk.Unlock()

	for i, link := range links {
		// check preload list
		fmt.Println("Preloader() Queueing preload of", link.Link.String(), i)
		if pl, existing := ps.preloads[link.Link]; existing {
			pl.refCnt++
			continue
		}

		// check blockstore
		key := link.Link.(cidlink.Link).Cid.KeyString()
		if has, err := ps.Has(preloadCtx.Ctx, key); err != nil {
			log.Errorf("preload storage Has() failed: %s", err.Error())
		} else if has {
			continue
		}

		// haven't seen this link before, queue for preloading
		ps.preloads[link.Link] = &preloadingLink{
			loaded: make(chan struct{}),
			refCnt: 1,
		}
		select {
		case <-preloadCtx.Ctx.Done():
		case ps.requests <- request{
			linkCtx: linking.LinkContext{
				Ctx:        preloadCtx.Ctx,
				LinkPath:   preloadCtx.BasePath.AppendSegment(link.Segment),
				LinkNode:   link.LinkNode,
				ParentNode: preloadCtx.ParentNode,
			},
			link: link.Link,
		}:
		}
	}
}

func (ps *PreloadStorage) Loader(linkCtx linking.LinkContext, link ipld.Link) (io.Reader, error) {
	defer ps.PrintStats()

	key := link.(cidlink.Link).Cid.KeyString()
	if has, err := ps.Has(linkCtx.Ctx, key); err != nil {
		return nil, err
	} else if has {
		fmt.Println("Loader", link.String(), "has block")
		// have a preloaded block
		ps.preloadedHits++
		return ps.GetStream(linkCtx.Ctx, key)
	}

	fmt.Println("Loader", link.String(), "no block, checking preloader")

	// hit the preloader
	ps.preloadsLk.Lock()
	pl, ok := ps.preloads[link]
	if ok {
		pl.refCnt--
		if pl.refCnt <= 0 {
			delete(ps.preloads, link)
		}
	}
	ps.preloadsLk.Unlock()
	if !ok {
		ps.preloadMisses++
		fmt.Println("Loader", link.String(), "not preloading, loading directly")
		defer func() {
			fmt.Println("Loader", link.String(), "done loading directly")
		}()
		return ps.originalLoader(linkCtx, link)
	}
	fmt.Println("Loader", link.String(), "preloading, waiting for load")
	ps.preloadingHits++
	ps.preloadLink(pl, linkCtx, link)
	select {
	case <-linkCtx.Ctx.Done():
		return nil, linkCtx.Ctx.Err()
	case <-pl.loaded:
		if pl.err != nil {
			fmt.Println("Error preloading link", pl.err)
			return nil, pl.err
		}
		// TODO: delete from storage if/when possible?  if pl.refCnt <= 0 {}
		return ps.GetStream(linkCtx.Ctx, key)
	}
}

func (ps *PreloadStorage) preloadLink(pl *preloadingLink, linkCtx linking.LinkContext, link datamodel.Link) {
	pl.loadSyncer.Do(func() {
		defer close(pl.loaded)
		fmt.Println("Preloading", link.String())
		reader, err := ps.originalLoader(linkCtx, link)
		if err != nil {
			pl.err = err
		} else {
			data, err := io.ReadAll(reader)
			if err != nil {
				pl.err = err
				return
			}
			if err := ps.Put(linkCtx.Ctx, link.(cidlink.Link).Cid.KeyString(), data); err != nil {
				pl.err = err
				return
			}
			fmt.Println("Preloaded", link.String())
		}
	})
}

func (ps *PreloadStorage) Start(ctx context.Context) {
	if ps.cancel != nil {
		panic("already started")
	}
	ctx, ps.cancel = context.WithCancel(ctx)
	go ps.run(ctx)
}

func (ps *PreloadStorage) run(ctx context.Context) {
	feed := make(chan *request)
	defer func() {
		fmt.Println("Ended worker")
		//close(feed)
	}()

	for i := 0; i < ps.parallelism; i++ {
		go func() {
			for request := range feed {
				ps.preloadsLk.RLock()
				pl, ok := ps.preloads[request.link]
				ps.preloadsLk.RUnlock()
				if !ok {
					continue
				}
				ps.preloadLink(pl, request.linkCtx, request.link)
			}
		}()
	}

	requestBuffer := list.New()
	var send chan<- *request
	var next *request
	for {
		select {
		case request := <-ps.requests:
			if next == nil {
				next = &request
				send = feed
			} else {
				requestBuffer.PushBack(&request)
			}
		case send <- next:
			if requestBuffer.Len() > 0 {
				next = requestBuffer.Remove(requestBuffer.Front()).(*request)
			} else {
				next = nil
				send = nil
			}
		case <-ctx.Done():
			return
		}
	}
}

func (ps *PreloadStorage) Stop() error {
	if ps.cancel != nil {
		ps.cancel()
		ps.cancel = nil
	}
	return ps.f.Close()
}

func (ps *PreloadStorage) Has(ctx context.Context, key string) (bool, error) {
	return ps.storage.Has(ctx, key)
}

func (ps *PreloadStorage) Get(ctx context.Context, key string) ([]byte, error) {
	return ps.storage.Get(ctx, key)
}

func (ps *PreloadStorage) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	return ps.storage.GetStream(ctx, key)
}

func (ps *PreloadStorage) Put(ctx context.Context, key string, content []byte) error {
	return ps.storage.Put(ctx, key, content)
}

func (ps *PreloadStorage) PrintStats() {
	fmt.Println("PreloadStorage stats:")
	fmt.Println("  preloading hits:", ps.preloadingHits)
	fmt.Println("  preloading misses:", ps.preloadMisses)
	fmt.Println("  preloaded hits:", ps.preloadedHits)
}
