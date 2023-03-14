package bitswaphelpers

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/filecoin-project/lassie/pkg/storage"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/linking/preload"
	"go.uber.org/multierr"
)

type preloadingLink struct {
	refCnt     uint64
	loadSyncer sync.Once
	loaded     chan struct{}
	err        error
}

type request struct {
	linkCtx linking.LinkContext
	link    ipld.Link
}

type PreloadCachingStorage struct {
	parentLinkSystem linking.LinkSystem
	fetcher          linking.BlockReadOpener
	concurrency      int

	cacheStorage storage.ReadableWritableStorage
	cancel       context.CancelFunc

	TraversalLinkSystem *linking.LinkSystem
	BitswapLinkSystem   *linking.LinkSystem

	notFound   map[string]struct{}
	preloadsLk sync.RWMutex
	preloads   map[ipld.Link]*preloadingLink
	requests   chan request

	preloadedHits  int
	preloadingHits int
	preloadMisses  int
}

func NewPreloadCachingStorage(
	parentLinkSystem linking.LinkSystem,
	cacheStorage storage.ReadableWritableStorage,
	fetcher linking.BlockReadOpener,
	concurrency int,
) (*PreloadCachingStorage, error) {
	cs := &PreloadCachingStorage{
		fetcher:          fetcher,
		parentLinkSystem: parentLinkSystem,
		concurrency:      concurrency,
		cacheStorage:     cacheStorage,
		notFound:         make(map[string]struct{}),
		preloads:         make(map[ipld.Link]*preloadingLink),
		requests:         make(chan request),
	}
	// LinkSystem for traversal is a copy of the parent but with the read
	// operation replaced with our multi-functional loader
	tls := parentLinkSystem
	tls.StorageReadOpener = cs.Loader
	cs.TraversalLinkSystem = &tls

	// LinkSystem for bitswap is a copy of the parent but with the read and write
	// operations working directly on the cache
	bls := parentLinkSystem
	bls.StorageReadOpener = cs.cacheReadOpener
	bls.StorageWriteOpener = cs.cacheWriteOpener
	cs.BitswapLinkSystem = &bls

	return cs, nil
}

func (cs *PreloadCachingStorage) Preloader(preloadCtx preload.PreloadContext, links []preload.Link) {
	cs.preloadsLk.Lock()
	defer cs.preloadsLk.Unlock()

	for _, link := range links {
		// check not found list
		if _, nf := cs.notFound[string(link.Link.(cidlink.Link).Cid.Hash())]; nf {
			continue
		}

		// check preload list
		if pl, existing := cs.preloads[link.Link]; existing {
			pl.refCnt++
			continue
		}

		linkCtx := linking.LinkContext{
			Ctx:        preloadCtx.Ctx,
			LinkPath:   preloadCtx.BasePath.AppendSegment(link.Segment),
			LinkNode:   link.LinkNode,
			ParentNode: preloadCtx.ParentNode,
		}

		// check parent
		if cs.parentLinkSystem.StorageReadOpener != nil {
			if r, err := cs.parentLinkSystem.StorageReadOpener(linkCtx, link.Link); r != nil && err == nil {
				if closer, ok := r.(io.Closer); ok {
					closer.Close()
				}
				continue // found in parent
			} else if err != nil {
				if nf, ok := err.(interface{ NotFound() bool }); !ok || !nf.NotFound() {
					log.Errorf("preload parent StorageReadOpener failed: %s", err.Error())
				}
			} // else not found
		}

		// check cache
		key := link.Link.(cidlink.Link).Cid.KeyString()
		if has, err := cs.cacheStorage.Has(preloadCtx.Ctx, key); err != nil {
			log.Errorf("preload storage Has() failed: %s", err.Error())
		} else if has {
			continue
		}

		// haven't seen this link before, queue for preloading
		cs.preloads[link.Link] = &preloadingLink{
			loaded: make(chan struct{}),
			refCnt: 1,
		}
		select {
		case <-preloadCtx.Ctx.Done():
		case cs.requests <- request{
			linkCtx: linkCtx,
			link:    link.Link,
		}:
		}
	}
}

func (cs *PreloadCachingStorage) Loader(linkCtx linking.LinkContext, link ipld.Link) (io.Reader, error) {
	// check parent
	if cs.parentLinkSystem.StorageReadOpener != nil {
		if r, err := cs.parentLinkSystem.StorageReadOpener(linkCtx, link); r != nil && err == nil {
			return r, nil // found in parent, return
		} else if err != nil {
			if nf, ok := err.(interface{ NotFound() bool }); !ok || !nf.NotFound() {
				return nil, err // real error
			}
		} // else not found
	}

	// defer cs.PrintStats()

	key := link.(cidlink.Link).Cid.KeyString()
	if has, err := cs.cacheStorage.Has(linkCtx.Ctx, key); err != nil {
		return nil, err
	} else if has {
		// have a preloaded block
		cs.preloadedHits++
		// load from cache, write to parent
		return loadTo(
			cs.cacheReadOpener,
			[]linking.BlockWriteOpener{cs.parentLinkSystem.StorageWriteOpener},
			linkCtx,
			link)
	}

	// hit the preloader
	cs.preloadsLk.Lock()
	if _, nf := cs.notFound[string(link.(cidlink.Link).Cid.Hash())]; nf {
		cs.preloadsLk.Unlock()
		return nil, carstorage.ErrNotFound{Cid: link.(cidlink.Link).Cid}
	}

	pl, ok := cs.preloads[link]
	if ok {
		pl.refCnt--
		if pl.refCnt <= 0 {
			delete(cs.preloads, link)
		}
	}
	cs.preloadsLk.Unlock()
	if !ok {
		cs.preloadMisses++
		// load from fetcher, write to parent and cache and return
		return loadTo(
			func(linkCtx linking.LinkContext, link ipld.Link) (io.Reader, error) {
				r, err := cs.fetcher(linkCtx, link)
				if nf, ok := err.(interface{ NotFound() bool }); ok && nf.NotFound() {
					cs.preloadsLk.Lock()
					cs.notFound[string(link.(cidlink.Link).Cid.Hash())] = struct{}{}
					cs.preloadsLk.Unlock()
				}
				return r, err
			},
			[]linking.BlockWriteOpener{cs.cacheWriteOpener, cs.parentLinkSystem.StorageWriteOpener},
			linkCtx,
			link)
	}
	cs.preloadingHits++
	cs.preloadLink(pl, linkCtx, link)
	select {
	case <-linkCtx.Ctx.Done():
		return nil, linkCtx.Ctx.Err()
	case <-pl.loaded:
		if pl.err != nil {
			return nil, pl.err
		}
		// TODO: delete from storage if/when possible?  if pl.refCnt <= 0 {}
		// load from cache, write direct to parent and return
		return loadTo(
			cs.cacheReadOpener,
			[]linking.BlockWriteOpener{cs.parentLinkSystem.StorageWriteOpener},
			linkCtx,
			link)
	}
}

func (cs *PreloadCachingStorage) cacheReadOpener(linkCtx linking.LinkContext, link ipld.Link) (io.Reader, error) {
	key := link.(cidlink.Link).Cid.KeyString()
	return cs.cacheStorage.GetStream(linkCtx.Ctx, key)
}

func (cs *PreloadCachingStorage) cacheWriteOpener(linkCtx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
	var buf bytes.Buffer
	return &buf, func(link ipld.Link) error {
		key := link.(cidlink.Link).Cid.KeyString()
		return cs.cacheStorage.Put(linkCtx.Ctx, key, buf.Bytes())
	}, nil
}

func loadTo(
	source linking.BlockReadOpener,
	destination []linking.BlockWriteOpener,
	linkCtx linking.LinkContext,
	link ipld.Link) (io.Reader, error) {

	reader, err := source(linkCtx, link)
	if err != nil {
		return nil, err
	}
	blc := make([]linking.BlockWriteCommitter, 0, len(destination))
	for _, blo := range destination {
		writer, c, err := blo(linkCtx)
		if err != nil {
			return nil, err
		}
		blc = append(blc, c)
		reader = io.TeeReader(reader, writer)
	}
	byts, err := io.ReadAll(reader) // slurp it in so the writers can commit
	if err != nil {
		return nil, err
	}
	err = nil
	for _, c := range blc {
		err = multierr.Append(err, c(link))
	}
	return bytes.NewBuffer(byts), err
}

func (cs *PreloadCachingStorage) preloadLink(pl *preloadingLink, linkCtx linking.LinkContext, link ipld.Link) {
	pl.loadSyncer.Do(func() {
		defer close(pl.loaded)
		reader, err := cs.fetcher(linkCtx, link)
		if err != nil {
			if nf, ok := err.(interface{ NotFound() bool }); ok && nf.NotFound() {
				cs.preloadsLk.Lock()
				cs.notFound[string(link.(cidlink.Link).Cid.Hash())] = struct{}{}
				cs.preloadsLk.Unlock()
			}
			pl.err = err
		} else {
			data, err := io.ReadAll(reader)
			if err != nil {
				pl.err = err
				return
			}
			if err := cs.cacheStorage.Put(linkCtx.Ctx, link.(cidlink.Link).Cid.KeyString(), data); err != nil {
				pl.err = err
				return
			}
		}
	})
}

func (cs *PreloadCachingStorage) Start(ctx context.Context) {
	if cs.cancel != nil {
		panic("already started")
	}
	ctx, cs.cancel = context.WithCancel(ctx)
	go cs.run(ctx)
}

func (cs *PreloadCachingStorage) run(ctx context.Context) {
	feed := make(chan *request)
	defer func() {
		//close(feed)
	}()

	for i := 0; i < cs.concurrency; i++ {
		go func() {
			for request := range feed {
				cs.preloadsLk.RLock()
				pl, ok := cs.preloads[request.link]
				cs.preloadsLk.RUnlock()
				if !ok {
					continue
				}
				cs.preloadLink(pl, request.linkCtx, request.link)
			}
		}()
	}

	requestBuffer := list.New()
	var send chan<- *request
	var next *request
	for {
		select {
		case request := <-cs.requests:
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

func (cs *PreloadCachingStorage) Stop() error {
	if cs.cancel != nil {
		cs.cancel()
		cs.cancel = nil
	}
	if closer, ok := cs.cacheStorage.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

/*
func (cs *PreloadCachingStorage) Has(ctx context.Context, key string) (bool, error) {
	return cs.cacheStorage.Has(ctx, key)
}

func (cs *PreloadCachingStorage) Get(ctx context.Context, key string) ([]byte, error) {
	return cs.cacheStorage.Get(ctx, key)
}

func (cs *PreloadCachingStorage) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	return cs.cacheStorage.GetStream(ctx, key)
}

func (cs *PreloadCachingStorage) Put(ctx context.Context, key string, content []byte) error {
	return cs.cacheStorage.Put(ctx, key, content)
}
*/

func (cs *PreloadCachingStorage) PrintStats() {
	fmt.Println("PreloadCachingStorage stats:")
	fmt.Println("  preloads:", len(cs.preloads))
	fmt.Println("  not found:", len(cs.notFound))
	fmt.Println("  preloaded hits:", cs.preloadedHits)
	fmt.Println("  preloading hits:", cs.preloadingHits)
	fmt.Println("  preload misses:", cs.preloadMisses)
}
