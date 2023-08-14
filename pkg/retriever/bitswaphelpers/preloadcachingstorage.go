package bitswaphelpers

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/linking/preload"
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

	cacheLinkSystem linking.LinkSystem
	cancel          context.CancelFunc

	TraversalLinkSystem *linking.LinkSystem
	BitswapLinkSystem   *linking.LinkSystem

	notFound   map[string]struct{}
	preloadsLk sync.RWMutex
	preloads   map[ipld.Link]*preloadingLink
	requests   chan request

	loadCount      int
	preloadedHits  int
	preloadingHits int
	preloadMisses  int
}

type PreloadStats struct {
	// LoadCount is the number of times the Loader was called
	LoadCount int
	// ActivePreloads is the number of links currently being preloaded
	ActivePreloads int // number of links currently being preloaded
	// NotFound is the number of links that were a decisive "not found" (by the
	// preloader)
	NotFound int
	// PreloadedHits is the number of times a link was loaded from the cache
	// (already full preloaded)
	PreloadedHits int
	// PreloadingHits is the number of times a link was loaded that was in the
	// queue to be preloaded but hadn't yet been fully loaded.
	PreloadingHits int
	// PreloadMisses is the number of times a link was not found in the cache or
	// queued to be preloaded. This should only happen once per traversal (the
	// root).
	PreloadMisses int
}

// PreloadedPercent returns the percentage of loads that were hits in the
// preloaded cache. This is a good indicator of how much of the traversal was
// able to be satisfied by the preloaded cache, however it is not able to
// capture the number of blocks that were actively being loaded when they were
// requested.
func (s PreloadStats) PreloadedPercent() uint64 {
	return uint64(math.Round(float64(s.PreloadedHits) / float64(s.LoadCount) * 100))
}

// PreloadingHitRate returns the fraction of loads that were hits in the
// queue of links to be preloaded. This means that the block was flagged as
// being needed by the preloader, but had not yet been preloaded, or was in the
// process of being preloaded when it was required by the traversal.
//
// PreloadingHitRate() + PreloadedHitRate() should be close to a value
// of 1.0 (not exactly as the first block will be a complete preloader miss),
// together they provide a measure of the performance of the preloader for the
// traversal.
func (s PreloadStats) PreloadingPercent() uint64 {
	return uint64(math.Round(float64(s.PreloadingHits) / float64(s.LoadCount) * 100))
}

// Print prints the stats to stdout
func (s PreloadStats) Print() {
	fmt.Println("PreloadCachingStorage stats:")
	fmt.Printf("%25s: %v\n", "loadCount", s.LoadCount)
	fmt.Printf("%25s: %v\n", "active preloads", s.ActivePreloads)
	fmt.Printf("%25s: %v\n", "not found", s.NotFound)
	fmt.Printf("%25s: %v\n", "preloaded hits", s.PreloadedHits)
	fmt.Printf("%25s: %v\n", "preloading hits", s.PreloadingHits)
	fmt.Printf("%25s: %v\n", "preload misses", s.PreloadMisses)
	fmt.Printf("%25s: %v%%\n", "preloaded hit percent", s.PreloadedPercent())
	fmt.Printf("%25s: %v%%\n", "preloading hit percent", s.PreloadingPercent())
}

// GetStats returns the current stats for the PreloadCachingStorage.
func (cs *PreloadCachingStorage) GetStats() PreloadStats {
	return PreloadStats{
		LoadCount:      cs.loadCount,
		ActivePreloads: len(cs.preloads),
		NotFound:       len(cs.notFound),
		PreloadedHits:  cs.preloadedHits,
		PreloadingHits: cs.preloadingHits,
		PreloadMisses:  cs.preloadMisses,
	}
}

// NewPreloadCachingStorage creates a new PreloadCachingStorage.
//
// The parentLinkSystem is used directly by the traversal for both reads
// (existing blocks, or blocks already traversed and stored) and writes (new
// blocks discovered during traversal). Writes will be properly ordered
// according to the traversal.
//
// The cacheLinkSystem is used by the preloader as a temporary space for
// preloaded blocks. Writes are not properly ordered, only according to the
// preloader's ability to fetch blocks in its queue. When a block is requested
// by the traversal and it is already in the cacheLinkSystem, it is copied to
// the parentLinkSystem.
//
// The fetcher is used by both the preloader and the loader to fetch blocks. It
// should be able to fetch blocks in a thread-safe manner.
//
// The concurrency parameter controls how many blocks can be preloaded at once
// (the preloader will fetch up to this many blocks at once using the fetcher).
func NewPreloadCachingStorage(
	parentLinkSystem linking.LinkSystem,
	cacheLinkSystem linking.LinkSystem,
	fetcher linking.BlockReadOpener,
	concurrency int,
) (*PreloadCachingStorage, error) {
	cs := &PreloadCachingStorage{
		fetcher:          fetcher,
		parentLinkSystem: parentLinkSystem,
		concurrency:      concurrency,
		cacheLinkSystem:  cacheLinkSystem,
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
	bls.StorageReadOpener = cs.cacheLinkSystem.StorageReadOpener
	bls.StorageWriteOpener = cs.cacheLinkSystem.StorageWriteOpener
	cs.BitswapLinkSystem = &bls

	return cs, nil
}

// Preloader is compatible with go-ipld-prime's linking/preload.Loader and
// should be provided to a traversal Config as the Preloader field.
func (cs *PreloadCachingStorage) Preloader(preloadCtx preload.PreloadContext, link preload.Link) {
	cs.preloadsLk.Lock()
	defer cs.preloadsLk.Unlock()

	fmt.Println("Preloader link", link.Link.String())
	logger.Debugw("preload link", "link", link)

	// links coming in here aren't necessarily deduplicated and may be ones we've
	// already attempted to load or are queued for loading, so we have to check
	// all the places we might have seen this link before before queueing it up.

	// check not found list
	if _, nf := cs.notFound[string(link.Link.(cidlink.Link).Cid.Hash())]; nf {
		return
	}

	// check preload list
	if pl, existing := cs.preloads[link.Link]; existing {
		pl.refCnt++
		return
	}

	linkCtx := linking.LinkContext{
		Ctx:        preloadCtx.Ctx,
		LinkPath:   preloadCtx.BasePath.AppendSegment(link.Segment),
		LinkNode:   link.LinkNode,
		ParentNode: preloadCtx.ParentNode,
	}

	// check parent
	if has, err := linkSystemHas(cs.parentLinkSystem, linkCtx, link.Link); err != nil {
		logger.Errorf("parent LinkSystem block existence check failed: %s", err.Error())
	} else if has {
		return
	}

	// check cache
	if has, err := linkSystemHas(cs.cacheLinkSystem, linkCtx, link.Link); err != nil {
		logger.Errorf("cache LinkSystem block existence check failed: %s", err.Error())
	} else if has {
		return
	}

	// haven't seen this link before, queue for preloading
	cs.preloads[link.Link] = &preloadingLink{
		loaded: make(chan struct{}),
		refCnt: 1,
	}
	req := request{
		linkCtx: linkCtx,
		link:    link.Link,
	}
	select {
	case <-preloadCtx.Ctx.Done():
	case cs.requests <- req:
	}
}

// Loader is compatible with go-ipld-prime's StorageReadOpener. It is not
// intended to be used directly but is wired up as the StorageReadOpener for
// the TraversalLinkSystem.
func (cs *PreloadCachingStorage) Loader(linkCtx linking.LinkContext, link ipld.Link) (io.Reader, error) {
	cs.loadCount++

	logger.Debugw("load link", "link", link)

	// 1. Check the parent LinkSystem: we may already have the the block we need
	//    next in the traversal; allow for duplicates or a pre-filled parent
	//    LinkSystem.
	// 2. Check the cache LinkSystem: we may have already fully preloaded the
	//    block we need, but not consumed it into the parent LinkSystem yet.
	// 3. Check the notFound list: we may have tried to load, or preload this
	//    block before and it was not found and flagged as such.
	// 4. Check the preloads list
	//   4a. If the block is not in the preload list, load it directly from the
	//	     fetcher and write it to both the the cache and parent LinkSystem.
	//   4b. If the block is in the preload list, wait for it to be loaded and
	//       then write it to the parent LinkSystem (it will already be in the
	//       cache).

	// 1. Check the parent LinkSystem
	if r, err := linkSystemGetStream(cs.parentLinkSystem, linkCtx, link); r != nil && err == nil {
		return r, nil // found in parent, return
	} else if err != nil {
		if nf, ok := err.(interface{ NotFound() bool }); !ok || !nf.NotFound() {
			return nil, err // real error
		}
	} // else not found

	// 2. Check the cache LinkSystem
	if r, err := linkSystemGetStream(cs.cacheLinkSystem, linkCtx, link); r != nil && err == nil {
		// have a preloaded block
		cs.preloadedHits++
		logger.Debugw("load link successfully from preload cache", "link", link)
		// loaded from cache, write to parent and return
		return cs.loadToParent(r, linkCtx, link)
	} else if err != nil {
		if nf, ok := err.(interface{ NotFound() bool }); !ok || !nf.NotFound() {
			return nil, err // real error
		}
	} // else not found

	cs.preloadsLk.Lock()

	// 3. Check the notFound list
	if _, nf := cs.notFound[string(link.(cidlink.Link).Cid.Hash())]; nf {
		cs.preloadsLk.Unlock()
		return nil, carstorage.ErrNotFound{Cid: link.(cidlink.Link).Cid}
	}

	// 4. Check the preloads list
	pl, ok := cs.preloads[link]
	if ok {
		pl.refCnt--
		if pl.refCnt <= 0 {
			delete(cs.preloads, link)
		}
	}
	cs.preloadsLk.Unlock()

	if !ok {
		// 4a. If the block is not in the preload list
		cs.preloadMisses++
		// load directly from the fetcher, if it can be fetched
		fmt.Println("cache miss, fetcher()", link.String())
		r, err := cs.fetcher(linkCtx, link)
		if err != nil {
			fmt.Println("cache miss, fetcher() failed", link.String())
			if nf, ok := err.(interface{ NotFound() bool }); ok && nf.NotFound() {
				cs.preloadsLk.Lock()
				cs.notFound[string(link.(cidlink.Link).Cid.Hash())] = struct{}{}
				cs.preloadsLk.Unlock()
			}
			return nil, err
		}
		fmt.Println("load link successfully from after cache miss, link=", link.String())
		logger.Debugw("load link successfully from after cache miss", "link", link)
		// loaded from fetcher, write to parent and return
		return cs.loadToParent(r, linkCtx, link)
	}

	// 4b. If the block is in the preload list
	cs.preloadingHits++
	// the block may be in progress, or it may be in the queue, calling
	// preloadLink() will process it right now if it's in the queue, or be a
	// noop if it's in progress with the preloader; either way we wait on
	// pl.loaded.
	fmt.Println("queueing preload link=", link.String())
	cs.preloadLink(pl, linkCtx, link)

	select {
	case <-linkCtx.Ctx.Done():
		return nil, linkCtx.Ctx.Err()
	case <-pl.loaded:
		if pl.err != nil {
			return nil, pl.err
		}
		// TODO: if an abstracted form of this code is extracted from here, we
		// should probably make affordance to allow a "delete" of the preload
		// entry since it shouldn't be needed in the preloader anymore. For the
		// Lassie case the underlying store is the same, so a delete would be a
		// noop because we'll need it for the life of the traversal anyway.
		r, err := linkSystemGetStream(cs.cacheLinkSystem, linkCtx, link)
		if err != nil {
			// this can occur if cache LS + parent LS share a store and another
			// retriever fills the block in the parent store, so we repeat the
			// parent store check here.
			if r, err := linkSystemGetStream(cs.parentLinkSystem, linkCtx, link); r != nil && err == nil {
				fmt.Println("load link successfully from after cache hit from main store, link=", link.String())
				logger.Debugw("load link successfully from after cache hit from main store", "link", link)
				return r, nil // found in parent, return
			} else if err != nil {
				return nil, err
			}
		}
		fmt.Println("load link successfully from after cache hit, link=", link.String())
		logger.Debugw("load link successfully from after cache hit", "link", link)
		// loaded from cache, write to parent and return
		return cs.loadToParent(r, linkCtx, link)
	}
}

// Load a block from a reader and pipe it to the parent LinkSystem and return as
// a reader for the traverser.
func (cs *PreloadCachingStorage) loadToParent(reader io.Reader, linkCtx linking.LinkContext, link ipld.Link) (io.Reader, error) {
	writer, c, err := cs.parentLinkSystem.StorageWriteOpener(linkCtx)
	if err != nil {
		fmt.Println("loadToParent swo err", link.String(), err.Error())
		return nil, err
	}
	byts, err := io.ReadAll(io.TeeReader(reader, writer)) // slurp it in so the writer can commit
	if err != nil {
		fmt.Println("loadToParent readAll err", link.String(), err.Error())
		return nil, err
	}
	if err = c(link); err != nil {
		fmt.Println("loadToParent commit err", link.String(), err.Error())
		return nil, err
	}
	fmt.Println("loadToParent done", link.String())
	return bytes.NewBuffer(byts), nil
}

func (cs *PreloadCachingStorage) preloadLink(pl *preloadingLink, linkCtx linking.LinkContext, link ipld.Link) {
	pl.loadSyncer.Do(func() {
		defer close(pl.loaded)
		fmt.Println("preloadLink fetcher()", link.String())
		reader, err := cs.fetcher(linkCtx, link)
		if err != nil {
			if nf, ok := err.(interface{ NotFound() bool }); ok && nf.NotFound() {
				cs.preloadsLk.Lock()
				cs.notFound[string(link.(cidlink.Link).Cid.Hash())] = struct{}{}
				cs.preloadsLk.Unlock()
			}
			pl.err = err
		} else {
			w, c, err := cs.cacheLinkSystem.StorageWriteOpener(linkCtx)
			if err != nil {
				pl.err = err
				return
			}
			if _, err := io.Copy(w, reader); err != nil {
				pl.err = err
				return
			}
			if err := c(link); err != nil {
				pl.err = err
				return
			}
		}
	})
}

// Start the preloader; this will start the background goroutines that will
// fetch blocks from the fetcher and cache them as they are flagged by the
// preloader. If you Start(), you should also Stop().
func (cs *PreloadCachingStorage) Start(ctx context.Context) error {
	if cs.cancel != nil {
		return errors.New("already started")
	}
	ctx, cs.cancel = context.WithCancel(ctx)
	go cs.run(ctx)
	return nil
}

func (cs *PreloadCachingStorage) run(ctx context.Context) {
	feed := make(chan *request)
	defer func() {
		close(feed)
	}()

	for i := 0; i < cs.concurrency; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case request, ok := <-feed:
					if !ok {
						return
					}
					cs.preloadsLk.RLock()
					pl, ok := cs.preloads[request.link]
					cs.preloadsLk.RUnlock()
					if !ok {
						continue
					}
					cs.preloadLink(pl, request.linkCtx, request.link)
				}
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

// Stop the preloader; this will stop the background goroutines.
func (cs *PreloadCachingStorage) Stop() {
	if cs.cancel != nil {
		cs.cancel()
		cs.cancel = nil
	}
}

// linkSystemHas is a Has() for a LinkSystem.
func linkSystemHas(linkSys linking.LinkSystem, linkCtx linking.LinkContext, link ipld.Link) (bool, error) {
	if linkSys.StorageReadOpener != nil {
		if r, err := linkSys.StorageReadOpener(linkCtx, link); r != nil && err == nil {
			if closer, ok := r.(io.Closer); ok {
				closer.Close()
			}
			return true, nil
		} else if err != nil {
			if nf, ok := err.(interface{ NotFound() bool }); !ok || !nf.NotFound() {
				return false, err // actual error
			} // else not found
		}
	}
	return false, nil
}

func linkSystemGetStream(linkSys linking.LinkSystem, linkCtx linking.LinkContext, link ipld.Link) (io.Reader, error) {
	if linkSys.StorageReadOpener != nil {
		return linkSys.StorageReadOpener(linkCtx, link)
	}
	// fake a not-found - because there's no linksystem for it to be found in!
	return nil, carstorage.ErrNotFound{Cid: link.(cidlink.Link).Cid}
}
