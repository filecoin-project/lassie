package groupworkpool

import (
	"container/list"
	"context"
	"sync"
)

type WorkFunc func()

// GroupWorkPool is a worker pool with a fixed number of workers. The pool
// executes work in groups. Each group has a fixed maximum number of workers
// it can use.
type GroupWorkPool interface {
	// AddGroup adds a new group to the pool. Cancellation of the provided
	// context will cause the group to be removed from the pool.
	AddGroup(context.Context) Group
	// Start starts the pool. Cancellation of the provided context will cause
	// the pool to stop.
	Start(context.Context)
	// Stop stops the pool. Will have the same effect as cancelling the context
	// passed to Start.
	Stop()
}

// Group is a group of workers that can execute work.
type Group interface {
	// Enqueue queues a work function to be executed by the group. It does not
	// block and the WorkFunc should be assumed to execute in another goroutine.
	Enqueue(WorkFunc)
}

type pool struct {
	ctx             context.Context
	cancel          context.CancelFunc
	totalWorkers    int
	workersPerGroup int
	lk              sync.Mutex
	cond            *sync.Cond
	work            *list.List
}

type groupWork struct {
	f     WorkFunc
	group *group
}

type group struct {
	pool   *pool
	ctx    context.Context
	lk     sync.Mutex
	work   *list.List
	active int
}

// New creates a new GroupWorkPool with the given number of total workers and
// total workers per group.
func New(totalWorkers, workersPerGroup int) GroupWorkPool {
	pool := &pool{
		totalWorkers:    totalWorkers,
		workersPerGroup: workersPerGroup,
		work:            list.New(),
	}
	pool.cond = sync.NewCond(&pool.lk)
	return pool
}

func (p *pool) Start(ctx context.Context) {
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.lk.Lock()
	defer p.lk.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < p.totalWorkers; i++ {
		wg.Add(1)
		go func() {
			wg.Done()
			p.worker()
		}()
	}
	wg.Add(1)
	go func() {
		wg.Done()
		<-p.ctx.Done()
		p.lk.Lock()
		p.cond.Broadcast()
		p.lk.Unlock()
	}()
	wg.Wait()
}

func (p *pool) Stop() {
	p.cancel()
}

func (p *pool) worker() {
	p.lk.Lock()
	defer p.lk.Unlock()
	for p.ctx.Err() == nil {
		// pop work from the pool and execute it
		next := p.work.Front()
		if next != nil {
			p.work.Remove(next)
			p.lk.Unlock()
			gw := next.Value.(groupWork)
			// execute work
			gw.f()
			// notify group that work is done
			gw.group.workDone()
			p.lk.Lock()
			continue
		}
		if p.ctx.Err() == nil {
			p.cond.Wait()
		}
	}
}

func (p *pool) enqueue(gw groupWork) {
	p.lk.Lock()
	defer p.lk.Unlock()
	p.work.PushBack(gw)
	p.cond.Signal()
}

func (p *pool) AddGroup(ctx context.Context) Group {
	return &group{
		pool: p,
		ctx:  ctx,
		work: list.New(),
	}
}

func (g *group) Enqueue(f WorkFunc) {
	g.lk.Lock()
	defer g.lk.Unlock()
	g.work.PushBack(f)
	g.maybePromote()
}

func (g *group) workDone() {
	g.lk.Lock()
	defer g.lk.Unlock()
	g.active--
	g.maybePromote()
}

func (g *group) maybePromote() {
	// assume we have a lock
	if g.ctx.Err() != nil {
		// clear the queue
		g.work.Init()
		g.active = 0
		return
	}
	for g.active < g.pool.workersPerGroup {
		next := g.work.Front()
		if next == nil {
			return
		}
		g.work.Remove(next)
		g.active++
		g.pool.enqueue(groupWork{
			f:     next.Value.(WorkFunc),
			group: g,
		})
	}
}
