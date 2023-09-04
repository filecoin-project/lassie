package groupworkpool_test

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/retriever/bitswaphelpers/groupworkpool"
	"github.com/stretchr/testify/require"
)

type asyncTracker struct {
	lk             sync.Mutex
	delay          time.Duration
	worked         map[int]int
	groupActive    map[int]int
	maxGroupActive map[int]int
	active         int
	maxActive      int
}

func newAsyncTracker(delay time.Duration) *asyncTracker {
	return &asyncTracker{
		delay:          delay,
		worked:         make(map[int]int),
		groupActive:    make(map[int]int),
		maxGroupActive: make(map[int]int),
	}
}

func (a *asyncTracker) work(id int) {
	a.lk.Lock()
	a.worked[id]++
	a.groupActive[id]++
	if a.groupActive[id] > a.maxGroupActive[id] {
		a.maxGroupActive[id] = a.groupActive[id]
	}
	a.active++
	if a.active > a.maxActive {
		a.maxActive = a.active
	}
	a.lk.Unlock()
	time.Sleep(a.delay)
	a.lk.Lock()
	a.groupActive[id]--
	a.active--
	a.lk.Unlock()
}

func TestGroupWorkPool(t *testing.T) {
	scenarios := []struct {
		name            string
		groups          int
		workPerGroup    int
		workers         int
		workersPerGroup int
		workDelay       time.Duration
	}{
		{
			name:            "1 group, 1 worker",
			groups:          1,
			workPerGroup:    1,
			workers:         1,
			workersPerGroup: 1,
			workDelay:       0,
		},
		{
			name:            "1 group, 10 workers",
			groups:          1,
			workPerGroup:    1,
			workers:         10,
			workersPerGroup: 1,
			workDelay:       0,
		},
		{
			name:            "1 group, 10 workers, 10 work per group",
			groups:          1,
			workPerGroup:    10,
			workers:         10,
			workersPerGroup: 1,
			workDelay:       0,
		},
		{
			name:            "1 group, 10 workers, 100 work per group",
			groups:          1,
			workPerGroup:    100,
			workers:         10,
			workersPerGroup: 1,
			workDelay:       0,
		},
		{
			name:            "10 groups, 10 workers, 100 work per group",
			groups:          10,
			workPerGroup:    100,
			workers:         10,
			workersPerGroup: 1,
			workDelay:       0,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			pool := groupworkpool.New(scenario.workers, scenario.workersPerGroup)
			pool.Start(context.Background())
			defer pool.Stop()

			asyncTracker := newAsyncTracker(10 * time.Millisecond)

			var wg sync.WaitGroup
			for gid := 0; gid < scenario.groups; gid++ {
				wg.Add(scenario.workPerGroup + 1)
				go func(gid int) {
					time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond) // add a bit of jitter
					group := pool.AddGroup(context.Background())
					for i := 0; i < scenario.workPerGroup; i++ {
						time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond) // add a bit of jitter
						group.Enqueue(func() {
							asyncTracker.work(gid)
							wg.Done()
						})
					}
					wg.Done()
				}(gid)
			}

			wg.Wait()

			for gid := 0; gid <= 10; gid++ {
				require.True(t, asyncTracker.maxGroupActive[gid] <= scenario.workersPerGroup)
				require.True(t, asyncTracker.maxActive <= scenario.workers)
			}

			t.Log("max group active:", asyncTracker.maxGroupActive)
			t.Log("max total active:", asyncTracker.maxActive)
		})
	}
}

func TestGroupCancellation(t *testing.T) {
	pool := groupworkpool.New(2, 1)
	pool.Start(context.Background())
	defer pool.Stop()
	asyncTracker := newAsyncTracker(10 * time.Millisecond)

	// 2 groups, 10 work per group, second group is cancelled after first
	// work item so we should see 10 work items for first group and 1 for
	// second group

	var wg sync.WaitGroup
	wg.Add(2)  // one per group setup
	wg.Add(10) // work for first group
	wg.Add(1)  // work for second group
	for gid := 0; gid < 2; gid++ {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func(gid int, ctx context.Context, cancel context.CancelFunc) {
			group := pool.AddGroup(ctx)
			for i := 0; i < 10; i++ {
				group.Enqueue(func() {
					asyncTracker.work(gid)
					if gid == 1 {
						cancel()
					}
					wg.Done()
				})
			}
			wg.Done()
		}(gid, ctx, cancel)
	}

	wg.Wait()

	require.Equal(t, 10, asyncTracker.worked[0])
	require.Equal(t, 1, asyncTracker.worked[1])
}
