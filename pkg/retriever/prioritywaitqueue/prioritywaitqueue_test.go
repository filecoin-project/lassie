package prioritywaitqueue_test

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/retriever/prioritywaitqueue"
)

type worker struct {
	wg     *sync.WaitGroup
	id     int
	sleep  int
	work   int
	queue  prioritywaitqueue.PriorityWaitQueue[*worker]
	doneCb func(int)
}

func (w *worker) run() {
	w.wg.Done()
	w.wg.Wait()
	time.Sleep(time.Duration(w.sleep) * time.Millisecond)
	done := w.queue.Wait(w)
	time.Sleep(time.Duration(w.work) * time.Millisecond)
	w.doneCb(w.id)
	done()
}

func runWorker(wg *sync.WaitGroup, id, sleep, work int, queue prioritywaitqueue.PriorityWaitQueue[*worker], doneCb func(int)) {
	w := worker{wg, id, sleep, work, queue, doneCb}
	go w.run()
}

var workerCmp prioritywaitqueue.ComparePriority[*worker] = func(a *worker, b *worker) bool {
	return a.id < b.id
}

func TestPriorityWaitQueue(t *testing.T) {
	tests := []struct {
		name         string
		sleeps       []int // how long to sleep before queueing
		works        []int // how long the work takes
		expected     []int // the order we expect them to be executed in
		initialPause time.Duration
	}{
		{
			// start one, block queue, then queue remaining at the same time and
			// expect them to all be run in proper order
			name:     "same start",
			sleeps:   []int{00, 100, 100, 100, 100, 100, 100, 100, 100, 100},
			works:    []int{500, 100, 100, 100, 100, 100, 100, 100, 100, 100},
			expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// similar to above, but we pause to allow all the workers to queue
			name:         "same start, initial pause",
			sleeps:       []int{100, 100, 100, 100, 100, 100, 100, 100, 100, 100},
			works:        []int{100, 100, 100, 100, 100, 100, 100, 100, 100, 100},
			expected:     []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			initialPause: 10 * time.Millisecond,
		},
		{
			// same as previous but with messy arrival order
			name:     "same start, skewed",
			sleeps:   []int{00, 100, 200, 100, 200, 100, 200, 100, 200, 100},
			works:    []int{500, 100, 100, 100, 100, 100, 100, 100, 100, 100},
			expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// same as previous but without the initial straight-through #1
			name:         "same start, skewed, initial pause",
			sleeps:       []int{100, 100, 200, 100, 200, 100, 200, 100, 200, 100},
			works:        []int{100, 100, 100, 100, 100, 100, 100, 100, 100, 100},
			expected:     []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			initialPause: 10 * time.Millisecond,
		},
		{
			// start one, block queue, then queue 4, run them, with the final one
			// blocking beyond when we add 5 more and expect them all to be run in
			// proper order
			name:     "batched start",
			sleeps:   []int{00, 200, 200, 200, 200, 500, 500, 500, 500, 500},
			works:    []int{250, 50, 50, 50, 500, 50, 50, 50, 50, 50},
			expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// similar to above, but without the initial straight-through #1
			name:         "batched start, initial pause",
			sleeps:       []int{200, 200, 200, 200, 200, 500, 500, 500, 500, 500},
			works:        []int{50, 50, 50, 50, 500, 50, 50, 50, 50, 50},
			expected:     []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			initialPause: 10 * time.Millisecond,
		},
		{
			// same as previous but with different batches
			name:     "batched start reverse",
			sleeps:   []int{00, 500, 500, 500, 500, 500, 200, 200, 200, 200},
			works:    []int{250, 50, 50, 50, 50, 50, 50, 50, 50, 500},
			expected: []int{0, 6, 7, 8, 9, 1, 2, 3, 4, 5},
		},
		{
			// same as previous but without the initial straight-through #1
			name:         "batched start reverse, initial pause",
			sleeps:       []int{200, 500, 500, 500, 500, 500, 200, 200, 200, 200},
			works:        []int{50, 50, 50, 50, 50, 50, 50, 50, 50, 500},
			expected:     []int{0, 6, 7, 8, 9, 1, 2, 3, 4, 5},
			initialPause: 10 * time.Millisecond,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// setup
			opts := make([]prioritywaitqueue.Option[*worker], 0)
			if tc.initialPause > 0 {
				opts = append(opts, prioritywaitqueue.WithInitialPause[*worker](tc.initialPause))
			}
			queue := prioritywaitqueue.New(workerCmp, opts...)
			out := make([]int, 0)
			lk := sync.Mutex{}
			doneWg := sync.WaitGroup{}
			doneWg.Add(len(tc.sleeps))
			doneCb := func(id int) {
				lk.Lock()
				defer lk.Unlock()
				out = append(out, id)
				doneWg.Done()
			}

			if queue.InitialPauseDone(false) == (tc.initialPause > 0) {
				t.Errorf("initial pause done: %v, expected %v", queue.InitialPauseDone(false), !(tc.initialPause > 0))
			}

			// run
			startWg := sync.WaitGroup{}
			startWg.Add(len(tc.sleeps))
			for id := range tc.sleeps {
				runWorker(&startWg, id, tc.sleeps[id], tc.works[id], queue, doneCb)
			}

			// verify
			doneWg.Wait()
			if len(out) != 10 {
				t.Errorf("recorded %d outputs, expected 10", len(out))
			}
			if !reflect.DeepEqual(out, tc.expected) {
				t.Errorf("did not get expected order of execution: %v <> %v", out, tc.expected)
			}
			if !queue.InitialPauseDone(false) {
				t.Errorf("initial pause should be done")
			}

			// InitialPauseDone should return immediately when asked to block
			doneCh := make(chan struct{})
			go func() {
				select {
				case <-doneCh:
				case <-time.After(1 * time.Millisecond):
					t.Errorf("InitialPauseDone should have returned immediately")
				}
			}()
			queue.InitialPauseDone(true)
			close(doneCh)
		})
	}
}
