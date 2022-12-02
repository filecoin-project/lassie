package prioritywaitqueue_test

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/lassie/pkg/retriever/prioritywaitqueue"
)

type worker struct {
	id     int
	sleep  int
	work   int
	queue  prioritywaitqueue.PriorityWaitQueue[*worker]
	doneCb func(int)
}

func (w *worker) run() {
	time.Sleep(time.Duration(w.sleep) * time.Millisecond)
	done := w.queue.Wait(w)
	time.Sleep(time.Duration(w.work) * time.Millisecond)
	w.doneCb(w.id)
	done()
}

func runWorker(id, sleep, work int, queue prioritywaitqueue.PriorityWaitQueue[*worker], doneCb func(int)) {
	w := worker{id, sleep, work, queue, doneCb}
	go w.run()
}

var workerCmp prioritywaitqueue.ComparePriority[*worker] = func(a *worker, b *worker) bool {
	return a.id < b.id
}

func TestPriorityWaitQueue(t *testing.T) {
	tests := []struct {
		name     string
		sleeps   []int // how long to sleep before queueing
		works    []int // how long the work takes
		expected []int // the order we expect them to be executed in
	}{
		{
			// start one, block queue, then queue remaining at the same time and
			// expect them to all be run in proper order
			name:     "same start",
			sleeps:   []int{00, 10, 10, 10, 10, 10, 10, 10, 10, 10},
			works:    []int{50, 10, 10, 10, 10, 10, 10, 10, 10, 10},
			expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// same as previous but with messy arrival order
			name:     "same start, skewed",
			sleeps:   []int{00, 10, 20, 10, 20, 10, 20, 10, 20, 10},
			works:    []int{50, 10, 10, 10, 10, 10, 10, 10, 10, 10},
			expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// start one, block queue, then queue 4, run them, with the final one
			// blocking beyond when we add 5 more and expect them all to be run in
			// proper order
			name:     "batched start",
			sleeps:   []int{00, 20, 20, 20, 20, 100, 100, 100, 100, 100},
			works:    []int{50, 10, 10, 10, 50, 10, 10, 10, 10, 10},
			expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// same as previous but with different batches
			name:     "batched start reverse",
			sleeps:   []int{00, 100, 100, 100, 100, 100, 20, 20, 20, 20},
			works:    []int{50, 10, 10, 10, 10, 10, 10, 10, 10, 50},
			expected: []int{0, 6, 7, 8, 9, 1, 2, 3, 4, 5},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// setup
			queue := prioritywaitqueue.New(workerCmp)
			out := make([]int, 0)
			lk := sync.Mutex{}
			wg := sync.WaitGroup{}
			wg.Add(len(tc.sleeps))
			doneCb := func(id int) {
				lk.Lock()
				defer lk.Unlock()
				out = append(out, id)
				wg.Done()
			}

			// run
			for id := range tc.sleeps {
				runWorker(id, tc.sleeps[id], tc.works[id], queue, doneCb)
			}

			// verify
			wg.Wait()
			if len(out) != 10 {
				t.Errorf("recorded %d outputs, expected 10", len(out))
			}
			if !reflect.DeepEqual(out, tc.expected) {
				t.Errorf("did not get expected order of execution: %v <> %v", out, tc.expected)
			}
		})
	}
}
