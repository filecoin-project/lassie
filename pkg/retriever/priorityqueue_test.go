package retriever

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

type item struct {
	id  int
	put int // the number of seconds to sleep before being added to the queue
	get int // the number of seconds to sleep before being removed from the queue
}

var cmdFunc = func(a, b *item) bool { return a.id < b.id }

func TestQueue(t *testing.T) {
	tests := []struct {
		name     string
		puts     []int // how long to sleep before queueing
		gets     []int // how long to sleep before dequeueing
		expected []int // the order we expect the items to be in
	}{
		{
			// queue all at the same time and
			// expect them to all be run in proper order
			name:     "same start",
			puts:     []int{10, 10, 10, 10, 10, 10, 10, 10, 10, 10},
			gets:     []int{50, 10, 10, 10, 10, 10, 10, 10, 10, 10},
			expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// same as previous but with messy arrival order
			name:     "same start, skewed",
			puts:     []int{00, 10, 15, 10, 15, 10, 15, 10, 15, 10},
			gets:     []int{50, 10, 10, 10, 10, 10, 10, 10, 10, 10},
			expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// start one, block queue, then queue 4, run them, with the final one
			// blocking beyond when we add 5 more and expect them all to be run in
			// proper order
			name:     "batched start",
			puts:     []int{00, 20, 20, 20, 20, 150, 150, 150, 150, 150},
			gets:     []int{50, 10, 10, 10, 100, 10, 10, 10, 10, 10},
			expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// same as previous but with different batches
			name:     "batched start reverse",
			puts:     []int{00, 150, 150, 150, 150, 150, 20, 20, 20, 20},
			gets:     []int{50, 10, 10, 10, 10, 10, 10, 10, 10, 100},
			expected: []int{0, 6, 7, 8, 9, 1, 2, 3, 4, 5},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// setup
			queue := NewPriorityQueue(cmdFunc)
			out := make(chan int, len(tc.puts))
			wg := sync.WaitGroup{}
			wg.Add(len(tc.puts))

			items := make([]item, len(tc.puts))
			for i := range tc.puts {
				items[i] = item{i, tc.puts[i], tc.gets[i]}
			}

			for i := range tc.puts {
				go func(t *item, q *PriorityQueue[*item]) {
					defer wg.Done()

					time.Sleep(time.Duration(t.put) * time.Millisecond)
					q.Put(t)
					time.Sleep(time.Duration(t.get) * time.Millisecond)
					item, _ := q.Get()
					out <- item.id
				}(&items[i], &queue)
			}

			wg.Wait()
			close(out)

			var results []int
			for id := range out {
				results = append(results, id)
			}

			if len(results) != 10 {
				t.Errorf("recorded %d outputs, expected 10", len(out))
			}

			if !reflect.DeepEqual(results, tc.expected) {
				t.Errorf("did not get expected order of execution: %v <> %v", results, tc.expected)
			}
		})
	}
}

func BenchmarkEmptyQueuePut(b *testing.B) {
	queue := NewPriorityQueue(cmdFunc)

	for n := 0; n < b.N; n++ {
		queue.Put(&item{0, 10, 20})
	}
}

func BenchmarkNotEmptyQueuePut(b *testing.B) {
	queue := NewPriorityQueue(cmdFunc)
	queue.Put(&item{0, 10, 20})

	for n := 0; n < b.N; n++ {
		queue.Put(&item{0, 10, 20})
	}
}
