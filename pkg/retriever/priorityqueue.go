package retriever

import (
	"container/heap"
)

type PriorityQueue[T any] struct {
	itemsCh chan sortableItems[T] // holds our heap items
	emptyCh chan bool             // true if the queue is empty
	cmp     func(a, b T) bool     // compare function
}

// Makes a new Queue
func NewPriorityQueue[T any](cmp func(a, b T) bool) PriorityQueue[T] {
	itemsCh := make(chan sortableItems[T], 1)
	emptyCh := make(chan bool, 1)
	emptyCh <- true
	return PriorityQueue[T]{itemsCh, emptyCh, cmp}
}

// Get will return the priority item and whether or not the queue was empty.
// If empty is true, the return item will be a nil pointer to T.
func (q *PriorityQueue[T]) Get() (T, bool) {
	var items sortableItems[T]

	select {
	case items = <-q.itemsCh: // grab the items
	case <-q.emptyCh:
		q.emptyCh <- true
		return *new(T), true
	}

	// safely grab last item
	item := heap.Pop(&items).(*T)
	if items.Len() == 0 {
		q.emptyCh <- true // mark queue as empty
	} else {
		q.itemsCh <- items // communicate remaining items back to the channel
	}

	return *item, false
}

// Put adds an item to the queue and piroritizes it.
func (q *PriorityQueue[T]) Put(item T) {
	items := sortableItems[T]{[]*T{}, q.cmp}

	// wait until...
	select {
	case items = <-q.itemsCh: // we can grab existing items
	case <-q.emptyCh: // or queue is empty
	}

	heap.Push(&items, item)
	q.itemsCh <- items
}

// Len returns the number of items in the queue
func (q *PriorityQueue[T]) Len() int { return len(q.itemsCh) }

// sortableItems implements sort.Interface
type sortableItems[T any] struct {
	data []*T
	cmp  func(a, b T) bool
}

func (t sortableItems[T]) Len() int { return len(t.data) }

func (t sortableItems[T]) Less(i, j int) bool {
	return t.cmp(*t.data[i], *t.data[j])
}

func (t sortableItems[T]) Swap(i, j int) {
	t.data[i], t.data[j] = t.data[j], t.data[i]
}

func (t *sortableItems[T]) Push(x any) {
	item := x.(T)
	t.data = append(t.data, &item)
}

func (t *sortableItems[T]) Pop() any {
	old := t
	n := len(old.data) - 1
	item := old.data[n]
	old.data[n] = nil
	t.data = old.data[:n]
	return item
}
