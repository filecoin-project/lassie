package retriever

import (
	"container/heap"
	"context"
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

func (q *PriorityQueue[T]) Get(ctx context.Context) (T, error) {
	var items sortableItems[T]

	select {
	case items = <-q.itemsCh: // grab the items
	case <-ctx.Done():
		return *new(T), ctx.Err()
	}

	// safely grab last item
	item := heap.Pop(&items).(*T)
	if items.Len() == 0 {
		q.emptyCh <- true // mark queue as empty
	} else {
		q.itemsCh <- items // communicate remaining items back to the channel
	}

	return *item, nil
}

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
