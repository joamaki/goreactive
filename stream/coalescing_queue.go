// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package stream

import (
	"container/list"
	"sync"
)

// coalescingQueue combines a queue and a map to implement a queue
// that only keeps the latest value (by key) pushed to it.
type coalescingQueue[K comparable, V any] struct {
	sync.Mutex

	// fullCond is used to wait for slots to free up when pushing items
	fullCond *sync.Cond

	// nonEmptyCond is used to wait for items when popping
	nonEmptyCond *sync.Cond

	bufSize int
	values  map[K]V
	queue   *list.List
	closed  bool
}

func newCoalescingQueue[K comparable, V any](bufSize int) *coalescingQueue[K, V] {
	q := &coalescingQueue[K, V]{
		bufSize: bufSize,
		values:  make(map[K]V),
		queue:   list.New(),
	}
	q.fullCond = sync.NewCond(q)
	q.nonEmptyCond = sync.NewCond(q)
	return q
}

func (q *coalescingQueue[K, V]) Close() {
	q.Lock()
	q.closed = true
	q.nonEmptyCond.Signal()
	q.fullCond.Signal()
	q.Unlock()
}

func (q *coalescingQueue[K, V]) Push(k K, v V) {
	q.Lock()
	defer q.Unlock()

	if q.closed { return }

	if _, ok := q.values[k]; !ok {
		// The key has not been seen yet, push it to the
		// queue.
		for !q.closed && len(q.values) >= q.bufSize {
			q.fullCond.Wait()
		}
		if q.closed { return }
		q.queue.PushBack(k)
		q.values[k] = v
		q.nonEmptyCond.Signal()
	} else {
		// The key already exists, just update the value.
		q.values[k] = v
	}
}

func (q *coalescingQueue[K, V]) Pop() (key K, item V, ok bool) {
	q.Lock()
	defer q.Unlock()

	// If the queue is empty, wait until an item is pushed.
	for !q.closed && q.queue.Front() == nil {
		q.nonEmptyCond.Wait()
	}

	// If the queue is empty and closed we signal to consumer
	// to stop.
	if q.closed && q.queue.Front() == nil {
		ok = false
		return
	}

	ok = true
	key = q.queue.Remove(q.queue.Front()).(K)
	item = q.values[key]
	delete(q.values, key)

	q.fullCond.Signal()

	return
}
