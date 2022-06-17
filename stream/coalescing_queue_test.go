// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package stream

import (
	"sync"
	"testing"
	"time"
)

func TestCoalesingQueue(t *testing.T) {
	var wg sync.WaitGroup
	q := newCoalescingQueue[int, string](3)

	q.Push(1, "one")
	q.Push(2, "two")
	q.Push(1, "oneone")
	q.Push(3, "three")

	// Queue now full, push in the background
	wg.Add(1)
	fourBlocking := true
	go func() {
		q.Push(4, "four")
		fourBlocking = false
		wg.Done()
	}()
	// Check that last push is (likely) blocking
	time.Sleep(time.Millisecond)
	if !fourBlocking {
		t.Fatalf("expected Push(4) to block")
	}

	expectPop := func(ek int, ev string) {
		k, v, ok := q.Pop()
		if !ok {
			t.Fatalf("expected pop to succeed")
		}
		if k != ek {
			t.Fatalf("expected key %d, got %d", ek, k)
		}
		if v != ev {
			t.Fatalf("expected %q, but got %q", ev, v)
		}
	}
	expectPop(1, "oneone")

	// There should now be room in the queue, so wait
	// for the background push to finish.
	wg.Wait()

	expectPop(2, "two")
	expectPop(3, "three")

	q.Close()

	// We can still drain items after it's closed.
	expectPop(4, "four")

	// Pushing after it's closed should be no-op
	q.Push(1, "oneoneone")

	_, _, ok := q.Pop()
	if ok {
		t.Fatalf("expected q.Pop to return false after closing and draining")
	}
}
