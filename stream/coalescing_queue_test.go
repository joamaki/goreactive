// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package stream

import "testing"

func TestCoalesingQueue(t *testing.T) {
	q := newCoalescingQueue[int, string](16)

	q.Push(1, "one")
	q.Push(2, "two")
	q.Push(1, "oneone")
	q.Push(3, "three")

	k, v, ok := q.Pop()
	if !ok {
		t.Fatalf("expected pop to succeed")
	}
	if v != "oneone" {
		t.Fatalf("expected \"oneone\", but got %s", v)
	}
	if k != 1 {
		t.Fatalf("expected key 1, got %d", k)
	}

	k, v, ok = q.Pop()
	if !ok {
		t.Fatalf("expected pop to succeed")
	}
	if v != "two" {
		t.Fatalf("expected \"two\", but got %s", v)
	}
	if k != 2 {
		t.Fatalf("expected key 1, got %d", k)
	}

	q.Close()

	// We can still drain items after it's closed.
	k, v, ok = q.Pop()
	if !ok {
		t.Fatalf("expected pop to succeed")
	}
	if v != "three" {
		t.Fatalf("expected pop to return latest (three), but got %s", v)
	}
	if k != 3 {
		t.Fatalf("expected key 3, got %d", k)
	}

	// Pushing after it's closed should be no-op
	q.Push(1, "oneoneone")

	_, _, ok = q.Pop()
	if ok {
		t.Fatalf("expected q.Pop to return false after closing and draining")
	}
}
