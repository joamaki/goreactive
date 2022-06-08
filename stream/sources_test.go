// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package stream

import (
	"context"
	"errors"
	"testing"
)

func TestSingleFirstStuck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. successful Single with First
	res1, err := First(ctx, Single(1))
	assertNil(t, "case 1", err)
	if res1 != 1 {
		t.Fatalf("case 1: expected 1, got %d", res1)
	}

	// 2. First with Stuck
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	_, err = First(ctx2, Stuck[int]())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("case 2: expected Canceled error, got %s", err)
	}
}

func TestFromSliceToSlice(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	xs := []int{1, 2, 3, 4}

	// 1. non-empty FromSlice->ToSlice
	xs1, err := ToSlice(ctx, FromSlice(xs))
	assertNil(t, "case 1", err)
	assertSlice(t, "case 1", xs, xs1)

	// 2. empty FromSlice->ToSlice
	xs2, err := ToSlice(ctx, FromSlice([]int{}))
	assertNil(t, "case 2", err)
	assertSlice(t, "case 2", []int{}, xs2)

	// 3. nil FromSlice->ToSlice
	xs3, err := ToSlice(ctx, FromSlice[int](nil))
	assertNil(t, "case 3", err)
	assertSlice(t, "case 3", []int{}, xs3)

	// 4. cancelled context
	cancel()
	xs4, err := ToSlice(ctx, FromSlice(xs))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("case 4: expected canceled error, got %s", err)
	}
	if len(xs4) != 0 {
		t.Fatalf("case 4: expected empty slice, got %v", xs4)
	}
}

func TestFromChannelToChannels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. successful round-trip
	{
		in := make(chan int, 10)
		for i := 0; i < 10; i++ {
			in <- i
		}
		close(in)

		src := FromChannel(in)
		out, errs := ToChannels(ctx, src)

		for i := 0; i < 10; i++ {
			item := <-out
			if item != i {
				t.Fatalf("expected %d, got %d", item, i)
			}
		}

		if err := <-errs; err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}

	// 2. empty input channel
	{
		in := make(chan int)
		close(in)

		src := FromChannel(in)
		out, errs := ToChannels(ctx, src)

		if item, ok := <-out; ok {
			t.Fatalf("expected out to be closed, got %d", item)
		}
		if err := <-errs; err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}

	// 3. cancelled context
	cancel()
	{
		in := make(chan int, 10)
		for i := 0; i < 10; i++ {
			in <- i
		}
		close(in)

		src := FromChannel(in)
		out, errs := ToChannels(ctx, src)

		if err := <-errs; err != context.Canceled {
			t.Fatalf("expected Canceled error, got %s", err)
		}
		if item, ok := <-out; ok {
			t.Fatalf("expected out to be closed, got %d", item)
		}
	}
}

func TestRangeTake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. take 5 from source with 10 items.
	zeroToFour, err := ToSlice(ctx, Take(5, Range(0, 10)))
	assertNil(t, "case 1", err)
	assertSlice(t, "case 1", []int{0, 1, 2, 3, 4}, zeroToFour)

	// 2. take 0 from source with 10 items.
	empty, err := ToSlice(ctx, Take(0, Range(0, 10)))
	assertNil(t, "case 2", err)
	assertSlice(t, "case 2", []int{}, empty)

	// 3. make sure source is cancelled after 'n' items
	wasCancelled := false
	onesSrc := FuncObservable[int](
		func(ctx context.Context, next func(int) error) error {
			for {
				if ctx.Err() != nil {
					wasCancelled = true
					return ctx.Err()
				}
				next(1)
			}
		})
	ones, err := ToSlice(ctx, Take[int](5, onesSrc))
	assertNil(t, "case 3", err)
	assertSlice(t, "case 3", []int{1, 1, 1, 1, 1}, ones)
	if !wasCancelled {
		t.Fatalf("upstream source not cancelled")
	}
}

//
// Benchmarks
//

func BenchmarkFromSlice(b *testing.B) {
	ctx := context.Background()
	s := make([]int, b.N, b.N)
	b.ResetTimer()

	err := Discard(ctx, FromSlice(s))
	if err != nil {
		b.Fatal(err)
	}
}
