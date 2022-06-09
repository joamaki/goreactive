// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package stream

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"testing"
	"time"
)

func TestMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	double := func(x int) int { return x * 2 }

	// 1. mapping a non-empty source
	{
		src := Range(0, 5)
		src = Map(src, double)
		result, err := ToSlice(ctx, src)
		assertNil(t, "case 1", err)
		assertSlice(t, "case 1", []int{0, 2, 4, 6, 8}, result)
	}

	// 2. mapping an empty source
	{
		src := Map(Empty[int](), double)
		result, err := ToSlice(ctx, src)
		assertNil(t, "case 2", err)
		assertSlice(t, "case 2", []int{}, result)
	}

	// 3. cancelled context
	cancel()
	{
		src := Map(Range(0, 100), double)
		result, err := ToSlice(ctx, src)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("case 3: expected Canceled error, got %s", err)
		}
		assertSlice(t, "case 3", []int{}, result)
	}
}

func TestFlatMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	negated := func(x int) Observable[int] {
		return FromSlice([]int{x, -x})
	}

	// 1. mapping a non-empty source
	{
		src := Range(0, 3)
		src = FlatMap(src, negated)
		result, err := ToSlice(ctx, src)
		assertNil(t, "case 1", err)
		assertSlice(t, "case 1", []int{0, 0, 1, -1, 2, -2}, result)
	}

	// 2. mapping an empty source
	{
		src := FlatMap(Empty[int](), negated)
		result, err := ToSlice(ctx, src)
		assertNil(t, "case 2", err)
		assertSlice(t, "case 2", []int{}, result)
	}

	// 3. cancelled context
	cancel()
	{
		src := FlatMap(Range(0, 100), negated)
		result, err := ToSlice(ctx, src)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("case 3: expected Canceled error, got %s", err)
		}
		assertSlice(t, "case 3", []int{}, result)
	}
}

func TestParallelMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	double := func(x int) int { return x * 2 }

	// 1. mapping a non-empty source
	{
		src := Range(0, 1000)
		expected, _ := ToSlice(ctx, Map(src, double))

		src = ParallelMap(src, 5, double)
		result, err := ToSlice(ctx, src)
		assertNil(t, "case 1", err)
		sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
		assertSlice(t, "case 1", expected, result)
	}

	// 2. mapping an empty source
	{
		src := ParallelMap(Empty[int](), 5, double)
		result, err := ToSlice(ctx, src)
		assertNil(t, "case 2", err)
		assertSlice(t, "case 2", []int{}, result)
	}

	// 3. cancelled context
	cancel()
	{
		src := ParallelMap(Range(0, 100), 5, double)
		result, err := ToSlice(ctx, src)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("case 3: expected Canceled error, got %s", err)
		}
		assertSlice(t, "case 3", []int{}, result)
	}
}

func TestConcat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. successful case
	res1, err := ToSlice(ctx, Concat(Just(1), Just(2), Just(3)))
	if err != nil {
		t.Fatalf("case 1 errored: %s", err)
	}
	assertSlice(t, "case 1", res1, []int{1, 2, 3})

	// 2. test cancelled concat
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	res2, err := ToSlice(ctx2, Concat(Just(1), Stuck[int]()))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("case 2 expected Canceled error, got %s", err)
	}
	assertSlice(t, "case 2", []int{}, res2)

	// 3. test empty concat
	res3, err := ToSlice(ctx, Concat[int]())
	if err != nil {
		t.Fatalf("case 3 errored: %s", err)
	}
	assertSlice(t, "case 3", []int{}, res3)
}

func TestBroadcast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	numSubs := 3

	expected := []int{1, 2, 3, 4, 5}

	in := make(chan int)

	// Create an unbuffered broadcast of 'in'
	src := Broadcast(ctx, 0, FromChannel(in))

	subErrs := make(chan error, numSubs)
	defer close(subErrs)

	subReady := make(chan struct{}, numSubs)
	defer close(subReady)

	for i := 0; i < numSubs; i++ {
		go func() {
			items, errs := ToChannels(ctx, src)
			signaled := false
			index := 0
			for {
				select {
				case item := <-items:
					if item == 0 {
						if !signaled {
							subReady <- struct{}{}
							signaled = true
						}
						continue
					}
					if item != expected[index] {
						subErrs <- fmt.Errorf("%d != %d", item, expected[index])
						return
					}
					index++

				case err := <-errs:
					subErrs <- err
					return
				}
			}
		}()
	}

	// Synchronize with workers by feeding 0s until all workers have acked.
nextSub:
	for i := 0; i < numSubs; i++ {
		for {
			select {
			case in <- 0:
			case <-subReady:
				continue nextSub
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}

	// Feed in the actual test data.
	for _, i := range expected {
		in <- i
	}
	close(in)

	// Process errors from the subscribers
	for i := 0; i < numSubs; i++ {
		err := <-subErrs
		if err != nil {
			t.Fatalf("error: %s", err)
		}
	}
}

func TestMerge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	expected := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}

	in1 := FromSlice(expected[0:3])
	in2 := FromSlice(expected[3:6])
	in3 := FromSlice(expected[6:9])

	// 1. successful merge
	out1, err := ToSlice(ctx, Merge(in1, in2, in3))
	assertNil(t, "case 1", err)
	// items are observed in non-deterministic order, so we'll
	// need to sort first to verify.
	sort.Slice(out1, func(i, j int) bool { return out1[i] < out1[j] })
	assertSlice(t, "case 1", expected, out1)

	// 2. downstream error
	stopErr := errors.New("stop")
	err = Merge(in1, in2, in3).Observe(
		ctx,
		func(x int) error { return stopErr })
	if !errors.Is(err, stopErr) {
		t.Fatalf("expected downstream error %s, got %s", stopErr, err)
	}

	// 3. upstream error
	_, err = ToSlice(ctx, Merge(in1, Error[int](stopErr)))
	if !errors.Is(err, stopErr) {
		t.Fatalf("expected upstream error %s, got %s", stopErr, err)
	}

	// 4. cancelled context
	cancel()
	_, err = ToSlice(ctx, Merge(in1, in2, in3))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled error, got %s", err)
	}
}

func TestCoalesceByKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	values := make(chan int, 16)
	toKey := func(v int) int {
		return v
	}
	buffered := CoalesceByKey(FromChannel(values), toKey, 16)

	out, _ := ToChannels(ctx, buffered)

	// Feed in some duplicate values.
	values <- 1
	values <- 2
	values <- 3
	values <- 1
	values <- 1
	values <- 3

	expected := []int{1, 2, 3}
	for _, e := range expected {
		v := <-out
		if v != e {
			t.Fatalf("expected %d, got %d", e, v)
		}
	}
}

func TestRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		err1 = errors.New("err1")
		err2 = errors.New("err2")
	)

	emit, complete, obs := fromCallback[int](1)
	pred := func(err error) bool {
		return errors.Is(err, err1)
	}
	// Retry if error is 'err1', otherwise stop.
	obs = Retry(obs, pred)

	items, errs := ToChannels(ctx, obs)

	emit(1)
	if item := <-items; item != 1 {
		t.Fatalf("expected 1, got %d", item)
	}

	emit(2)
	if item := <-items; item != 2 {
		t.Fatalf("expected 2, got %d", item)
	}

	complete(err1) // this should be retried
	emit(3)
	if item := <-items; item != 3 {
		t.Fatalf("expected 3, got %d", item)
	}

	complete(err2) // this should stop the observing
	emit(4)        // ignored
	complete(nil)  // ignored

	if item, ok := <-items; ok {
		t.Fatalf("expected items channel to be closed, got item %d", item)
	}

	if err := <-errs; err != err2 {
		t.Fatalf("expected error %s, got %s", err2, err)
	}
}

func TestSplitHead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	headSrc, tailSrc := SplitHead(Range(0, 5))

	// Need to consume tail first as head blocks
	// until it is instantiated.
	tailItems, err := ToSlice(ctx, tailSrc)
	assertNil(t, "ToSlice(tail)", err)
	assertSlice(t, "tail", []int{1, 2, 3, 4}, tailItems)

	headItems, err := ToSlice(ctx, headSrc)
	assertNil(t, "ToSlice(head)", err)
	assertSlice(t, "head", []int{0}, headItems)
}

func TestBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	takeSlowly := func(src Observable[int]) []int {
		items := []int{}
		err := Take(5, src).Observe(ctx,
			func(item int) error {
				time.Sleep(5 * time.Millisecond)
				items = append(items, item)
				return nil
			})
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		return items
	}

	// Test with empty source
	ticksSlice := takeSlowly(Buffer(Empty[int](), 10, BackpressureBlock))
	assertSlice(t, "empty block", []int{}, ticksSlice)
	ticksSlice = takeSlowly(Buffer(Empty[int](), 10, BackpressureDrop))
	assertSlice(t, "empty drop", []int{}, ticksSlice)

	// Test with one item source
	ticksSlice = takeSlowly(Buffer(Just(1), 10, BackpressureBlock))
	assertSlice(t, "single block", []int{1}, ticksSlice)
	ticksSlice = takeSlowly(Buffer(Just(1), 10, BackpressureDrop))
	assertSlice(t, "single drop", []int{1}, ticksSlice)

	// Test blocking strategy
	ticks := Interval(time.Millisecond)
	ticks = Buffer(ticks, 10, BackpressureBlock)
	ticksSlice = takeSlowly(ticks)
	assertSlice(t, "interval block", []int{0, 1, 2, 3, 4}, ticksSlice)

	// Test dropping strategy
	ticks = Interval(time.Millisecond)
	ticks = Buffer(ticks, 1, BackpressureDrop)
	ticksSlice = takeSlowly(ticks)

	cumulativeDiff := 0
	prevItem := ticksSlice[0]
	for _, item := range ticksSlice[1:] {
		cumulativeDiff += item - prevItem
		prevItem = item
	}
	if cumulativeDiff <= len(ticksSlice) {
		t.Fatalf("interval drop: Items not dropped, cumulative diff %d, items: %v", cumulativeDiff, ticksSlice)
	}
}

func TestTakeSkip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	skippedSrc := Skip(10, Range(0, 20))
	skipped, err := ToSlice(ctx, skippedSrc)
	assertNil(t, "ToSlice", err)
	expected, err := ToSlice(ctx, Range(10, 20))
	assertNil(t, "ToSlice", err)
	assertSlice(t, "skip 10", expected, skipped)

	takenSrc := Take(5, skippedSrc)
	expected, err = ToSlice(ctx, Range(10, 15))
	assertNil(t, "ToSlice", err)

	taken1, err := ToSlice(ctx, takenSrc)
	assertNil(t, "ToSlice", err)
	assertSlice(t, "skip 10, take 5", expected, taken1)

	taken2, err := ToSlice(ctx, takenSrc)
	assertNil(t, "ToSlice", err)
	assertSlice(t, "skip 10, take 5", expected, taken2)
}

//
// Benchmarks
//

func BenchmarkMerge(b *testing.B) {
	ctx := context.Background()
	s := make([]int, b.N, b.N)
	b.ResetTimer()

	count := 0
	err := Merge(FromSlice(s)).Observe(
		ctx,
		func(item int) error {
			count++
			return nil
		})

	if err != nil {
		b.Fatal(err)
	}

	if count != b.N {
		b.Fatalf("expected %d items, got %d", b.N, count)
	}
}

func BenchmarkBroadcast(b *testing.B) {
	ctx := context.Background()
	s := make([]int, b.N, b.N)
	b.ResetTimer()

	count := 0
	err := Broadcast(ctx, 16, FromSlice(s)).Observe(
		ctx,
		func(item int) error {
			count++
			return nil
		})

	if err != nil {
		b.Fatal(err)
	}

	if count != b.N {
		b.Fatalf("expected %d items, got %d", b.N, count)
	}
}

func BenchmarkMap(b *testing.B) {
	ctx := context.Background()
	s := make([]int, b.N, b.N)
	b.ResetTimer()

	count := 0
	err :=
		Map(FromSlice(s), func(x int) int { return x * 2 }).
			Observe(
				ctx,
				func(item int) error {
					count++
					return nil
				})

	if err != nil {
		b.Fatal(err)
	}

	if count != b.N {
		b.Fatalf("expected %d items, got %d", b.N, count)
	}
}

func BenchmarkParallelMap(b *testing.B) {
	ctx := context.Background()
	s := make([]int, b.N, b.N)
	b.ResetTimer()

	count := 0
	err :=
		ParallelMap(FromSlice(s), runtime.NumCPU(),
			func(x int) int { return x * 2 }).
			Observe(
				ctx,
				func(item int) error {
					count++
					return nil
				})

	if err != nil {
		b.Fatal(err)
	}

	if count != b.N {
		b.Fatalf("expected %d items, got %d", b.N, count)
	}
}
