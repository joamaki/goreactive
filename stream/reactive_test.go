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
	res1, err := ToSlice(ctx, Concat(Single(1), Single(2), Single(3)))
	if err != nil {
		t.Fatalf("case 1 errored: %s", err)
	}
	assertSlice(t, "case 1", res1, []int{1, 2, 3})

	// 2. test cancelled concat
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	res2, err := ToSlice(ctx2, Concat(Single(1), Stuck[int]()))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("case 2 expected Canceled error, got %s", err)
	}
	assertSlice(t, "case 2", []int{1}, res2)

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

//
// Test helpers
//

func assertSlice[T comparable](t *testing.T, what string, expected []T, actual []T) {
	if len(expected) != len(actual) {
		t.Fatalf("assertSlice[%s]: expected %d items, got %d", what, len(expected), len(actual))
	}
	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("assertSlice[%s]: at index %d, expected %v, got %v", what, i, expected[i], actual[i])
		}
	}
}

func assertNil(t *testing.T, what string, err error) {
	if err != nil {
		t.Fatalf("unexpected error in %s: %s", what, err)
	}
}

// fromCallback creates an observable that is fed by the returned 'emit' function.
// The 'stop' function is called when downstream cancels.
// Useful in tests, but unsafe in general as this creates an hot observable that only has
// sane behaviour with single observer.
func fromCallback[T any](bufSize int) (emit func(T), complete func(error), obs Observable[T]) {
	items := make(chan T, bufSize)
	errs := make(chan error, bufSize)

	emit = func(x T) {
		items <- x
	}

	complete = func(err error) {
		errs <- err
	}

	obs = FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case err := <-errs:
					return err
				case item := <-items:
					if err := next(item); err != nil {
						return err
					}
				}
			}
		})

	return
}
