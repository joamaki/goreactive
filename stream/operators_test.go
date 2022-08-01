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
	"sync/atomic"

)

func checkCancelled(t *testing.T, what string, src Observable[int]) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := ToSlice(ctx, src)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled error, got %s", err)
	}
	assertSlice(t, what, []int{}, result)
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
	checkCancelled(t, "case 3", Map(Range(0, 100), double))
}

func TestFilter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	isOdd := func(x int) bool { return x%2!=0 }

	// 1. filtering a non-empty source
	{
		src := Range(0, 5)
		src = Filter(src, isOdd)
		result, err := ToSlice(ctx, src)
		assertNil(t, "case 1", err)
		assertSlice(t, "case 1", []int{1,3}, result)
	}

	// 2. filtering an empty source
	{
		src := Filter(Empty[int](), isOdd)
		result, err := ToSlice(ctx, src)
		assertNil(t, "case 2", err)
		assertSlice(t, "case 2", []int{}, result)
	}

	// 3. cancelled context
	checkCancelled(t, "case 3", Filter(Range(0, 100), isOdd))
}

func TestReduce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sum := func(result, x int) int { return result + x }

	// 1. Reducing a non-empty source
	{
		src := Range(0,5)
		src = Reduce(src, 0, sum)
		result, err := ToSlice(ctx, src)
		assertNil(t, "case 1", err)
		assertSlice(t, "case 1", []int{0+0+1+2+3+4}, result)
	}

	// 2. Reducing an empty source
	{
		src := Reduce(Empty[int](), 0, sum)
		result, err := ToSlice(ctx, src)
		assertNil(t, "case 2", err)
		assertSlice(t, "case 2", []int{0}, result)
	}

	// 3. cancelled context
	checkCancelled(t, "case 3", Reduce(Range(0, 100), 0, sum))
}

func TestScan(t *testing.T) {
	src := Scan(Range(1,4), 1, func(x, y int) int {
		         return x * y
	})
	xs, err := ToSlice(context.TODO(), src)
	assertNil(t, "Scan", err)
	assertSlice(t, "scan",
	            []int{1*1, 1*1*2, 1*1*2*3},
	            xs)
}

func TestZip2(t *testing.T) {
	// 1. Non-empty sources
	src := Zip2(Range(0,5), Range(5, 10))
	xs, err := ToSlice(context.TODO(), src)
	assertNil(t, "case 1", err)
	assertSlice(t, "case 1",
	            []Tuple2[int,int]{
			{0,5},
			{1,6},
			{2,7},
			{3,8},
			{4,9},
	            },
		    xs)

        // 2. One shorter than the other
	src = Zip2(Range(0,5), Just(5))
	xs, err = ToSlice(context.TODO(), src)
	assertNil(t, "case 2", err)
	assertSlice(t, "case 2",
	            []Tuple2[int,int]{
			{0,5},
	            },
		    xs)

	// 3. One empty
	src = Zip2(Range(0,5), Empty[int]())
	xs, err = ToSlice(context.TODO(), src)
	assertNil(t, "case 3", err)
	assertSlice(t, "case 3",
	            []Tuple2[int,int]{},
		    xs)

	// 4. Cancelled context
	checkCancelled(t, "case 4",
		Map(Zip2(Range(0,5), Stuck[int]()), func(t Tuple2[int,int]) int { return t.V1 }),
	)
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
	checkCancelled(t, "case 3", FlatMap(Range(0, 100), negated))
}

func TestFlatten(t *testing.T) {
	src := FromSlice([][]int{{1,2}, {3,4}})
	result, err := ToSlice(context.TODO(), Flatten(src))
	assertNil(t, "ToSlice(Flatten)", err)
	assertSlice(t, "ToSlice(Flatten)", []int{1,2,3,4}, result)
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

	// 3. downstream error
	{
		var nope = errors.New("nope")
		src := ParallelMap(Range(1,100), 2, double)
		err := src.Observe(
			ctx,
			func(item int) error {
				return nope
			})
		if !errors.Is(err, nope) {
			t.Fatalf("expected error %s, got %s", nope, err)
		}
	}

	// 4. cancelled context
	checkCancelled(t, "case 4", ParallelMap(Range(0, 100), 5, double))
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
	checkCancelled(t, "case 2",Concat(Just(1), Stuck[int]()))

	// 3. test empty concat
	res3, err := ToSlice(ctx, Concat[int]())
	if err != nil {
		t.Fatalf("case 3 errored: %s", err)
	}
	assertSlice(t, "case 3", []int{}, res3)
}

func TestMulticast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	numSubs := 3

	expected := []int{1, 2, 3, 4, 5}

	in := make(chan int)

	// Create an unbuffered broadcast of 'in'
	src, connect := Multicast(MulticastParams{0, false}, FromChannel(in))

	subErrs := make(chan error, numSubs)
	defer close(subErrs)

	numReady := int32(0)

	for i := 0; i < numSubs; i++ {
		go func() {
			items, errs := ToChannels(ctx, src)
			index := 0
			ready := false
			for {
				select {
				case item := <-items:
					if item == 0 {
						if !ready {
							atomic.AddInt32(&numReady, 1)
							ready = true
						}
					} else {
						if item != expected[index] {
							subErrs <- fmt.Errorf("%d != %d", item, expected[index])
							return
						}
						index++
					}

				case err := <-errs:
					subErrs <- err
					return
				}
			}
		}()
	}

	connErrs := make(chan error)
	go func() { connErrs <- connect(ctx) }()

	// Synchronize with the subscriptions
	for atomic.LoadInt32(&numReady) != int32(numSubs) {
		in <- 0
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
			t.Errorf("error: %s", err)
		}
	}

	// Cancel the context and check that connect() terminates.
	cancel()

	err := <-connErrs
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("connect() error: %s", err)
	}
}

func TestMulticastCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	numSubs := 10

	src, connect := Multicast(MulticastParams{100, false}, Range(0, 10000))

	subErrs := make(chan error, numSubs)
	defer close(subErrs)

	for i := 0; i < numSubs; i++ {
		go func() {
			subErrs <- src.Observe(
				ctx,
				func(item int) error {
					time.Sleep(time.Millisecond)
					return nil
				})
		}()
	}

	connErrs := make(chan error)
	defer close(connErrs)
	go func() { connErrs <- connect(ctx) }()

	time.Sleep(10*time.Millisecond)
	cancel()

	// Process errors from the subscribers
	for i := 0; i < numSubs; i++ {
		err := <-subErrs
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("error: %s", err)
		}
	}

	err := <-connErrs
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("connect() error: %s", err)
	}
}

func TestMulticastEmitLatest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	expected := []int{0,1,2,3,4}
	lastItem := expected[len(expected)-1]

	src, connect := Multicast(MulticastParams{16, true}, Concat(FromSlice(expected), Stuck[int]()))
	go connect(ctx)

	// Subscribe first to wait for all items to be emitted
	src.Observe(ctx, func(item int) error {
		    if item == lastItem {
			    return errors.New("stop")
		    }
		    return nil
	})

	// Then subscribe again to check that the latest item is seen.
	x, err := First(ctx, src)
	assertNil(t, "First", err)
	if x != lastItem {
		t.Fatalf("expected to see %d, got %d", lastItem, lastItem)
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

	values := []int{1,2,3,2,3,4}

	toKey := func(v int) int {
		return v
	}

	// Coalesce with a buffer size of 3 (unique keys).
	buffered := CoalesceByKey(FromSlice(values), toKey, 3)

	out, err := ToSlice(ctx, Delay(buffered, time.Millisecond))
	assertNil(t, "CoalesceByKey", err)

	expected := []int{1, 2, 3, 4}
	assertSlice(t, "CoalesceByKey", expected, out)
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

func TestThrottle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	waitMillis := 20
	go func() {
		time.Sleep(time.Duration(waitMillis) * time.Millisecond)
		cancel()
	}()

	ratePerSecond := 2000.0
	values, err := ToSlice(ctx, Throttle(Range(0,100000), ratePerSecond, 1))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled error, got %s", err)
	}

	expectedLen := int(float64(waitMillis)/1000.0 * ratePerSecond )

	lenDiff := len(values) - expectedLen
	if lenDiff < 0 {
		lenDiff *= -1
	}
	// Check that we're within 20%
	if lenDiff > expectedLen/5 {
		t.Fatalf("expected ~%d values, got %d, diff: %d", expectedLen, len(values), lenDiff)
	}
}

func TestDelay(t *testing.T) {
	t0 := time.Now()
	delay := 10*time.Millisecond
	_, err := First(context.TODO(), Delay(Just(1), delay))
	t1 := time.Now()
	assertNil(t, "Delay", err)

	tdiff := t1.Sub(t0)
	if tdiff < delay || tdiff > 2*delay {
		t.Fatalf("expected delay of ~%s, got %s", delay, tdiff)
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

	taken3, err := ToSlice(ctx, Take(5, Concat(Range(0, 5), Stuck[int]())))
	assertNil(t, "ToSlice", err)
	assertSlice(t, "take 5 of 5", []int{0,1,2,3,4}, taken3)
}

func TestTakeWhile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pred := func(x int) bool { return x < 5 }

	xs, err := ToSlice(ctx, TakeWhile(pred, Range(0, 100)))
	assertNil(t, "ToSlice+TakeWhile+Range", err)
	assertSlice(t, "TakeWhile < 5", []int{0,1,2,3,4}, xs)

	xs, err = ToSlice(ctx, TakeWhile(pred, Empty[int]()))
	assertNil(t, "ToSlice+TakeWhile+Empty", err)
	assertSlice(t, "TakeWhile of Empty", []int{}, xs)

	cancel()
	xs, err = ToSlice(ctx, TakeWhile(pred, Stuck[int]()))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled, got %s", err)
	}
	assertSlice(t, "TakeWhile of cancelled Stuck", []int{}, xs)
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

func TestRetryNext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		err1 = errors.New("err1")
		err2 = errors.New("err2")
	)

	obs := Range(1, 100)

	isErr1 := func(err error) bool {
		return errors.Is(err, err1)
	}

	prev := 0
	err := RetryNext(obs, isErr1).Observe(
		ctx,
		func(i int) error {
			if i == 10 && prev != i {
				prev = i
				return err1
			}
			if i == 20 && prev != i {
				prev = i
				return err2
			}
			prev = i
			return nil
		})

	if !errors.Is(err, err2) {
		t.Fatalf("expected error %s, got %s", err2, err1)
	}

	if prev != 20 {
		t.Fatalf("expected last item processed to be 20, got %d", prev)
	}
}

func TestRetryFuncs(t *testing.T) {
	err := errors.New("err")

	// Retry 10 times with exponential backoff up to 10ms.
	var retry RetryFunc
	retry = AlwaysRetry
	retry = BackoffRetry(retry, time.Millisecond, 10*time.Millisecond)
	retry = LimitRetries(retry, 6)

	t0 := time.Now()
	for i := 0; i < 10; i++ {
		if i < 6 {
			if !retry(err) {
				t.Fatalf("expected retry to succeed at attempt %d", i)
			}
		} else {
			if retry(err) {
				t.Fatalf("expected retry to fail at attempt %d", i)
			}
		}
	}
	tdiff := time.Now().Sub(t0)
	expectedDiff := time.Duration(1 + 2 + 4 + 8 + 10 + 10) * time.Millisecond

	if tdiff < expectedDiff || tdiff > 2*expectedDiff {
		t.Fatalf("expected backoff duration to be ~%s, it was %s", expectedDiff, tdiff)
	}
}

func TestOnNext(t *testing.T) {
	sum := 0
	src := OnNext(Range(0,5), func(item int) {
		sum += item
	})
	_, err := ToSlice(context.TODO(), src)
	assertNil(t, "OnNext", err)
	if sum != 0+1+2+3+4 {
		t.Fatal("unexpected sum")
	}
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
	mcast, connect := Multicast(DefaultMulticastParams, FromSlice(s))

	go connect(ctx)
	err := mcast.Observe(
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
