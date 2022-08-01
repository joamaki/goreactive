// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Map applies a function onto an observable.
func Map[A, B any](src Observable[A], apply func(A) B) Observable[B] {
	return FuncObservable[B](
		func(ctx context.Context, next func(B) error) error {
			return src.Observe(
				ctx,
				func(a A) error { return next(apply(a)) })
		})
}

// FlatMap applies a function that returns an observable of Bs to the source observable of As.
// The observable from the function is flattened (hence FlatMap).
func FlatMap[A, B any](src Observable[A], apply func(A) Observable[B]) Observable[B] {
	return FuncObservable[B](
		func(ctx context.Context, next func(B) error) error {
			return src.Observe(
				ctx,
				func(a A) error {
					return apply(a).Observe(
						ctx,
						next)

				})
		})
}

// Flatten takes an observable of slices of T and returns an observable of T.
func Flatten[T any](src Observable[[]T]) Observable[T] {
	return FlatMap(
		src,
		func(items []T) Observable[T] {
			return FromSlice(items)
		})
}

// ParallelMap maps a function in parallel to the source. The errors from downstream
// are propagated asynchronously towards the source.
func ParallelMap[A, B any](src Observable[A], par int, apply func(A) B) Observable[B] {
	return FuncObservable[B](
		func(ctx context.Context, next func(B) error) error {
			in := make(chan A, par)
			out := make(chan B, par)

			// nextErrs is for propagating error from 'next' towards upstream.
			nextErrs := make(chan error, 1)
			defer close(nextErrs)

			// observeErrs is for propagating error from the observing worker
			// to this goroutine.
			observeErrs := make(chan error, 1)
			defer close(observeErrs)

			// Spawn 'par' workers to process each item.
			var wg sync.WaitGroup
			wg.Add(par)
			for n := 0; n < par; n++ {
				go func() {
					defer wg.Done()
					for v := range in {
						out <- apply(v)
					}
				}()
			}

			// Start feeding the workers from the 'src' stream.
			go func() {
				err := src.Observe(
					ctx,
					func(a A) error {
						select {
						case err := <-nextErrs:
							// Error from downstream, propagate it. This will
							// end up in 'observeErrs' and will be returned
							// from this Observe().
							return err
						case in <- a:
						}
						return nil
					})

				// Close the input channel towards workers and wait for
				// them to finish.
				close(in)
				wg.Wait()

				// Close the output channel to stop feeding downstream
				// and send the final error out.
				close(out)
				observeErrs <- err
			}()

			// Feed items downstream. Done here to both sequantially feed downstream
			// and to do it from the goroutine that called Observe() for better
			// stack traces.
			for item := range out {
				if err := next(item); err != nil {
					nextErrs <- err
					break
				}
			}

			// Drain items from out to avoid blocking workers if we stopped above
			// due to 'next' error.
			for range out {
			}

			return <-observeErrs
		})
}

// Filter keeps only the elements for which the filter function returns true.
func Filter[T any](src Observable[T], filter func(T) bool) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			return src.Observe(
				ctx,
				func(x T) error {
					if filter(x) {
						return next(x)
					}
					return nil
				})
		})
}

// Reduce takes an initial state, and a function 'reduce' that is called on each element
// along with a state and returns an observable with a single result state produced
// by the last call to 'reduce'.
func Reduce[T, Result any](src Observable[T], init Result, reduce func(Result, T) Result) Observable[Result] {
	result := init
	return FuncObservable[Result](
		func(ctx context.Context, next func(Result) error) error {
			err := src.Observe(
				ctx,
				func(x T) error {
					result = reduce(result, x)
					return nil
				})
			if err != nil {
				return err
			}
			next(result)
			return nil
		})
}

// Scan takes an initial state and a step function that is called on each element with the
// previous state and returns an observable of the states returned by the step function.
// E.g. Scan is like Reduce that emits the intermediate states.
func Scan[In, Out any](src Observable[In], init Out, step func(Out, In) Out) Observable[Out] {
	prev := init
	return FuncObservable[Out](
		func(ctx context.Context, next func(Out) error) error {
			return src.Observe(
				ctx,
				func(x In) error {
					prev = step(prev, x)
					return next(prev)
				})
		})
}

// Zip2 takes two observables and merges them into an observable of pairs
func Zip2[V1, V2 any](src1 Observable[V1], src2 Observable[V2]) Observable[Tuple2[V1,V2]] {
	return FuncObservable[Tuple2[V1, V2]](
		func(ctx context.Context, next func(Tuple2[V1,V2]) error) error {
			subCtx, cancel := context.WithCancel(ctx)

			errs := make(chan error, 2)
			defer close(errs)

			v1s := ToChannel(subCtx, errs, src1)
			v2s := ToChannel(subCtx, errs, src2)

			var errOut error
			for {
				v1, ok := <-v1s
				if !ok {
					break
				}
				v2, ok := <-v2s
				if !ok {
					break
				}

				if err := next(Tuple2[V1,V2]{V1: v1, V2: v2}); err != nil {
					errOut = err
					break
				}
			}

			cancel()

			// Drain
			for range v1s {}
			for range v2s {}

			if err := <-errs; err != nil && errOut == nil {
				errOut = err
			}
			if err := <-errs; err != nil && errOut == nil {
				errOut = err
			}

			// Only care about canceled if parent was canceled.
			if errors.Is(errOut, context.Canceled) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					return nil
				}
			}

			return errOut

		})
}

// Concat takes one or more observable of the same type and emits the items from each of
// them in order.
func Concat[T any](srcs ...Observable[T]) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			for _, src := range srcs {
				err := src.Observe(
					ctx,
					next)
				if err != nil {
					return err
				}
			}
			return nil
		})
}

type MulticastParams struct {
	// BufferSize is the number of items to buffer per observer before backpressure
	// towards the source.
	BufferSize int

	// EmitLatest if set will emit the latest seen item when neb observer
	// subscribes.
	EmitLatest bool
}

var DefaultMulticastParams = MulticastParams{16, false}

// Multicast creates a publish-subscribe observable that "multicasts" items
// from the 'src' observable to subscribers.
//
// Returns the wrapped observable and a function to connect observers to the
// source observable. Connect will block until source observable completes and
// returns the error if any from the source observable.
// 
// Observers can subscribe both before and after the source has been connected,
// but may miss events if subscribing after connect.
func Multicast[T any](params MulticastParams, src Observable[T]) (mcast Observable[T], connect func(context.Context) error) {
	var (
		mu           sync.Mutex
		subId        int
		subs         = make(map[int]chan T)
		observeError error
		latestValue  T
		haveLatest   bool
	)

	// Use a separate context for signalling to subscribers that the observer has finished.
	mcastCtx, cancel := context.WithCancel(context.Background())

	connect = func(ctx context.Context) error {
		err := src.Observe(
			ctx,
			func(item T) error {
				mu.Lock()
				if params.EmitLatest {
					latestValue = item
					haveLatest = true
				}
				for _, sub := range subs {
					sub <- item
				}
				mu.Unlock()
				return nil
			})

		mu.Lock()
		observeError = err
		mu.Unlock()
		cancel()
		return err
	}

	mcast = FuncObservable[T](
		func(subCtx context.Context, next func(T) error) error {
			// Create a channel for this subscriber and add it to the
			// map of subscribers.
			mu.Lock()
			thisId := subId
			subId++
			items := make(chan T, params.BufferSize)
			subs[thisId] = items

			if params.EmitLatest && haveLatest {
				next(latestValue)
			}

			mu.Unlock()

			// Start feeding downstream from the items channel. Stop
			// if either 'next' fails, or subscriber context or the
			// broadcast context is cancelled.
			var err error
			for err == nil {
				select {
				case <-mcastCtx.Done():
					// Broadcast context cancelled, so we know there's an error waiting.
					mu.Lock()
					err = observeError
					mu.Unlock()

					// The worker has finished, so we can now safely close and drain any
					// remaining items.
					close(items)
					for item := range items {
						if errNext := next(item); errNext != nil {
							err = errNext
							break
						}
					}
					return err

				case <-subCtx.Done():
					err = subCtx.Err()

				case item := <-items:
					err = next(item)
				}
			}

			// Drain all items to unblock worker until we acquire the lock.
			go func() {
				for range items {
				}
			}()

			// When we acquire a lock, we know we have exclusive access to 'items'
			// and can close it and remove the subscriber.
			mu.Lock()
			close(items)
			delete(subs, thisId)
			mu.Unlock()

			return err
		})

	return
}

// CoalesceByKey buffers updates from the input observable and keeps only the latest version of the
// value for the same key when the observer is slow in consuming the values.
func CoalesceByKey[K comparable, V any](src Observable[V], toKey func(V) K, bufferSize int) Observable[V] {
	return FuncObservable[V](
		func(ctx context.Context, next func(V) error) error {
			queue := newCoalescingQueue[K, V](bufferSize)
			errs := make(chan error, 1)
			go func() {
				errs <- src.Observe(
					ctx,
					func(value V) error {
						queue.Push(toKey(value), value)
						return nil
					})
				queue.Close()
			}()
			for {
				if _, v, ok := queue.Pop(); ok {
					next(v)
				} else {
					return <-errs
				}
			}
		})
}

type mergeNext[T any] struct {
	item T
	errs chan error
}

// Merge multiple observables into one. Error from any one of the sources will
// cancel and complete the stream. Error from downstream is propagated to the
// upstream that emitted the item.
//
// Beware: the observables are observed from goroutines spawned by Merge()
// and thus run concurrently, e.g. functions doFoo and doBar are called from
// different goroutines than Observe():
//   Merge(Map(foo, doFoo), Map(bar, doBar)).Observe(...)
func Merge[T any](srcs ...Observable[T]) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			mergeCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			items := make(chan mergeNext[T], len(srcs))

			errs := make(chan error, len(srcs))
			defer close(errs)

			// Use a wait group to wait for the forked goroutines to
			// exit before we return.
			var wg sync.WaitGroup
			wg.Add(len(srcs))

			// Fork goroutines to observe each source. We feed
			// the items to the 'items' channel in order to maintain
			// the invariant of calling 'next' from the goroutine calling
			// Observe().
			for _, src := range srcs {
				go func(src Observable[T]) {
					defer wg.Done()

					nextErrs := make(chan error, 1)
					defer close(nextErrs)

					errs <- src.Observe(
						mergeCtx,
						func(item T) error {
							items <- mergeNext[T]{item, nextErrs}
							return <-nextErrs
						})
				}(src)
			}

			// Fork a goroutine to handle errors.
			var finalError error
			go func() {
				srcsRunning := len(srcs)

				firstError := true
				for srcsRunning > 0 {
					select {
					case err := <-errs:
						if err != nil && firstError {
							// Remember the error and cancel the context
							// to stop other upstreams.
							finalError = err
							firstError = false
							cancel()
						}
						srcsRunning--
					}
				}

				wg.Wait()
				close(items)
			}()

			// Feed downstream until all sources are done.
			for req := range items {
				req.errs <- next(req.item)
			}

			return finalError
		})
}

// Throttle limits the rate at which items are emitted.
func Throttle[T any](src Observable[T], ratePerSecond float64, burst int) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			limiter := rate.NewLimiter(rate.Limit(ratePerSecond), burst)
			return src.Observe(
				ctx,
				func(item T) error {
					if err := limiter.Wait(ctx); err != nil {
						return err
					}
					return next(item)
				})
		})
}

// Delay shifts the items emitted from source by the given duration.
func Delay[T any](src Observable[T], duration time.Duration) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			first := true
			return src.Observe(
				ctx,
				func(item T) error {
					if first {
						time.Sleep(duration)
						first = false
					}
					return next(item)
				})

		})
}

type BackpressureStrategy string
const (
	// Items are dropped if buffer is full
	BackpressureDrop = BackpressureStrategy("drop")

	// Observing blocks until there is room in the buffer
	BackpressureBlock = BackpressureStrategy("block")
)


// Buffer buffers 'n' items with configurable backpressure strategy.
// Downstream errors are not propagated towards 'src'.
func Buffer[T any](src Observable[T], bufSize int, strategy BackpressureStrategy) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			bufCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			buf := make(chan T, bufSize)
			var send func(T) error
			if strategy == BackpressureBlock {
				send = func(item T) error {
					buf <- item
					return nil
				}
			} else if strategy == BackpressureDrop {
				send = func(item T) error {
					select {
					case buf <- item:
					default:
					}
					return nil
				}
			} else {
				return fmt.Errorf("Unknown backpressure strategy: %q", strategy)
			}

			errs := make(chan error, 1)

			// Fork a goroutine to push items to the buffer
			go func() {
				errs <- src.Observe(bufCtx, send)
				close(errs)
				close(buf)
			}()

			// Send items downstream from the buffer.
			var nextErr error
			for item := range buf {
				nextErr = next(item)
				if nextErr != nil {
					cancel()
					break
				}
			}

			// Wait for observing to stop.
			observeErr := <-errs

			if nextErr != nil {
				return nextErr
			}
			if observeErr != nil {
				return observeErr
			}
			return ctx.Err()
		})

}

// OnNext calls the supplied function on each emitted item.
func OnNext[T any](src Observable[T], f func(T)) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			return src.Observe(
				ctx,
				func(item T) error {
					f(item)
					return next(item)
				})
		})

}

// Take takes 'n' items from the source 'src'.
// The context given to source observable is cancelled if it emits
// more than 'n' items. If all 'n' items were emitted this cancelled
// error is ignored.
func Take[T any](n int, src Observable[T]) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			ctx, cancel := context.WithCancel(ctx)
			remaining := n
			err := src.Observe(ctx,
				func(item T) error {
					if remaining > 0 {
						if err := next(item); err != nil {
							return err
						}
						remaining--
					}
					if remaining == 0 {
						cancel()
					}
					return nil
				})

			// If all 'n' items were emitted, ignore the cancelled
			// error.
			if remaining == 0 && errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})
}

// TakeWhile takes items from the source until 'pred' returns false after which
// the observable is completed.
func TakeWhile[T any](pred func(T) bool, src Observable[T]) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			ctx, cancel := context.WithCancel(ctx)
			done := false
			err := src.Observe(ctx,
				func(item T) error {
					if !done && pred(item) {
						if err := next(item); err != nil {
							return err
						}
					} else {
						done = true
						cancel()
					}
					return nil
				})
			if done && errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})
}


// Skip skips the first 'n' items from the source.
func Skip[T any](n int, src Observable[T]) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			skip := n
			return src.Observe(ctx,
				func(item T) error {
					if skip > 0 {
						skip--
						return nil
					}
					return next(item)
				})
		})
}

// SplitHead splits the source 'src' into two: 'head' which receives the first item,
// and 'tail' that receives the rest. Errors from source are only handed to 'tail'.
func SplitHead[T any](src Observable[T]) (head Observable[T], tail Observable[T]) {
	headChan := make(chan T, 1)
	head = FromChannel(headChan)
	tail = FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			first := true
			err := src.Observe(
				ctx,
				func(item T) error {
					if first {
						headChan <- item
						close(headChan)
						first = false
						return nil
					}
					return next(item)
				})
			if first {
				// First element never arrived.
				close(headChan)
			}
			return err

		})
	return
}

//
// Retrying and error handling
// 

// RetryFunc decides whether the processing should be retried for the given error
type RetryFunc func(err error) bool

// Retry resubscribes to the observable if it completes with an error.
func Retry[T any](src Observable[T], shouldRetry RetryFunc) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			for {
				err := src.Observe(
					ctx,
					next)
				if !shouldRetry(err) {
					return err
				}
			}
		})
}

// RetryNext retries the call to 'next' if it returned an error.
func RetryNext[T any](src Observable[T], shouldRetry RetryFunc) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			retriedNext := func(item T) error {
				err := next(item)
				for err != nil && shouldRetry(err) {
					err = next(item)
				}
				return err
			}
			return src.Observe(ctx, retriedNext)
		})
}

// AlwaysRetry always asks for a retry regardless of the error.
func AlwaysRetry(err error) bool {
	return true
}

// BackoffRetry retries with an exponential backoff.
func BackoffRetry(shouldRetry RetryFunc, minBackoff, maxBackoff time.Duration) RetryFunc {
	backoff := minBackoff
	return func(err error) bool {
		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		return shouldRetry(err)
	}

}

// LimitRetries limits the number of retries with the given retry method.
// e.g. LimitRetries(BackoffRetry(time.Millisecond, time.Second), 5)
func LimitRetries(shouldRetry RetryFunc, numRetries int) RetryFunc {
	return func(err error) bool {
		if numRetries <= 0 {
			return false
		}
		numRetries--
		return shouldRetry(err)
	}
}
