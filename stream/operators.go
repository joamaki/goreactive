// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package stream

import (
	"context"
	"errors"
	"sync"
	"time"
)

// First returns the first item from 'src' observable and then closes it.
func First[T any](ctx context.Context, src Observable[T]) (item T, err error) {
	subCtx, cancel := context.WithCancel(ctx)
	err = src.Observe(subCtx,
		func(x T) error {
			item = x
			cancel()
			return nil
		})
	return
}

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
// along with a state and returns an observable with a single result state.
func Reduce[T, Result any](src Observable[T], init Result, reduce func(T, Result) Result) Observable[Result] {
	result := init
	return FuncObservable[Result](
		func(ctx context.Context, next func(Result) error) error {
			err := src.Observe(
				ctx,
				func(x T) error {
					result = reduce(x, result)
					return nil
				})
			if err != nil {
				return err
			}
			next(result)
			return nil
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

// Broadcast creates a publish-subscribe observable that broadcasts items
// from the 'src' observable to subscribers.
//
// It immediately and only once observes the input and broadcasts the items
// to downstream observers. If 'ctx' is cancelled all current observers are
// completed, the input observable is cancelled.
//
// 'bufSize' is the number of items to buffer per observer before backpressure
// towards the source.
func Broadcast[T any](ctx context.Context, bufSize int, src Observable[T]) Observable[T] {
	var (
		mu           sync.RWMutex
		subId        int
		subs         = make(map[int]chan T)
		observeError error
	)

	// Use a separate context for signalling to subscribers that the observer has finished.
	bcastCtx, cancel := context.WithCancel(context.Background())

	// Spawn a worker that subscribes to the source and broadcasts the
	// items to all subscribers.
	go func() {
		err := src.Observe(
			ctx,
			func(item T) error {
				mu.RLock()
				for _, sub := range subs {
					sub <- item
				}
				mu.RUnlock()
				return nil
			})

		mu.Lock()
		observeError = err
		mu.Unlock()
		cancel()
	}()

	return FuncObservable[T](
		func(subCtx context.Context, next func(T) error) error {
			// Create a channel for this subscriber and add it to the
			// map of subscribers.
			mu.Lock()
			thisId := subId
			subId++
			items := make(chan T, bufSize)
			subs[thisId] = items
			mu.Unlock()

			// Start feeding downstream from the items channel. Stop
			// if either 'next' fails, or subscriber context or the
			// broadcast context is cancelled.
			var err error
			for err == nil {
				select {
				case <-bcastCtx.Done():
					// Broadcast context cancelled, so we know there's an error waiting.
					mu.RLock()
					err = observeError
					mu.RUnlock()

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

// Merge multiple observables into one. Error from one of the sources cancels
// context and completes the stream.
func Merge[T any](srcs ...Observable[T]) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			mergeCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			reqs := make(chan mergeNext[T], len(srcs))
			defer close(reqs)

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
					nextErrs := make(chan error, 1)
					errs <- src.Observe(
						mergeCtx,
						func(item T) error {
							reqs <- mergeNext[T]{item, nextErrs}
							return <-nextErrs
						})
					wg.Done()
				}(src)
			}

			// Feed downstream until either all sources have finished,
			// or a source encounters an error. Errors from downstream
			// are propagated upstream and come back down here through
			// 'errs'.
			var err error
			srcsRunning := len(srcs)
		loop:
			for srcsRunning > 0 {
				select {
				case err = <-errs:
					if err != nil {
						break loop
					}
					srcsRunning--
				case req := <-reqs:
					req.errs <- next(req.item)
				}
			}

			// Wait for all sources to terminate.
			cancel()
			wg.Wait()

			return err
		})
}

// Delay emits item from input at most once per given time interval.
func Delay[T any](src Observable[T], interval time.Duration) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			return src.Observe(
				ctx,
				func(item T) error {
					select {
					case <-ctx.Done():
					case <-ticker.C:
					}
					return next(item)
				})
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

// Retry resubscribes to the observable if it completes with an error.
func Retry[T any](src Observable[T], shouldRetry func(err error) bool) Observable[T] {
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

// Take takes 'n' items from the source 'src'.
// The context given to source observable is cancelled if it emits
// more than 'n' items. If all 'n' items were emitted this cancelled
// error is ignored.
func Take[T any](n int, src Observable[T]) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			ctx, cancel := context.WithCancel(ctx)
			err := src.Observe(ctx,
				func(item T) error {
					if n > 0 {
						next(item)
						n--
					} else {
						cancel()
					}
					return nil
				})

			// If all 'n' items were emitted, ignore the cancelled
			// error.
			if n == 0 && errors.Is(err, context.Canceled) {
				return nil
			}
			return err
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