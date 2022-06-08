// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package stream

import (
	"context"
	"time"
)

//
// Sources, e.g. operators that create new observables.
//

// Single creates an observable with a single item.
func Single[T any](item T) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			return next(item)
		})
}

// Stuck creates an observable that never emits anything and
// just waits for the context to be cancelled.
// Mainly meant for testing.
func Stuck[T any]() Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			<-ctx.Done()
			return ctx.Err()
		})
}

// Error creates an observable that fails immediately with given error.
func Error[T any](err error) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			return err
		})
}

// Empty creates an empty observable that completes immediately.
func Empty[T any]() Observable[T] {
	return Error[T](nil)
}

// Discard discards all items from 'src' and returns an error if any.
func Discard[T any](ctx context.Context, src Observable[T]) error {
	return src.Observe(ctx, func(item T) error { return nil })
}

// FromSlice converts a slice into an Observable.
func FromSlice[T any](items []T) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			var err error
			for _, item := range items {
				if ctx.Err() != nil {
					err = ctx.Err()
					break
				}
				err = next(item)
				if err != nil {
					break
				}
			}
			return err
		})
}

// FromChannel creates an observable from a channel. The channel is consumed
// by the first observer.
func FromChannel[T any](in <-chan T) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			// TODO: we're blocking on receive and only handling
			// context cancellation after it. Issue here is that
			// if we just do for+select we don't know if 'in' is
			// closed and should stop.

			for v := range in {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := next(v); err != nil {
					return err
				}
			}
			return nil
		})
}

// Interval emits an increasing counter value every 'interval' period.
func Interval(interval time.Duration) Observable[int] {
	return FuncObservable[int](
		func(ctx context.Context, next func(int) error) error {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			done := ctx.Done()
			for i := 0; ; i++ {
				select {
				case <-done:
					return ctx.Err()
				case <-ticker.C:
					if err := next(i); err != nil {
						return err
					}
				}
			}
		})
}

// Range creates an observable that emits integers in range from...to-1.
func Range(from, to int) Observable[int] {
	return FuncObservable[int](
		func(ctx context.Context, next func(int) error) error {
			for i := from; i < to; i++ {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := next(i); err != nil {
					return err
				}
			}
			return nil
		})
}
