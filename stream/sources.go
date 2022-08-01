// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package stream

import (
	"context"
	"fmt"
	"sync"
	"time"
)

//
// Sources, e.g. operators that create new observables.
//

// Just creates an observable with a single item.
func Just[T any](item T) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
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

// FromSlice converts a slice into an Observable.
func FromSlice[T any](items []T) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			for _, item := range items {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := next(item); err != nil {
					return err
				}
			}
			return nil
		})
}

// FromAnySlice converts a slice of 'any' into an Observable of specified type.
func FromAnySlice[T any](items []any) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			for _, anyItem := range items {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				item, ok := anyItem.(T)
				if !ok {
					var target T
					return fmt.Errorf("FromAnySlice[%T]: %T not castable to target type", target, anyItem)
				}
				if err := next(item); err != nil {
					return err
				}
			}
			return nil
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

func FromFunction[T any](f func() T) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return next(f())
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

// Deferred creates an observable that allows subscribing, but
// waits for the real observable to be provided later.
func Deferred[T any]() (src Observable[T], start func(Observable[T])) {
	var (
		mu sync.Mutex
		cond = sync.NewCond(&mu)
		realSrc Observable[T]
	)

	src = FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			mu.Lock()
			defer mu.Unlock()
			for realSrc == nil { cond.Wait() }
			return realSrc.Observe(ctx, next)
		})

	start = func(src Observable[T]) {
		mu.Lock()
		defer mu.Unlock()
		realSrc = src
		cond.Signal()
	}

	return

}

type observableValue[T any] struct {
	mu sync.RWMutex
	value T
	updates chan<- T
	closed bool
}

func (ov *observableValue[T]) Get() T {
	ov.mu.RLock()
	defer ov.mu.RUnlock()
	return ov.value
}

func (ov *observableValue[T]) Update(f func(*T)) {
	ov.mu.Lock()
	defer ov.mu.Unlock()
	if ov.closed { panic("ObservableValue is closed") }
	f(&ov.value)
	ov.updates <- ov.value
}

func (ov *observableValue[T]) Close() {
	ov.mu.Lock()
	defer ov.mu.Unlock()
	ov.closed = true
	close(ov.updates)
}

type ObservableValue[T any] interface {
	// Get retrieves the latest value
	Get() T

	// Update updates the value with a function that modifies
	// it. Observers of the value are notified of the new value.
	// Panics if called after Close().
	Update(f func(*T))

	// Close the value. Any observers to this value are completed.
	Close()
}

func NewObservableValue[T any](ctx context.Context, init T) (ObservableValue[T], Observable[T], error) {
	updates := make(chan T)

	// Wrap the updates channel into a multicast source that emits
	// the last seen value when subscribing.
	src, connect := Multicast(
		MulticastParams{BufferSize: 16, EmitLatest: true},
		FromChannel(updates))
	go connect(ctx)
	ov := &observableValue[T]{value: init, updates: updates}
	return ov, src, nil
}

