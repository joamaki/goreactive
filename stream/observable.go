// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package stream

import (
	"context"
)

type Observable[T any] interface {
	// Observe starts observing a stream of T's.
	// 'next' is called on each element sequentially. If it returns an error the stream closes
	// and this error is returned by Observe().
	// When 'ctx' is cancelled the stream closes and ctx.Err() is returned.
	//
	// Implementations of Observe() must maintain the following invariants:
	// - Observe blocks until the stream and any upstreams are closed.
	// - 'next' is called sequentially from the goroutine that called Observe() in
	//   order to maintain good stack traces and not require observer to be thread-safe.
	// - if 'next' returns an error it must not be called again and the same error
	//   must be returned by Observe().
	//
	// Handling of context cancellation is asynchronous and implementation may
	// choose to block on 'next' and only handle cancellation after 'next' returns.
	// If 'next' implements a long-running operation, then it is expected that the caller
	// will handle 'ctx' cancellation in the 'next' function itself.
	Observe(ctx context.Context, next func(T) error) error
}

// FuncObservable wraps a function that implements Observe. Convenience when declaring
// a struct to implement Observe() is overkill.
type FuncObservable[T any] func(context.Context, func(T) error) error

func (f FuncObservable[T]) Observe(ctx context.Context, next func(T) error) error {
	return f(ctx, next)
}
