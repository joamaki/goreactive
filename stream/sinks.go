// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package stream

import (
	"context"
)

//
// Sinks: operators that run an observable and send the output somewhere.
//

// First returns the first item from 'src' observable and then cancels
// the subscription.
func First[T any](ctx context.Context, src Observable[T]) (item T, err error) {
	subCtx, cancel := context.WithCancel(ctx)
	taken := false
	err = src.Observe(subCtx,
		func(x T) error {
			if taken {
				return nil
			}
			item = x
			taken = true
			cancel()
			return nil
		})
	if taken {
		return item, nil
	}
	return
}

// Last returns the last item from 'src' observable.
func Last[T any](ctx context.Context, src Observable[T]) (item T, err error) {
	err = src.Observe(ctx,
		func(x T) error {
			item = x
			return nil
		})
	return item, err
}

// ToSlice converts an Observable into a slice.
func ToSlice[T any](ctx context.Context, src Observable[T]) (items []T, err error) {
	items = make([]T, 0)
	err = src.Observe(
		ctx,
		func(item T) error {
			items = append(items, item)
			return nil
		})
	return
}

// ToChannels converts an observable into an item channel and error channel.
// When the source closes both channels are closed and an error (which may be nil)
// is always sent to the error channel.
func ToChannels[T any](ctx context.Context, src Observable[T]) (<-chan T, <-chan error) {
	out := make(chan T, 1)
	errs := make(chan error, 1)
	go func() {
		errs <- src.Observe(
			ctx,
			func(item T) error {
				out <- item
				return nil
			})
		close(out)
		close(errs)
	}()
	return out, errs
}

// ToChannel converts an observable into an item of channels. Errors are delivered
// to the supplied error channel.
func ToChannel[T any](ctx context.Context, errs chan<- error, src Observable[T]) <-chan T {
	out := make(chan T, 1)
	go func() {
		errs <- src.Observe(
			ctx,
			func(item T) error {
				out <- item
				return nil
			})
		close(out)
	}()
	return out
}

// Discard discards all items from 'src' and returns an error if any.
func Discard[T any](ctx context.Context, src Observable[T]) error {
	return src.Observe(ctx, func(item T) error { return nil })
}
