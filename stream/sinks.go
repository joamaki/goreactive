// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package stream

import (
	"context"
)

//
// Sinks: operators that run an observable and send the output somewhere.
//

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
