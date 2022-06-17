// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package stream

import (
	"context"
	"testing"
)

//
// Test helpers
//

func assertSlice[T comparable](t *testing.T, what string, expected []T, actual []T) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Fatalf("assertSlice[%s]: expected %d items, got %d (%v)", what, len(expected), len(actual), actual)
	}
	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("assertSlice[%s]: at index %d, expected %v, got %v", what, i, expected[i], actual[i])
		}
	}
}

func assertNil(t *testing.T, what string, err error) {
	t.Helper()
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
