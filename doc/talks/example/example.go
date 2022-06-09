package main

import (
	"context"
	"fmt"

	"github.com/joamaki/goreactive/stream"
)

type singleIntegerObservable int

func (num singleIntegerObservable) Observe(ctx context.Context, next func(int) error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return next(int(num))
}

func main() {
	var ten stream.Observable[int] = singleIntegerObservable(10)

	// The 'Map' operator takes a stream and a function and applies
	// the function to each element.
	twenty := stream.Map(
		ten,
		func(x int) int { return x * 2 },
	)

	twenty.Observe(context.Background(), func(x int) error {
		fmt.Printf("%d\n", x)
		return nil
	})
}
