// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/joamaki/goreactive/stream"
)

func fatal(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

func main() {
	// Create a context in which to execute the requests.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start a local HTTP server to test against.
	url, srv := startHTTPServer()
	defer srv.Shutdown(ctx)

	// Merge /hex, /oct and /dec streams into one.
	lines := stream.Merge(
		HTTPGetByLine(url+"/hex"),
		HTTPGetByLine(url+"/oct"),
		HTTPGetByLine(url+"/dec"),

		// Also once a second insert a dividing line
		stream.Map(stream.Interval(time.Second), func(_ int) string { return "-------" }),
	)

	// On errors we want to retry unless our context was cancelled.
	lines = stream.Retry(lines, func(err error) bool {
		if ctx.Err() == nil {
			// Sleep one second between retries.
			time.Sleep(time.Second)
			return true
		}
		return false
	})

	// Start observing the 'lines' stream and print each line to stdout.
	err := lines.Observe(ctx,
		func(line string) error {
			_, err := fmt.Println(line)
			return err
		})

	if err != nil && ctx.Err() == nil {
		fatal("error: %s", err)
	}
}
