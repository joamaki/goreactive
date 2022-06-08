// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	httpClient "github.com/joamaki/goreactive/clients/http"
	"github.com/joamaki/goreactive/stream"
)

func fatal(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

func GetByLine(resp stream.Observable[*http.Response]) stream.Observable[string] {
	return stream.FlatMap(resp, func(resp *http.Response) stream.Observable[string] {
		return stream.FuncObservable[string](
			func(ctx context.Context, next func(string) error) error {
				defer resp.Body.Close()
				scanner := bufio.NewScanner(resp.Body)
				for scanner.Scan() {
					if err := next(scanner.Text()); err != nil {
						return err
					}
				}
				return scanner.Err()
			})
	})
}

func streamHandler(format string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			w.WriteHeader(500)
			fmt.Fprintf(w, "error: no http.Flusher\n")
			return
		}

		w.WriteHeader(200)
		for i := 0; ; i++ {
			_, err := fmt.Fprintf(w, format+"\n", i)
			if err != nil {
				break
			}
			flusher.Flush()
			time.Sleep(time.Millisecond * 50)
		}
	}
}

func startHTTPServer() (string, *http.Server) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		fatal("error from Listen: %s", err)
	}

	srv := &http.Server{Addr: "127.0.0.1:0"}
	http.HandleFunc("/hex", streamHandler("0x%x"))
	http.HandleFunc("/dec", streamHandler("%d"))
	http.HandleFunc("/oct", streamHandler("0%o"))

	go func() {
		srv.Serve(listener)
		listener.Close()
	}()
	return "http://" + listener.Addr().String(), srv
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
		GetByLine(httpClient.Get(url+"/hex")),
		GetByLine(httpClient.Get(url+"/oct")),
		GetByLine(httpClient.Get(url+"/dec")),

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
