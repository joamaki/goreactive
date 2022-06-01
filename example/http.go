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

	. "github.com/joamaki/goreactive"
)

func HTTPGetByLine(url string) Observable[string] {
	return FuncObservable[string](
		func(ctx context.Context, next func(string) error) error {
			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				return err
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			scanner := bufio.NewScanner(resp.Body)
			for scanner.Scan() {
				if err := next(scanner.Text()); err != nil {
					return err
				}
			}
			return scanner.Err()
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

