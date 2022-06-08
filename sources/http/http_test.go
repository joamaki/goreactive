// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/joamaki/goreactive/stream"
)

func startHTTPServer(t *testing.T) (string, *http.Server) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("error from Listen: %s", err)
	}

	srv := &http.Server{Addr: "127.0.0.1:0"}
	http.HandleFunc("/test", func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprintf(w, "method:%s header[foo]:%s\n", req.Method, req.Header.Get("foo"))
		defer req.Body.Close()
		b, err := io.ReadAll(req.Body)
		if err != nil {
			w.Write([]byte(err.Error()))
		} else {
			w.Write(b)
		}
	})
	go func() {
		srv.Serve(listener)
		listener.Close()
	}()
	return "http://" + listener.Addr().String() + "/test", srv
}

func TestHttp(t *testing.T) {
	// Create a context in which to execute the requests.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start a local HTTP server to test against.
	url, srv := startHTTPServer(t)
	defer srv.Shutdown(ctx)

	// Test GET with extra header
	getStream := ResponseBody(Get(url, WithHeader("foo", "bar")))
	respBody, err := stream.First(ctx, getStream)
	if err != nil {
		t.Fatalf("unexpected error from Get: %s", err)
	}
	if string(respBody) != "method:GET header[foo]:bar\n" {
		t.Fatalf("unexpected response: %s", respBody)
	}

	// Test POST
	body := bytes.NewBufferString("hello")
	postStream := ResponseBody(Post(url, body, WithHeader("foo", "baz")))
	respBody, err = stream.First(ctx, postStream)
	if err != nil {
		t.Fatalf("unexpected error from Post: %s", err)
	}
	if string(respBody) != "method:POST header[foo]:baz\nhello" {
		t.Fatalf("unexpected response: %s", respBody)
	}
}
