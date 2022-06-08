// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package http

import (
	"context"
	"io"
	"net/http"

	"github.com/joamaki/goreactive/stream"
)

type Option func(*http.Request)

func WithBasicAuth(username, password string) func(*http.Request) {
	return func(req *http.Request) {
		req.SetBasicAuth(username, password)
	}
}

func WithBody(body io.Reader) func(*http.Request) {
	return func(req *http.Request) {
		rc, ok := body.(io.ReadCloser)
		if !ok && body != nil {
			rc = io.NopCloser(body)
		}
		req.Body = rc
	}
}

func WithHeader(key, value string) func(*http.Request) {
	return func(req *http.Request) {
		req.Header.Add(key, value)
	}
}

func Get(url string, options ...Option) stream.Observable[*http.Response] {
	return stream.FuncObservable[*http.Response](
		func(ctx context.Context, next func(*http.Response) error) error {
			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				return err
			}
			for _, opt := range options {
				opt(req)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			return next(resp)
		})
}

func Post(url string, body io.Reader, options ...Option) stream.Observable[*http.Response] {
	return stream.FuncObservable[*http.Response](
		func(ctx context.Context, next func(*http.Response) error) error {
			req, err := http.NewRequestWithContext(ctx, "POST", url, body)
			if err != nil {
				return err
			}

			for _, opt := range options {
				opt(req)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			return next(resp)
		})
}

func ResponseBody(in stream.Observable[*http.Response]) stream.Observable[[]byte] {
	return stream.FlatMap(in, func(resp *http.Response) stream.Observable[[]byte] {
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return stream.Error[[]byte](err)
		}
		return stream.Single(body)
	})
}
