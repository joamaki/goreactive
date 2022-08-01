.PHONY: all test test-race bench

all: test bench

test:
	(cd stream && go build ./... && go test ./...)
	(cd sources/k8s && go build ./... && go test ./...)
	(cd sources/http && go build ./... && go test ./...)

test-race:
	(cd stream && go test -race ./...)

bench:
	cd stream && go test ./... -bench .
