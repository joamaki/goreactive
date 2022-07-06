.PHONY: all test bench

all: test bench

test:
	(cd stream && go build ./... && go test ./...)
	(cd sources/k8s && go build ./... && go test ./...)
	(cd sources/http && go build ./... && go test ./...)

bench:
	cd stream && go test ./... -bench .
