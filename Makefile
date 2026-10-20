.PHONY: build test vet fmt
build:
	go build -trimpath -o bin/shardkv ./cmd/shardkv
test:
	go test -race -count=1 ./...
vet:
	go vet ./...
fmt:
	gofmt -w cmd internal
