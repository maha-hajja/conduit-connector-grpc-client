.PHONY: build test test-integration generate proto-generate generate-certs

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-grpc-client.version=${VERSION}'" -o conduit-connector-grpc-client cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

generate:
	go generate ./...

proto-generate:
	cd proto && buf generate

download:
	@echo Download go.mod dependencies
	@go mod download

install-tools: download
	@echo Installing tools from tools.go
	@go list -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy

generate-certs:
	sh test/generate-certs.sh
