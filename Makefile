.PHONY: all build test test-race test-bench vet fmt fmt-check proto proto-check chaos bench clean

BIN_DIR := bin
CMDS := scheduler worker submit status chaos bench

all: build

proto:
	@which protoc > /dev/null 2>&1 || (echo "protoc not found in PATH" && exit 1)
	protoc -I proto --go_out=proto --go_opt=paths=source_relative proto/distribuq.proto

proto-check: proto
	@git diff --exit-code proto/distribuq.pb.go || (echo "proto/distribuq.pb.go is out of date with distribuq.proto; run 'make proto'" && exit 1)

build:
	@mkdir -p $(BIN_DIR)
	@for cmd in $(CMDS); do \
		go build -o $(BIN_DIR)/$$cmd ./cmd/$$cmd; \
	done

test:
	go test ./... -count=1

test-task-logs:
	go test ./internal/scheduler/... -run=TaskLog -count=1

test-autoscaler:
	go test ./internal/autoscaler/... -count=1

test-race:
	go test ./... -race -count=1

test-bench:
	go test ./... -bench=. -run=^$ -benchtime=500ms

vet:
	go vet ./...

fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (echo "Unformatted files exist:" && gofmt -l . && exit 1)

chaos: build
	go run ./cmd/chaos -bin ./$(BIN_DIR)

bench: build
	go run ./cmd/bench -tasks 500 -concurrency 8

clean:
	rm -rf $(BIN_DIR)
