.PHONY: all build test test-race vet fmt fmt-check proto proto-check chaos clean

BIN_DIR := bin
CMDS := scheduler worker submit status chaos

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

test-race:
	go test ./... -race -count=1

vet:
	go vet ./...

fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (echo "Unformatted files exist:" && gofmt -l . && exit 1)

chaos: build
	go run ./cmd/chaos -bin ./$(BIN_DIR)

clean:
	rm -rf $(BIN_DIR)
