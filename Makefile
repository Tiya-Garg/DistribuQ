.PHONY: all build test test-race vet fmt fmt-check chaos clean

BIN_DIR := bin
CMDS := scheduler worker submit status chaos

all: build

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
