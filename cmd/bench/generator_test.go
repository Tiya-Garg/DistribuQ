package main

import (
	"strings"
	"testing"
)

func TestGeneratePayload(t *testing.T) {
	p := GeneratePayload(128)
	if !strings.HasPrefix(p, "echo ") {
		t.Fatalf("expected payload to start with echo, got %s", p)
	}
	if len(p) < 128 {
		t.Fatalf("expected payload length >= 128, got %d", len(p))
	}
}

func TestParseSchedulers(t *testing.T) {
	raw := " localhost:8000, localhost:8001 , , localhost:8002 "
	addrs := ParseSchedulers(raw)
	if len(addrs) != 3 {
		t.Fatalf("expected 3 addresses, got %d", len(addrs))
	}
	if addrs[0] != "localhost:8000" || addrs[1] != "localhost:8001" || addrs[2] != "localhost:8002" {
		t.Fatalf("unexpected parsed addresses: %v", addrs)
	}
}
