package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestSubmitPipelineFileMockServer(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/pipelines" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"id":"p1","status":"running"}`))
	}))
	defer ts.Close()

	tmpFile, err := os.CreateTemp("", "pipe-*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	_, _ = tmpFile.WriteString(`{"id":"p1","stages":[{"id":"s1","steps":[{"id":"st1","payload":"echo 1"}]}]}`)
	_ = tmpFile.Close()

	addr := ts.Listener.Addr().String()
	err = submitPipelineFile(addr, tmpFile.Name(), "mock-token", 2*time.Second)
	if err != nil {
		t.Fatalf("submitPipelineFile failed: %v", err)
	}
}
