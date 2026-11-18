package tracing

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestOTLPJSONExporterBatching(t *testing.T) {
	var receivedBatches atomic.Int32
	var receivedSpans atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBatches.Add(1)
		b, _ := io.ReadAll(r.Body)
		var payload struct {
			ResourceSpans []struct {
				ScopeSpans []struct {
					Spans []SpanSnapshot `json:"spans"`
				} `json:"scopeSpans"`
			} `json:"resourceSpans"`
		}
		_ = json.Unmarshal(b, &payload)
		if len(payload.ResourceSpans) > 0 && len(payload.ResourceSpans[0].ScopeSpans) > 0 {
			receivedSpans.Add(int32(len(payload.ResourceSpans[0].ScopeSpans[0].Spans)))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp := NewOTLPJSONExporter(srv.URL, 2)
	snap1 := SpanSnapshot{Name: "span1", StartTime: time.Now(), EndTime: time.Now()}
	snap2 := SpanSnapshot{Name: "span2", StartTime: time.Now(), EndTime: time.Now()}

	_ = exp.ExportSpan(snap1)
	if receivedBatches.Load() != 0 {
		t.Fatalf("expected no batch before limit")
	}

	_ = exp.ExportSpan(snap2)
	if receivedBatches.Load() != 1 {
		t.Fatalf("expected 1 batch sent after limit, got %d", receivedBatches.Load())
	}
	if receivedSpans.Load() != 2 {
		t.Fatalf("expected 2 spans in batch, got %d", receivedSpans.Load())
	}
}
