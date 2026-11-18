package tracing

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// OTLPJSONExporter exports finished spans via OTLP/HTTP JSON format.
type OTLPJSONExporter struct {
	mu       sync.Mutex
	endpoint string
	client   *http.Client
	buffer   []SpanSnapshot
	batchLen int
}

// NewOTLPJSONExporter creates an exporter targeting an OTLP/HTTP endpoint (e.g. Jaeger or OTel Collector).
func NewOTLPJSONExporter(endpoint string, batchLen int) *OTLPJSONExporter {
	if batchLen <= 0 {
		batchLen = 10
	}
	return &OTLPJSONExporter{
		endpoint: endpoint,
		client:   &http.Client{Timeout: 5 * time.Second},
		batchLen: batchLen,
	}
}

// ExportSpan buffers a span snapshot and flushes when batch limit is reached.
func (e *OTLPJSONExporter) ExportSpan(snap SpanSnapshot) error {
	e.mu.Lock()
	e.buffer = append(e.buffer, snap)
	shouldFlush := len(e.buffer) >= e.batchLen
	var toSend []SpanSnapshot
	if shouldFlush {
		toSend = e.buffer
		e.buffer = nil
	}
	e.mu.Unlock()

	if shouldFlush {
		return e.sendBatch(toSend)
	}
	return nil
}

func (e *OTLPJSONExporter) sendBatch(spans []SpanSnapshot) error {
	payload, err := json.Marshal(map[string]any{
		"resourceSpans": []map[string]any{
			{
				"scopeSpans": []map[string]any{
					{
						"spans": spans,
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	resp, err := e.client.Post(e.endpoint, "application/json", io.NopCloser(fmt.NewReader(payload)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
