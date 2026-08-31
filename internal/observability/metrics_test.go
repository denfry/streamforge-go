package observability

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsExposeProcessingAndQueueValues(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	metrics.RecordProcessing("success")
	metrics.SetQueueDepth(3)

	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	if rec.Code != 200 || !strings.Contains(body, "streamforge_event_processing_total") || !strings.Contains(body, "streamforge_worker_queue_depth 3") {
		t.Fatalf("unexpected metrics response: status=%d body=%s", rec.Code, body)
	}
}
