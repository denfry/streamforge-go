package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRateLimiterRejectsRequestsAfterConfiguredWindowCount(t *testing.T) {
	middleware := NewRateLimiter(1, time.Minute)
	handler := middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		req.RemoteAddr = "192.0.2.10:1234"
		handler.ServeHTTP(rec, req)
		if i == 0 && rec.Code != http.StatusOK {
			t.Fatalf("first request status=%d", rec.Code)
		}
		if i == 1 && rec.Code != http.StatusTooManyRequests {
			t.Fatalf("second request status=%d", rec.Code)
		}
	}
}

func TestHealthEndpointReportsDependencyState(t *testing.T) {
	deps := testDependencies()
	deps.Health = fakeHealth{status: map[string]string{"postgres": "ok", "redis": "degraded"}}
	rec := httptest.NewRecorder()
	NewRouter(deps).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

type fakeHealth struct {
	status map[string]string
}

func (f fakeHealth) Check(context.Context) map[string]string { return f.status }
