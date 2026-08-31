package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry          prometheus.Registerer
	httpRequests      *prometheus.CounterVec
	httpDuration      *prometheus.HistogramVec
	publishResults    *prometheus.CounterVec
	processingResults *prometheus.CounterVec
	retries           prometheus.Counter
	dlqPublishes      prometheus.Counter
	queueDepth        prometheus.Gauge
	shutdownDrains    *prometheus.CounterVec
}

func NewMetrics(registry prometheus.Registerer) *Metrics {
	if registry == nil {
		registry = prometheus.DefaultRegisterer
	}
	metrics := &Metrics{
		registry:          registry,
		httpRequests:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "streamforge_http_requests_total", Help: "HTTP requests handled by StreamForge."}, []string{"method", "route", "status"}),
		httpDuration:      prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "streamforge_http_request_duration_seconds", Help: "HTTP request duration in seconds."}, []string{"method", "route"}),
		publishResults:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "streamforge_kafka_publish_total", Help: "Kafka event publish results."}, []string{"result"}),
		processingResults: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "streamforge_event_processing_total", Help: "Event processing results."}, []string{"result"}),
		retries:           prometheus.NewCounter(prometheus.CounterOpts{Name: "streamforge_event_retries_total", Help: "Event processing retries."}),
		dlqPublishes:      prometheus.NewCounter(prometheus.CounterOpts{Name: "streamforge_dlq_publishes_total", Help: "Dead-letter event publishes."}),
		queueDepth:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "streamforge_worker_queue_depth", Help: "Current bounded worker queue depth."}),
		shutdownDrains:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "streamforge_shutdown_drain_total", Help: "Shutdown drain outcomes."}, []string{"result"}),
	}
	registry.MustRegister(metrics.httpRequests, metrics.httpDuration, metrics.publishResults, metrics.processingResults, metrics.retries, metrics.dlqPublishes, metrics.queueDepth, metrics.shutdownDrains)
	return metrics
}

func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	if registry, ok := m.registry.(prometheus.Gatherer); ok {
		return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	}
	return promhttp.Handler()
}

func (m *Metrics) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		capture := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(capture, r)
		route := r.URL.Path
		if pattern := routePattern(r); pattern != "" {
			route = pattern
		}
		m.httpRequests.WithLabelValues(r.Method, route, strconv.Itoa(capture.status)).Inc()
		m.httpDuration.WithLabelValues(r.Method, route).Observe(time.Since(started).Seconds())
	})
}

func (m *Metrics) RecordPublish(result string) {
	m.publishResults.WithLabelValues(result).Inc()
}

func (m *Metrics) RecordProcessing(result string) {
	m.processingResults.WithLabelValues(result).Inc()
}

func (m *Metrics) RecordRetry() {
	m.retries.Inc()
}

func (m *Metrics) RecordDLQ() {
	m.dlqPublishes.Inc()
}

func (m *Metrics) SetQueueDepth(depth int) {
	m.queueDepth.Set(float64(depth))
}

func (m *Metrics) RecordShutdown(result string) {
	m.shutdownDrains.WithLabelValues(result).Inc()
}

type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func routePattern(r *http.Request) string {
	return chi.RouteContext(r.Context()).RoutePattern()
}
