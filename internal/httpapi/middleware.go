package httpapi

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

type requestIDKey struct{}

type Middleware func(http.Handler) http.Handler

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if _, err := uuid.Parse(id); err != nil {
			id = uuid.NewString()
		}
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type rateLimitEntry struct {
	started time.Time
	count   int
}

type rateLimiter struct {
	mu      sync.Mutex
	limit   int
	window  time.Duration
	entries map[string]rateLimitEntry
}

func NewRateLimiter(limit int, window time.Duration) Middleware {
	if limit < 1 || window <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	limiter := &rateLimiter{limit: limit, window: window, entries: make(map[string]rateLimitEntry)}
	return limiter.middleware
}

func (l *rateLimiter) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		key := clientKey(r)
		l.mu.Lock()
		entry, ok := l.entries[key]
		if !ok || now.Sub(entry.started) >= l.window {
			entry = rateLimitEntry{started: now}
		}
		allowed := entry.count < l.limit
		if allowed {
			entry.count++
			l.entries[key] = entry
			if len(l.entries) > 10000 {
				l.removeExpired(now)
			}
		}
		l.mu.Unlock()

		if !allowed {
			w.Header().Set("Retry-After", durationSeconds(l.window-now.Sub(entry.started)))
			writeError(w, http.StatusTooManyRequests, "rate_limited", "request rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (l *rateLimiter) removeExpired(now time.Time) {
	for key, entry := range l.entries {
		if now.Sub(entry.started) >= l.window {
			delete(l.entries, key)
		}
	}
	if len(l.entries) <= 10000 {
		return
	}
	for key := range l.entries {
		delete(l.entries, key)
		if len(l.entries) <= 10000 {
			return
		}
	}
}

func clientKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	if r.RemoteAddr == "" {
		return "unknown"
	}
	return r.RemoteAddr
}

func durationSeconds(value time.Duration) string {
	seconds := int(value / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	return formatInt(seconds)
}

func formatInt(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	pos := len(digits)
	for value > 0 {
		pos--
		digits[pos] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[pos:])
}
