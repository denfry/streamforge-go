package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/denfry/streamforge-go/internal/analytics"
	"github.com/denfry/streamforge-go/internal/campaigns"
	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/denfry/streamforge-go/internal/events"
	"github.com/denfry/streamforge-go/internal/observability"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type CampaignService interface {
	Create(context.Context, domain.CampaignInput) (domain.Campaign, error)
	Get(context.Context, uuid.UUID) (domain.Campaign, error)
}

type EventProducer interface {
	Publish(context.Context, domain.Event) error
}
type HealthChecker interface {
	Check(context.Context) map[string]string
}

type Dependencies struct {
	Campaigns         CampaignService
	Producer          EventProducer
	Stats             analytics.Store
	Health            HealthChecker
	Metrics           *observability.Metrics
	BodyLimit         int64
	PublishTimeout    time.Duration
	StatsMaxRange     time.Duration
	RateLimitRequests int
	RateLimitWindow   time.Duration
}

func NewRouter(deps Dependencies) http.Handler {
	if deps.BodyLimit <= 0 {
		deps.BodyLimit = 64 * 1024
	}
	if deps.PublishTimeout <= 0 {
		deps.PublishTimeout = 2 * time.Second
	}

	r := chi.NewRouter()
	r.Use(requestID)
	if deps.Metrics != nil {
		r.Use(deps.Metrics.HTTPMiddleware)
	}
	r.Post("/v1/campaigns", deps.createCampaign)
	r.Get("/v1/campaigns/{id}", deps.getCampaign)
	r.Get("/v1/stats/campaign/{id}", deps.getCampaignStats)

	eventRoutes := chi.NewRouter()
	if deps.RateLimitRequests > 0 {
		eventRoutes.Use(NewRateLimiter(deps.RateLimitRequests, deps.RateLimitWindow))
	}
	eventRoutes.Post("/impression", deps.publishEvent(domain.EventTypeImpression))
	eventRoutes.Post("/click", deps.publishEvent(domain.EventTypeClick))
	r.Mount("/v1/events", eventRoutes)

	r.Get("/health", deps.health)
	if deps.Metrics != nil {
		r.Handle("/metrics", deps.Metrics.Handler())
	}
	return r
}

func (d Dependencies) createCampaign(w http.ResponseWriter, r *http.Request) {
	var input domain.CampaignInput
	if err := decodeJSON(w, r, d.BodyLimit, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body is invalid")
		return
	}
	campaign, err := d.Campaigns.Create(r.Context(), input)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, campaign)
}

func (d Dependencies) getCampaign(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_campaign_id", "campaign id must be a UUID")
		return
	}
	campaign, err := d.Campaigns.Get(r.Context(), id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, campaign)
}

func (d Dependencies) publishEvent(expected domain.EventType) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var event domain.Event
		if err := decodeJSON(w, r, d.BodyLimit, &event); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_json", "request body is invalid")
			return
		}
		if event.Type == "" {
			event.Type = expected
		}
		if err := events.ValidateEvent(event, expected); err != nil {
			writeServiceError(w, err)
			return
		}
		if _, err := d.Campaigns.Get(r.Context(), event.CampaignID); err != nil {
			writeServiceError(w, err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), d.PublishTimeout)
		defer cancel()
		if err := d.Producer.Publish(ctx, event); err != nil {
			writeError(w, http.StatusServiceUnavailable, "event_publish_failed", "event could not be accepted")
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"event_id": event.EventID.String(), "status": "accepted"})
	}
}

func (d Dependencies) health(w http.ResponseWriter, r *http.Request) {
	components := map[string]string{"api": "ok"}
	if d.Health != nil {
		for component, status := range d.Health.Check(r.Context()) {
			components[component] = status
		}
	}
	status := http.StatusOK
	for _, componentStatus := range components {
		if componentStatus != "ok" {
			status = http.StatusServiceUnavailable
			break
		}
	}
	writeJSON(w, status, map[string]any{"status": map[bool]string{true: "ok", false: "degraded"}[status == http.StatusOK], "components": components})
}
func (d Dependencies) getCampaignStats(w http.ResponseWriter, r *http.Request) {
	if d.Stats == nil {
		writeError(w, http.StatusServiceUnavailable, "analytics_unavailable", "analytics is unavailable")
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_campaign_id", "campaign id must be a UUID")
		return
	}
	from, to, err := parseStatsRange(r, d.StatsMaxRange)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_stats_range", err.Error())
		return
	}
	stats, err := d.Stats.CampaignStats(r.Context(), id, from, to)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "stats_unavailable", "statistics could not be loaded")
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func parseStatsRange(r *http.Request, maxRange time.Duration) (time.Time, time.Time, error) {
	if maxRange <= 0 {
		maxRange = 31 * 24 * time.Hour
	}
	fromValue, toValue := r.URL.Query().Get("from"), r.URL.Query().Get("to")
	now := time.Now().UTC()
	if fromValue == "" && toValue == "" {
		return now.Add(-24 * time.Hour), now, nil
	}
	if fromValue == "" || toValue == "" {
		return time.Time{}, time.Time{}, errors.New("from and to must be provided together")
	}
	from, err := time.Parse(time.RFC3339, fromValue)
	if err != nil {
		return time.Time{}, time.Time{}, errors.New("from must be RFC3339")
	}
	to, err := time.Parse(time.RFC3339, toValue)
	if err != nil {
		return time.Time{}, time.Time{}, errors.New("to must be RFC3339")
	}
	if !to.After(from) {
		return time.Time{}, time.Time{}, errors.New("to must be after from")
	}
	if to.Sub(from) > maxRange {
		return time.Time{}, time.Time{}, errors.New("requested range is too large")
	}
	return from.UTC(), to.UTC(), nil
}

func decodeJSON(w http.ResponseWriter, r *http.Request, limit int64, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain one JSON value")
	}
	return nil
}

func writeServiceError(w http.ResponseWriter, err error) {
	var validationErr *events.ValidationError
	switch {
	case errors.As(err, &validationErr):
		writeError(w, http.StatusBadRequest, "validation_failed", validationErr.Error())
	case errors.Is(err, campaigns.ErrNotFound):
		writeError(w, http.StatusNotFound, "campaign_not_found", "campaign was not found")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "request could not be completed")
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"code": code, "message": message})
}
