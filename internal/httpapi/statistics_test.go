package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/google/uuid"
)

func TestCampaignStatsRejectsInvertedOrExcessiveRanges(t *testing.T) {
	campaignID := uuid.New()
	deps := testDependencies()
	deps.Stats = &fakeStats{}
	for _, query := range []string{
		"/v1/stats/campaign/" + campaignID.String() + "?from=2026-08-31T02:00:00Z&to=2026-08-31T01:00:00Z",
		"/v1/stats/campaign/" + campaignID.String() + "?from=2025-01-01T00:00:00Z&to=2026-08-31T01:00:00Z",
	} {
		rec := httptest.NewRecorder()
		NewRouter(deps).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, query, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("query=%s status=%d body=%s", query, rec.Code, rec.Body.String())
		}
	}
}

func TestCampaignStatsReturnsAnalyticsStoreResponse(t *testing.T) {
	campaignID := uuid.New()
	deps := testDependencies()
	deps.Stats = &fakeStats{stats: domain.CampaignStats{CampaignID: campaignID, Impressions: 10, Clicks: 2, ClickThroughRate: 0.2}}
	path := "/v1/stats/campaign/" + campaignID.String()
	rec := httptest.NewRecorder()
	NewRouter(deps).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code != http.StatusOK || deps.Stats.(*fakeStats).calls != 1 {
		t.Fatalf("status=%d calls=%d body=%s", rec.Code, deps.Stats.(*fakeStats).calls, rec.Body.String())
	}
}

type fakeStats struct {
	stats domain.CampaignStats
	calls int
}

func (f *fakeStats) InsertEvent(context.Context, domain.Event, time.Time, int) error { return nil }
func (f *fakeStats) CampaignStats(_ context.Context, id uuid.UUID, from, to time.Time) (domain.CampaignStats, error) {
	f.calls++
	f.stats.CampaignID = id
	f.stats.From = from
	f.stats.To = to
	return f.stats, nil
}
func (f *fakeStats) Ping(context.Context) error { return nil }
func (f *fakeStats) Close() error               { return nil }
