package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/google/uuid"
)

func TestCreateCampaignReturnsCreatedCampaign(t *testing.T) {
	deps := testDependencies()
	req := httptest.NewRequest(http.MethodPost, "/v1/campaigns", strings.NewReader(`{"name":"Spring","daily_budget":250,"currency":"USD"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	NewRouter(deps).ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestEventEndpointReturnsAcceptedOnlyAfterPublish(t *testing.T) {
	campaignID := uuid.New()
	deps := testDependencies()
	deps.Campaigns = &fakeCampaigns{campaign: domain.Campaign{ID: campaignID, Name: "Spring", Currency: "USD", Status: "active"}}
	eventID := uuid.New()
	body := `{"event_id":"` + eventID.String() + `","campaign_id":"` + campaignID.String() + `","user_id":"user-1","occurred_at":"2026-08-31T07:00:00Z"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/events/impression", strings.NewReader(body))
	rec := httptest.NewRecorder()
	NewRouter(deps).ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || !deps.Producer.(*fakeProducer).published {
		t.Fatalf("status=%d published=%v body=%s", rec.Code, deps.Producer.(*fakeProducer).published, rec.Body.String())
	}
}

func TestEventEndpointMapsPublishFailureToServiceUnavailable(t *testing.T) {
	deps := testDependencies()
	deps.Producer = &fakeProducer{err: errors.New("broker unavailable")}
	campaignID := uuid.New()
	eventID := uuid.New()
	body := `{"event_id":"` + eventID.String() + `","campaign_id":"` + campaignID.String() + `","user_id":"user-1","occurred_at":"2026-08-31T07:00:00Z"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/events/impression", strings.NewReader(body))
	rec := httptest.NewRecorder()
	NewRouter(deps).ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func testDependencies() Dependencies {
	return Dependencies{Campaigns: &fakeCampaigns{campaign: domain.Campaign{ID: uuid.New(), Name: "Spring", Currency: "USD", Status: "active"}}, Producer: &fakeProducer{}}
}

type fakeCampaigns struct {
	campaign domain.Campaign
	err      error
}

func (f *fakeCampaigns) Create(context.Context, domain.CampaignInput) (domain.Campaign, error) {
	if f.err != nil {
		return domain.Campaign{}, f.err
	}
	return f.campaign, nil
}

func (f *fakeCampaigns) Get(context.Context, uuid.UUID) (domain.Campaign, error) {
	if f.err != nil {
		return domain.Campaign{}, f.err
	}
	return f.campaign, nil
}

type fakeProducer struct {
	published bool
	err       error
}

func (f *fakeProducer) Publish(context.Context, domain.Event) error {
	f.published = true
	return f.err
}
