package campaigns

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/google/uuid"
)

func TestCampaignServiceCreatesOnlyValidatedCampaigns(t *testing.T) {
	repo := &fakeRepository{}
	service := NewService(repo, &fakeCache{}, time.Minute)

	_, err := service.Create(context.Background(), domain.CampaignInput{Name: "", DailyBudget: 0, Currency: "USD"})
	if err == nil {
		t.Fatal("expected validation error")
	}
	if repo.createCalls != 0 {
		t.Fatalf("repository was called %d times", repo.createCalls)
	}
}

func TestCampaignServiceUsesRepositoryWhenCacheMisses(t *testing.T) {
	campaign := validCampaign()
	repo := &fakeRepository{campaign: campaign}
	cache := &fakeCache{}
	service := NewService(repo, cache, time.Minute)

	got, err := service.Get(context.Background(), campaign.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != campaign.ID || repo.getCalls != 1 || cache.setCalls != 1 {
		t.Fatalf("unexpected result: campaign=%+v repoGets=%d cacheSets=%d", got, repo.getCalls, cache.setCalls)
	}
}

func TestCampaignServiceTreatsCacheFailureAsMiss(t *testing.T) {
	campaign := validCampaign()
	repo := &fakeRepository{campaign: campaign}
	cache := &fakeCache{getErr: errors.New("redis unavailable")}

	got, err := NewService(repo, cache, time.Minute).Get(context.Background(), campaign.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != campaign.ID || repo.getCalls != 1 {
		t.Fatalf("cache failure prevented repository read: %+v", got)
	}
}

type fakeRepository struct {
	campaign    domain.Campaign
	createCalls int
	getCalls    int
}

func (f *fakeRepository) Create(context.Context, domain.CampaignInput) (domain.Campaign, error) {
	f.createCalls++
	return f.campaign, nil
}

func (f *fakeRepository) Get(context.Context, uuid.UUID) (domain.Campaign, error) {
	f.getCalls++
	return f.campaign, nil
}

func (f *fakeRepository) EnsureUser(context.Context, string) error { return nil }

type fakeCache struct {
	campaign domain.Campaign
	getErr   error
	setCalls int
}

func (f *fakeCache) GetCampaign(context.Context, uuid.UUID) (domain.Campaign, bool, error) {
	if f.getErr != nil {
		return domain.Campaign{}, false, f.getErr
	}
	if f.campaign.ID == uuid.Nil {
		return domain.Campaign{}, false, nil
	}
	return f.campaign, true, nil
}

func (f *fakeCache) SetCampaign(context.Context, domain.Campaign, time.Duration) error {
	f.setCalls++
	return nil
}

func (f *fakeCache) DeleteCampaign(context.Context, uuid.UUID) error { return nil }

func (f *fakeCache) IncrementEvent(context.Context, uuid.UUID, domain.EventType, time.Duration) error {
	return nil
}

func validCampaign() domain.Campaign {
	return domain.Campaign{ID: uuid.New(), Name: "Spring", DailyBudget: 250, Currency: "USD", Status: "active"}
}
