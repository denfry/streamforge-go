package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/google/uuid"
	redisclient "github.com/redis/go-redis/v9"
)

func TestCacheRoundTripsCampaign(t *testing.T) {
	server := miniredis.RunT(t)
	client := redisclient.NewClient(&redisclient.Options{Addr: server.Addr()})
	cache := NewCache(client, "streamforge:")
	campaign := domain.Campaign{ID: uuid.New(), Name: "Spring", DailyBudget: 250, Currency: "USD", Status: "active"}

	if err := cache.SetCampaign(context.Background(), campaign, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, found, err := cache.GetCampaign(context.Background(), campaign.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || got.ID != campaign.ID || got.Name != campaign.Name {
		t.Fatalf("unexpected cache result: found=%v campaign=%+v", found, got)
	}
}

func TestCacheReturnsMissForUnknownCampaign(t *testing.T) {
	server := miniredis.RunT(t)
	client := redisclient.NewClient(&redisclient.Options{Addr: server.Addr()})
	cache := NewCache(client, "streamforge:")

	_, found, err := cache.GetCampaign(context.Background(), uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("unknown campaign was found")
	}
}

func TestCacheIncrementsExpiringEventCounter(t *testing.T) {
	server := miniredis.RunT(t)
	client := redisclient.NewClient(&redisclient.Options{Addr: server.Addr()})
	cache := NewCache(client, "streamforge:")
	campaignID := uuid.New()

	if err := cache.IncrementEvent(context.Background(), campaignID, domain.EventTypeClick, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := server.Get("streamforge:counter:" + campaignID.String() + ":click")
	if err != nil || got != "1" {
		t.Fatalf("counter=%q, err=%v; want 1", got, err)
	}
}
