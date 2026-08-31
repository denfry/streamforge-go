package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/google/uuid"
	redisclient "github.com/redis/go-redis/v9"
)

type Cache struct {
	client *redisclient.Client
	prefix string
}

func NewCache(client *redisclient.Client, prefix string) *Cache {
	return &Cache{client: client, prefix: prefix}
}

func (c *Cache) GetCampaign(ctx context.Context, id uuid.UUID) (domain.Campaign, bool, error) {
	value, err := c.client.Get(ctx, c.campaignKey(id)).Result()
	if errors.Is(err, redisclient.Nil) {
		return domain.Campaign{}, false, nil
	}
	if err != nil {
		return domain.Campaign{}, false, err
	}
	var campaign domain.Campaign
	if err := json.Unmarshal([]byte(value), &campaign); err != nil {
		return domain.Campaign{}, false, fmt.Errorf("decode campaign cache: %w", err)
	}
	return campaign, true, nil
}

func (c *Cache) SetCampaign(ctx context.Context, campaign domain.Campaign, ttl time.Duration) error {
	payload, err := json.Marshal(campaign)
	if err != nil {
		return fmt.Errorf("encode campaign cache: %w", err)
	}
	return c.client.Set(ctx, c.campaignKey(campaign.ID), payload, ttl).Err()
}

func (c *Cache) DeleteCampaign(ctx context.Context, id uuid.UUID) error {
	return c.client.Del(ctx, c.campaignKey(id)).Err()
}

func (c *Cache) IncrementEvent(ctx context.Context, campaignID uuid.UUID, eventType domain.EventType, ttl time.Duration) error {
	key := c.prefix + "counter:" + campaignID.String() + ":" + string(eventType)
	if err := c.client.Incr(ctx, key).Err(); err != nil {
		return err
	}
	return c.client.Expire(ctx, key, ttl).Err()
}

func (c *Cache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *Cache) Close() error {
	return c.client.Close()
}

func (c *Cache) campaignKey(id uuid.UUID) string {
	return c.prefix + "campaign:" + id.String()
}
