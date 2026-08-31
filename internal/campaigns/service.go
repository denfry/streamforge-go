package campaigns

import (
	"context"
	"errors"
	"time"

	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/denfry/streamforge-go/internal/events"
	"github.com/google/uuid"
)

var ErrNotFound = errors.New("campaign not found")

type Repository interface {
	Create(ctx context.Context, input domain.CampaignInput) (domain.Campaign, error)
	Get(ctx context.Context, id uuid.UUID) (domain.Campaign, error)
	EnsureUser(ctx context.Context, id string) error
}

type Cache interface {
	GetCampaign(ctx context.Context, id uuid.UUID) (domain.Campaign, bool, error)
	SetCampaign(ctx context.Context, campaign domain.Campaign, ttl time.Duration) error
	DeleteCampaign(ctx context.Context, id uuid.UUID) error
	IncrementEvent(ctx context.Context, campaignID uuid.UUID, eventType domain.EventType, ttl time.Duration) error
}

type Service struct {
	repo  Repository
	cache Cache
	ttl   time.Duration
}

func NewService(repo Repository, cache Cache, cacheTTL time.Duration) *Service {
	return &Service{repo: repo, cache: cache, ttl: cacheTTL}
}

func (s *Service) Create(ctx context.Context, input domain.CampaignInput) (domain.Campaign, error) {
	if err := events.ValidateCampaignInput(input); err != nil {
		return domain.Campaign{}, err
	}
	campaign, err := s.repo.Create(ctx, input)
	if err != nil {
		return domain.Campaign{}, err
	}
	if s.cache != nil {
		_ = s.cache.DeleteCampaign(ctx, campaign.ID)
	}
	return campaign, nil
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (domain.Campaign, error) {
	if s.cache != nil {
		if campaign, found, err := s.cache.GetCampaign(ctx, id); err == nil && found {
			return campaign, nil
		}
	}
	campaign, err := s.repo.Get(ctx, id)
	if err != nil {
		return domain.Campaign{}, err
	}
	if s.cache != nil {
		_ = s.cache.SetCampaign(ctx, campaign, s.ttl)
	}
	return campaign, nil
}

func (s *Service) EnsureUser(ctx context.Context, id string) error {
	return s.repo.EnsureUser(ctx, id)
}
