package postgres

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/denfry/streamforge-go/internal/campaigns"
	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Create(ctx context.Context, input domain.CampaignInput) (domain.Campaign, error) {
	id := uuid.New()
	now := time.Now().UTC()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Campaign{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var campaign domain.Campaign
	err = tx.QueryRow(ctx, `
		INSERT INTO campaigns (id, name, daily_budget, currency, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, 'active', $5, $5)
		RETURNING id, name, daily_budget::float8, currency, status, created_at, updated_at
	`, id, strings.TrimSpace(input.Name), input.DailyBudget, input.Currency, now).Scan(
		&campaign.ID,
		&campaign.Name,
		&campaign.DailyBudget,
		&campaign.Currency,
		&campaign.Status,
		&campaign.CreatedAt,
		&campaign.UpdatedAt,
	)
	if err != nil {
		return domain.Campaign{}, err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO campaign_targeting (campaign_id, country, device)
		VALUES ($1, NULLIF($2, ''), NULLIF($3, ''))
	`, campaign.ID, strings.TrimSpace(input.Targeting.Country), strings.TrimSpace(input.Targeting.Device))
	if err != nil {
		return domain.Campaign{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Campaign{}, err
	}
	campaign.Targeting = input.Targeting
	return campaign, nil
}

func (s *Store) Get(ctx context.Context, id uuid.UUID) (domain.Campaign, error) {
	var campaign domain.Campaign
	var country, device *string
	err := s.pool.QueryRow(ctx, `
		SELECT c.id, c.name, c.daily_budget::float8, c.currency, c.status,
		       c.created_at, c.updated_at, t.country, t.device
		FROM campaigns c
		LEFT JOIN campaign_targeting t ON t.campaign_id = c.id
		WHERE c.id = $1
	`, id).Scan(
		&campaign.ID,
		&campaign.Name,
		&campaign.DailyBudget,
		&campaign.Currency,
		&campaign.Status,
		&campaign.CreatedAt,
		&campaign.UpdatedAt,
		&country,
		&device,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Campaign{}, campaigns.ErrNotFound
	}
	if err != nil {
		return domain.Campaign{}, err
	}
	if country != nil {
		campaign.Targeting.Country = *country
	}
	if device != nil {
		campaign.Targeting.Device = *device
	}
	return campaign, nil
}

func (s *Store) EnsureUser(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO users (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`, id)
	return err
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) Close() {
	s.pool.Close()
}
