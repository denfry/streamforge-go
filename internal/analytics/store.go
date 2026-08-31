package analytics

import (
	"context"
	"time"

	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/google/uuid"
)

type Store interface {
	InsertEvent(ctx context.Context, event domain.Event, processedAt time.Time, attempt int) error
	CampaignStats(ctx context.Context, campaignID uuid.UUID, from, to time.Time) (domain.CampaignStats, error)
	Ping(ctx context.Context) error
	Close() error
}
