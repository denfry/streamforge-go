package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/google/uuid"
)

type Store struct {
	conn clickhouse.Conn
}

func NewStore(conn clickhouse.Conn) *Store {
	return &Store{conn: conn}
}

func (s *Store) InsertEvent(ctx context.Context, event domain.Event, processedAt time.Time, attempt int) error {
	metadata, err := json.Marshal(event.Metadata)
	if err != nil {
		return fmt.Errorf("encode event metadata: %w", err)
	}
	version := uint64(processedAt.UnixNano())
	if version == 0 {
		version = 1
	}
	return s.conn.Exec(ctx, `
		INSERT INTO streamforge.events
		(event_id, event_type, campaign_id, user_id, occurred_at, metadata, processed_at, processing_attempt, version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, event.EventID, event.Type, event.CampaignID, event.UserID, event.OccurredAt.UTC(), string(metadata), processedAt.UTC(), attempt, version)
}

func (s *Store) CampaignStats(ctx context.Context, campaignID uuid.UUID, from, to time.Time) (domain.CampaignStats, error) {
	var impressions, clicks uint64
	err := s.conn.QueryRow(ctx, `
		SELECT
			countIf(event_type = 'impression'),
			countIf(event_type = 'click')
		FROM streamforge.events FINAL
		WHERE campaign_id = ? AND occurred_at >= ? AND occurred_at < ?
	`, campaignID, from.UTC(), to.UTC()).Scan(&impressions, &clicks)
	if err != nil {
		return domain.CampaignStats{}, err
	}
	ctr := 0.0
	if impressions > 0 {
		ctr = float64(clicks) / float64(impressions)
	}
	return domain.CampaignStats{CampaignID: campaignID, From: from.UTC(), To: to.UTC(), Impressions: impressions, Clicks: clicks, ClickThroughRate: ctr}, nil
}

func (s *Store) Ping(ctx context.Context) error {
	return s.conn.Ping(ctx)
}

func (s *Store) Close() error {
	s.conn.Close()
	return nil
}

func NewConnection(dsn string) (clickhouse.Conn, error) {
	options, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return clickhouse.Open(options)
}
