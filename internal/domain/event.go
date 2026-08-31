package domain

import (
	"time"

	"github.com/google/uuid"
)

type EventType string

const (
	EventTypeImpression EventType = "impression"
	EventTypeClick      EventType = "click"
)

type Event struct {
	EventID    uuid.UUID         `json:"event_id"`
	Type       EventType         `json:"type"`
	CampaignID uuid.UUID         `json:"campaign_id"`
	UserID     string            `json:"user_id"`
	OccurredAt time.Time         `json:"occurred_at"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

type FailureMetadata struct {
	SourceTopic     string `json:"source_topic"`
	SourcePartition int    `json:"source_partition"`
	SourceOffset    int64  `json:"source_offset"`
	ErrorClass      string `json:"error_class"`
	Attempts        int    `json:"attempts"`
}

type EventEnvelope struct {
	SchemaVersion int              `json:"schema_version"`
	Event         Event            `json:"event"`
	Failure       *FailureMetadata `json:"failure,omitempty"`
}
