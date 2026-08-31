package events

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/denfry/streamforge-go/internal/domain"
)

type ValidationError struct {
	Fields map[string]string
}

func (e *ValidationError) Error() string {
	if len(e.Fields) == 0 {
		return "validation failed"
	}
	parts := make([]string, 0, len(e.Fields))
	for field, message := range e.Fields {
		parts = append(parts, fmt.Sprintf("%s: %s", field, message))
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

func ValidateEvent(event domain.Event, expectedType domain.EventType) error {
	fields := make(map[string]string)
	if event.EventID == [16]byte{} {
		fields["event_id"] = "must be a valid UUID"
	}
	if event.CampaignID == [16]byte{} {
		fields["campaign_id"] = "must be a valid UUID"
	}
	if event.Type != expectedType {
		fields["type"] = "does not match endpoint"
	}
	if event.UserID == "" || len(event.UserID) > 128 {
		fields["user_id"] = "must contain between 1 and 128 characters"
	}
	if event.OccurredAt.IsZero() {
		fields["occurred_at"] = "is required"
	} else if event.OccurredAt.After(time.Now().UTC().Add(5 * time.Minute)) {
		fields["occurred_at"] = "cannot be more than five minutes in the future"
	}
	if size, err := json.Marshal(event.Metadata); err != nil || len(size) > 8192 {
		fields["metadata"] = "must be valid and no larger than 8 KiB"
	}
	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

func ValidateCampaignInput(input domain.CampaignInput) error {
	fields := make(map[string]string)
	name := strings.TrimSpace(input.Name)
	if name == "" || len(name) > 160 {
		fields["name"] = "must contain between 1 and 160 characters"
	}
	if math.IsNaN(input.DailyBudget) || math.IsInf(input.DailyBudget, 0) || input.DailyBudget <= 0 {
		fields["daily_budget"] = "must be a finite positive number"
	}
	currency := strings.TrimSpace(input.Currency)
	if len(currency) != 3 || currency != strings.ToUpper(currency) {
		fields["currency"] = "must be a three-letter uppercase code"
	}
	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}
