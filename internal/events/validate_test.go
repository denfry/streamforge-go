package events

import (
	"strings"
	"testing"
	"time"

	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/google/uuid"
)

func TestValidateEventRejectsInvalidIdentityAndTimestamp(t *testing.T) {
	event := validEvent()
	event.EventID = uuid.Nil
	if err := ValidateEvent(event, domain.EventTypeImpression); err == nil {
		t.Fatal("expected event ID error")
	}

	event = validEvent()
	event.OccurredAt = time.Now().Add(10 * time.Minute)
	if err := ValidateEvent(event, domain.EventTypeImpression); err == nil {
		t.Fatal("expected future timestamp error")
	}
}

func TestValidateEventRejectsTypeMismatchAndLargeMetadata(t *testing.T) {
	event := validEvent()
	if err := ValidateEvent(event, domain.EventTypeClick); err == nil {
		t.Fatal("expected type mismatch")
	}

	event = validEvent()
	event.Metadata = map[string]string{"payload": strings.Repeat("x", 8193)}
	if err := ValidateEvent(event, domain.EventTypeImpression); err == nil {
		t.Fatal("expected metadata size error")
	}
}

func TestValidateCampaignInputRequiresPositiveBudgetAndCurrency(t *testing.T) {
	input := domain.CampaignInput{Name: "", DailyBudget: 0, Currency: "US"}
	if err := ValidateCampaignInput(input); err == nil {
		t.Fatal("expected campaign validation error")
	}
}

func validEvent() domain.Event {
	return domain.Event{
		EventID:    uuid.New(),
		Type:       domain.EventTypeImpression,
		CampaignID: uuid.New(),
		UserID:     "user-42",
		OccurredAt: time.Now().UTC().Add(-time.Minute),
		Metadata:   map[string]string{"placement": "homepage"},
	}
}
