package domain

import (
	"time"

	"github.com/google/uuid"
)

type Campaign struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	DailyBudget float64   `json:"daily_budget"`
	Currency    string    `json:"currency"`
	Status      string    `json:"status"`
	Targeting   Targeting `json:"targeting"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type CampaignInput struct {
	Name        string    `json:"name"`
	DailyBudget float64   `json:"daily_budget"`
	Currency    string    `json:"currency"`
	Targeting   Targeting `json:"targeting"`
}

type Targeting struct {
	Country string `json:"country,omitempty"`
	Device  string `json:"device,omitempty"`
}

type CampaignStats struct {
	CampaignID       uuid.UUID `json:"campaign_id"`
	From             time.Time `json:"from"`
	To               time.Time `json:"to"`
	Impressions      uint64    `json:"impressions"`
	Clicks           uint64    `json:"clicks"`
	ClickThroughRate float64   `json:"click_through_rate"`
}
