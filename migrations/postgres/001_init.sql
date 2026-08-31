CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS campaigns (
    id UUID PRIMARY KEY,
    name VARCHAR(160) NOT NULL,
    daily_budget NUMERIC(12, 2) NOT NULL CHECK (daily_budget > 0),
    currency CHAR(3) NOT NULL CHECK (currency = upper(currency)),
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS campaign_targeting (
    campaign_id UUID PRIMARY KEY REFERENCES campaigns(id) ON DELETE CASCADE,
    country VARCHAR(2),
    device VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS users (
    id VARCHAR(128) PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS campaigns_status_idx ON campaigns(status);
