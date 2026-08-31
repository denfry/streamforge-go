package config

import (
	"strings"
	"testing"
)

func TestLoadRejectsMissingRequiredValues(t *testing.T) {
	_, err := Load(func(key string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "POSTGRES_URL") {
		t.Fatalf("expected missing POSTGRES_URL error, got %v", err)
	}
}

func TestLoadAppliesBoundedDefaults(t *testing.T) {
	env := validEnv()
	cfg, err := Load(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WorkerCount < 1 || cfg.QueueCapacity < cfg.WorkerCount {
		t.Fatalf("invalid defaults: %+v", cfg)
	}
}

func TestLoadRejectsInvalidConcurrencyAndTimeoutValues(t *testing.T) {
	env := validEnv()
	env["WORKER_COUNT"] = "0"
	if _, err := Load(func(key string) string { return env[key] }); err == nil {
		t.Fatal("expected worker validation error")
	}

	env = validEnv()
	env["PUBLISH_TIMEOUT"] = "0s"
	if _, err := Load(func(key string) string { return env[key] }); err == nil {
		t.Fatal("expected timeout validation error")
	}
}

func validEnv() map[string]string {
	return map[string]string{
		"HTTP_ADDR":           ":8080",
		"POSTGRES_URL":        "postgres://app:app@postgres:5432/streamforge?sslmode=disable",
		"REDIS_ADDR":          "redis:6379",
		"KAFKA_BROKERS":       "kafka:9092",
		"KAFKA_TOPIC":         "streamforge.events",
		"KAFKA_DLQ_TOPIC":     "streamforge.events.dlq",
		"KAFKA_GROUP_ID":      "streamforge-workers",
		"CLICKHOUSE_URL":      "clickhouse://clickhouse:9000",
		"CLICKHOUSE_DB":       "streamforge",
		"WORKER_COUNT":        "4",
		"QUEUE_CAPACITY":      "16",
		"PUBLISH_TIMEOUT":     "2s",
		"PROCESS_TIMEOUT":     "5s",
		"SHUTDOWN_TIMEOUT":    "10s",
		"RETRY_MAX_ATTEMPTS":  "3",
		"RETRY_BASE_DELAY":    "25ms",
		"RATE_LIMIT_REQUESTS": "100",
		"RATE_LIMIT_WINDOW":   "1m",
	}
}
