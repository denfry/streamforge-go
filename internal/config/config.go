package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr           string
	PostgresURL        string
	RedisAddr          string
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaDLQTopic      string
	KafkaGroupID       string
	ClickHouseURL      string
	ClickHouseDatabase string
	WorkerCount        int
	QueueCapacity      int
	PublishTimeout     time.Duration
	ProcessTimeout     time.Duration
	ShutdownTimeout    time.Duration
	RetryMaxAttempts   int
	RetryBaseDelay     time.Duration
	RateLimitRequests  int
	RateLimitWindow    time.Duration
	CampaignCacheTTL   time.Duration
	StatsMaxRange      time.Duration
}

func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		HTTPAddr:           valueOr(getenv, "HTTP_ADDR", ":8080"),
		PostgresURL:        getenv("POSTGRES_URL"),
		RedisAddr:          valueOr(getenv, "REDIS_ADDR", "redis:6379"),
		KafkaTopic:         valueOr(getenv, "KAFKA_TOPIC", "streamforge.events"),
		KafkaDLQTopic:      valueOr(getenv, "KAFKA_DLQ_TOPIC", "streamforge.events.dlq"),
		KafkaGroupID:       valueOr(getenv, "KAFKA_GROUP_ID", "streamforge-workers"),
		ClickHouseURL:      getenv("CLICKHOUSE_URL"),
		ClickHouseDatabase: valueOr(getenv, "CLICKHOUSE_DB", "streamforge"),
	}

	brokers := strings.Split(getenv("KAFKA_BROKERS"), ",")
	cfg.KafkaBrokers = make([]string, 0, len(brokers))
	for _, broker := range brokers {
		if broker = strings.TrimSpace(broker); broker != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, broker)
		}
	}

	var err error
	if cfg.WorkerCount, err = positiveInt(getenv, "WORKER_COUNT", 4); err != nil {
		return Config{}, err
	}
	if cfg.QueueCapacity, err = positiveInt(getenv, "QUEUE_CAPACITY", 16); err != nil {
		return Config{}, err
	}
	if cfg.RetryMaxAttempts, err = boundedInt(getenv, "RETRY_MAX_ATTEMPTS", 3, 1, 10); err != nil {
		return Config{}, err
	}
	if cfg.RateLimitRequests, err = positiveInt(getenv, "RATE_LIMIT_REQUESTS", 100); err != nil {
		return Config{}, err
	}

	if cfg.PublishTimeout, err = positiveDuration(getenv, "PUBLISH_TIMEOUT", 2*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.ProcessTimeout, err = positiveDuration(getenv, "PROCESS_TIMEOUT", 5*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = positiveDuration(getenv, "SHUTDOWN_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.RetryBaseDelay, err = positiveDuration(getenv, "RETRY_BASE_DELAY", 25*time.Millisecond); err != nil {
		return Config{}, err
	}
	if cfg.RateLimitWindow, err = positiveDuration(getenv, "RATE_LIMIT_WINDOW", time.Minute); err != nil {
		return Config{}, err
	}
	if cfg.CampaignCacheTTL, err = positiveDuration(getenv, "CAMPAIGN_CACHE_TTL", 5*time.Minute); err != nil {
		return Config{}, err
	}
	if cfg.StatsMaxRange, err = positiveDuration(getenv, "STATS_MAX_RANGE", 31*24*time.Hour); err != nil {
		return Config{}, err
	}

	for _, required := range []struct {
		key   string
		value string
	}{
		{key: "POSTGRES_URL", value: cfg.PostgresURL},
		{key: "KAFKA_BROKERS", value: strings.Join(cfg.KafkaBrokers, ",")},
		{key: "CLICKHOUSE_URL", value: cfg.ClickHouseURL},
	} {
		if strings.TrimSpace(required.value) == "" {
			return Config{}, fmt.Errorf("%s is required", required.key)
		}
	}
	if cfg.QueueCapacity < cfg.WorkerCount {
		return Config{}, fmt.Errorf("QUEUE_CAPACITY must be at least WORKER_COUNT")
	}
	return cfg, nil
}

func valueOr(getenv func(string) string, key, fallback string) string {
	if value := strings.TrimSpace(getenv(key)); value != "" {
		return value
	}
	return fallback
}

func positiveInt(getenv func(string) string, key string, fallback int) (int, error) {
	value := strings.TrimSpace(getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", key, value)
	}
	return parsed, nil
}

func boundedInt(getenv func(string) string, key string, fallback, min, max int) (int, error) {
	value, err := positiveInt(getenv, key, fallback)
	if err != nil {
		return 0, err
	}
	if value < min || value > max {
		return 0, fmt.Errorf("%s must be between %d and %d, got %d", key, min, max, value)
	}
	return value, nil
}

func positiveDuration(getenv func(string) string, key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration, got %q", key, value)
	}
	return parsed, nil
}
