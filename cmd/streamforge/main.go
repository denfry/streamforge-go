package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/denfry/streamforge-go/internal/analytics"
	"github.com/denfry/streamforge-go/internal/campaigns"
	"github.com/denfry/streamforge-go/internal/config"
	"github.com/denfry/streamforge-go/internal/httpapi"
	"github.com/denfry/streamforge-go/internal/messaging"
	"github.com/denfry/streamforge-go/internal/observability"
	"github.com/denfry/streamforge-go/internal/processing"
	clickhousestore "github.com/denfry/streamforge-go/internal/storage/clickhouse"
	"github.com/denfry/streamforge-go/internal/storage/postgres"
	redisstore "github.com/denfry/streamforge-go/internal/storage/redis"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	redisclient "github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func main() {
	logger := observability.NewLogger(nil)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, logger); err != nil {
		logger.Error("streamforge stopped with error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	appCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	pool, err := pgxpool.New(appCtx, cfg.PostgresURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pool.Ping(appCtx); err != nil {
		return err
	}
	if err := postgres.ApplyMigration(appCtx, pool, "migrations/postgres/001_init.sql"); err != nil {
		return err
	}

	redisClient := redisclient.NewClient(&redisclient.Options{Addr: cfg.RedisAddr})
	defer redisClient.Close()
	cache := redisstore.NewCache(redisClient, "streamforge:")
	if err := cache.Ping(appCtx); err != nil {
		return err
	}

	clickConn, err := clickhousestore.NewConnection(cfg.ClickHouseURL)
	if err != nil {
		return err
	}
	defer clickConn.Close()
	if err := clickhousestore.ApplyMigration(appCtx, clickConn, "migrations/clickhouse/001_events.sql"); err != nil {
		return err
	}
	analyticsStore := clickhousestore.NewStore(clickConn)

	campaignService := campaigns.NewService(postgres.NewStore(pool), cache, cfg.CampaignCacheTTL)
	writer := messaging.NewWriter(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer writer.Close()
	producer := messaging.NewProducer(writer, cfg.KafkaTopic)
	dlqWriter := messaging.NewWriter(cfg.KafkaBrokers, cfg.KafkaDLQTopic)
	defer dlqWriter.Close()
	dlq := messaging.NewDLQPublisher(dlqWriter, cfg.KafkaDLQTopic)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:       cfg.KafkaBrokers,
		GroupID:       cfg.KafkaGroupID,
		Topic:         cfg.KafkaTopic,
		QueueCapacity: cfg.QueueCapacity,
		MinBytes:      1,
		MaxBytes:      1 << 20,
		MaxWait:       500 * time.Millisecond,
		StartOffset:   kafka.FirstOffset,
		MaxAttempts:   3,
	})
	defer reader.Close()

	metrics := observability.NewMetrics(prometheus.NewRegistry())
	health := dependencyHealth{pool: pool, cache: cache, analytics: analyticsStore, brokers: cfg.KafkaBrokers}
	handler := httpapi.NewRouter(httpapi.Dependencies{
		Campaigns:         campaignService,
		Producer:          producer,
		Stats:             analyticsStore,
		Health:            health,
		Metrics:           metrics,
		PublishTimeout:    cfg.PublishTimeout,
		StatsMaxRange:     cfg.StatsMaxRange,
		RateLimitRequests: cfg.RateLimitRequests,
		RateLimitWindow:   cfg.RateLimitWindow,
	})
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	consumer := processing.NewConsumer(processing.ConsumerDependencies{
		Reader:         reader,
		Analytics:      analyticsStore,
		DLQ:            dlq,
		WorkerCount:    cfg.WorkerCount,
		QueueCapacity:  cfg.QueueCapacity,
		Retry:          processing.RetryPolicy{MaxAttempts: cfg.RetryMaxAttempts, BaseDelay: cfg.RetryBaseDelay, Classify: func(error) bool { return true }},
		ProcessTimeout: cfg.ProcessTimeout,
		DLQTimeout:     cfg.PublishTimeout,
	})

	serverErrors := make(chan error, 1)
	consumerErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	go func() { consumerErrors <- consumer.Run(appCtx) }()

	select {
	case err := <-serverErrors:
		cancel()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case err := <-consumerErrors:
		if err != nil {
			cancel()
			_ = server.Close()
			return err
		}
		return nil
	case <-ctx.Done():
		cancel()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	serverErr := server.Shutdown(shutdownCtx)
	select {
	case consumerErr := <-consumerErrors:
		if serverErr != nil {
			return serverErr
		}
		return consumerErr
	case <-shutdownCtx.Done():
		return shutdownCtx.Err()
	}
}

type dependencyHealth struct {
	pool      *pgxpool.Pool
	cache     *redisstore.Cache
	analytics analytics.Store
	brokers   []string
}

func (h dependencyHealth) Check(ctx context.Context) map[string]string {
	status := map[string]string{"postgres": "ok", "redis": "ok", "clickhouse": "ok", "kafka": "ok"}
	if err := h.pool.Ping(ctx); err != nil {
		status["postgres"] = "degraded"
	}
	if err := h.cache.Ping(ctx); err != nil {
		status["redis"] = "degraded"
	}
	if err := h.analytics.Ping(ctx); err != nil {
		status["clickhouse"] = "degraded"
	}
	if !brokerReachable(ctx, h.brokers) {
		status["kafka"] = "degraded"
	}
	return status
}

func brokerReachable(ctx context.Context, brokers []string) bool {
	for _, broker := range brokers {
		dialer := net.Dialer{Timeout: time.Second}
		conn, err := dialer.DialContext(ctx, "tcp", broker)
		if err == nil {
			_ = conn.Close()
			return true
		}
	}
	return false
}

var _ httpapi.HealthChecker = dependencyHealth{}
var _ analytics.Store = (*clickhousestore.Store)(nil)
