package integration

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/denfry/streamforge-go/internal/campaigns"
	"github.com/denfry/streamforge-go/internal/domain"
	clickhousestore "github.com/denfry/streamforge-go/internal/storage/clickhouse"
	"github.com/denfry/streamforge-go/internal/storage/postgres"
	redisstore "github.com/denfry/streamforge-go/internal/storage/redis"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	redisclient "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestDependencyAdaptersEndToEnd(t *testing.T) {
	if os.Getenv("STREAMFORGE_INTEGRATION") != "1" {
		t.Skip("set STREAMFORGE_INTEGRATION=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	root := repositoryRoot()

	postgresContainer := startContainer(t, ctx, testcontainers.ContainerRequest{
		Image:        "postgres:16.4-alpine",
		Env:          map[string]string{"POSTGRES_DB": "streamforge", "POSTGRES_USER": "app", "POSTGRES_PASSWORD": "app"},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForListeningPort(nat.Port("5432/tcp")),
	})
	postgresHost := containerEndpoint(t, ctx, postgresContainer, "5432/tcp")
	pool, err := pgxpool.New(ctx, "postgres://app:app@"+postgresHost+"/streamforge?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := postgres.ApplyMigration(ctx, pool, filepath.Join(root, "migrations", "postgres", "001_init.sql")); err != nil {
		t.Fatal(err)
	}

	redisContainer := startContainer(t, ctx, testcontainers.ContainerRequest{
		Image:        "redis:7.4.1-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForListeningPort(nat.Port("6379/tcp")),
	})
	redisHost := containerEndpoint(t, ctx, redisContainer, "6379/tcp")
	redisClient := redisclient.NewClient(&redisclient.Options{Addr: redisHost})
	defer redisClient.Close()
	cache := redisstore.NewCache(redisClient, "integration:")
	if err := cache.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	clickhouseContainer := startContainer(t, ctx, testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:24.8-alpine",
		ExposedPorts: []string{"9000/tcp"},
		WaitingFor:   wait.ForListeningPort(nat.Port("9000/tcp")),
	})
	clickhouseHost := containerEndpoint(t, ctx, clickhouseContainer, "9000/tcp")
	clickConn, err := clickhousedriver.Open(&clickhousedriver.Options{Addr: []string{clickhouseHost}})
	if err != nil {
		t.Fatal(err)
	}
	defer clickConn.Close()
	if err := clickhousestore.ApplyMigration(ctx, clickConn, filepath.Join(root, "migrations", "clickhouse", "001_events.sql")); err != nil {
		t.Fatal(err)
	}
	analyticsStore := clickhousestore.NewStore(clickConn)

	campaignService := campaigns.NewService(postgres.NewStore(pool), cache, time.Minute)
	campaign, err := campaignService.Create(ctx, domain.CampaignInput{Name: "Integration", DailyBudget: 100, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := campaignService.Get(ctx, campaign.ID); err != nil {
		t.Fatal(err)
	}
	event := domain.Event{EventID: uuid.New(), Type: domain.EventTypeImpression, CampaignID: campaign.ID, UserID: "integration-user", OccurredAt: time.Now().UTC()}
	if err := analyticsStore.InsertEvent(ctx, event, time.Now().UTC(), 1); err != nil {
		t.Fatal(err)
	}
	stats, err := analyticsStore.CampaignStats(ctx, campaign.ID, time.Now().Add(-time.Minute), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Impressions != 1 {
		t.Fatalf("impressions=%d, want 1", stats.Impressions)
	}

	kafkaContainer := startContainer(t, ctx, testcontainers.ContainerRequest{
		Image: "apache/kafka:4.0.0",
		Env: map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_LISTENERS":                                "CONTROLLER://:9093,PLAINTEXT://:9092",
			"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://localhost:9092",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
		},
		ExposedPorts: []string{"9092/tcp"},
		WaitingFor:   wait.ForListeningPort(nat.Port("9092/tcp")),
	})
	code, output, err := kafkaContainer.Exec(ctx, []string{"/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--create", "--if-not-exists", "--topic", "integration.events", "--partitions", "1", "--replication-factor", "1"})
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		bytes, _ := io.ReadAll(output)
		t.Fatalf("Kafka topic creation failed: %s", bytes)
	}
}

func startContainer(t *testing.T, ctx context.Context, request testcontainers.ContainerRequest) testcontainers.Container {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: request, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = container.Terminate(cleanupCtx)
	})
	return container
}

func containerEndpoint(t *testing.T, ctx context.Context, container testcontainers.Container, port string) string {
	t.Helper()
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mapped, err := container.MappedPort(ctx, nat.Port(port))
	if err != nil {
		t.Fatal(err)
	}
	return host + ":" + mapped.Port()
}

func repositoryRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
