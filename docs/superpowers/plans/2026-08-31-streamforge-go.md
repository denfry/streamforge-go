# StreamForge Go Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and verify an independently runnable Go advertising-event service with campaign APIs, Kafka ingestion, bounded concurrent processing, ClickHouse analytics, PostgreSQL transactions, Redis cache, DLQ handling, metrics, and documented operational limits.

**Architecture:** `cmd/streamforge` wires a `net/http` + `chi` API, PostgreSQL campaign store, Redis cache, Kafka producer/consumer, ClickHouse analytics store, Prometheus metrics, and a bounded worker pool. HTTP requests publish validated events to Kafka; workers process them at least once, retry transient ClickHouse failures, publish terminal failures to a DLQ, and commit only contiguous completed offsets.

**Tech Stack:** Go 1.24+, `net/http`, `chi`, `pgx/v5`, `go-redis/v9`, `segmentio/kafka-go`, `clickhouse-go/v2`, Prometheus client, PostgreSQL, Redis, Kafka-compatible broker, ClickHouse, Docker Compose, testcontainers-go, golangci-lint, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-31-streamforge-go-design.md`

## Global Constraints

- Keep StreamForge in its own Git repository; do not create a shared monorepo package.
- Expose only the specified campaign, event, statistics, health, and metrics endpoints.
- Use PostgreSQL as the transactional source of truth, Redis only as disposable cache/short-lived state, Kafka as the durable event boundary, and ClickHouse for analytics.
- Every goroutine has an owner and a cancellation path.
- All in-memory queues are bounded and all shared mutable state has explicit synchronization.
- Kafka commits represent only contiguous completed offsets per partition.
- Use at-least-once processing with event IDs and idempotent analytical storage; do not claim exactly-once delivery.
- Do not publish unmeasured throughput, fake traffic, fake users, or commercial experience.
- Keep credentials in environment variables; commit `.env.example`, never real secrets.
- Every implementation task ends with its focused test command and a conventional commit.

---

## File Map

Create these files and keep each responsibility narrow:

- `go.mod`, `go.sum`: module and pinned dependencies.
- `.gitignore`, `.dockerignore`, `.env.example`: repository and secret hygiene.
- `Makefile`: repeatable format, lint, test, race, integration, build, and smoke commands.
- `.golangci.yml`: enabled static-analysis rules.
- `Dockerfile`, `docker-compose.yml`: non-root API image and local dependencies.
- `.github/workflows/ci.yml`: formatting, lint, tests, race, build, integration, smoke.
- `cmd/streamforge/main.go`: dependency wiring, lifecycle, graceful shutdown.
- `internal/config/config.go`: environment parsing and validation.
- `internal/config/config_test.go`: invalid and default configuration behavior.
- `internal/domain/campaign.go`: campaign and targeting data types.
- `internal/domain/event.go`: event envelope and event types.
- `internal/events/validate.go`, `internal/events/validate_test.go`: request and envelope validation.
- `internal/campaigns/service.go`, `internal/campaigns/service_test.go`: campaign use cases and cache policy.
- `internal/storage/postgres/store.go`, `internal/storage/postgres/migrate.go`: PostgreSQL adapter and embedded migration.
- `internal/storage/postgres/store_test.go`: adapter contract tests with a test database.
- `internal/storage/redis/cache.go`, `internal/storage/redis/cache_test.go`: cache adapter and short-lived counters.
- `internal/storage/clickhouse/store.go`, `internal/storage/clickhouse/store_test.go`: event inserts and statistics queries.
- `internal/messaging/kafka.go`, `internal/messaging/kafka_test.go`: producer, consumer, and DLQ topic configuration.
- `internal/processing/retry.go`, `internal/processing/retry_test.go`: retry classification and bounded backoff.
- `internal/processing/offsets.go`, `internal/processing/offsets_test.go`: contiguous offset tracking.
- `internal/processing/pool.go`, `internal/processing/pool_test.go`: bounded worker pool lifecycle.
- `internal/processing/consumer.go`, `internal/processing/consumer_test.go`: fetch, dispatch, processing, DLQ, and commit flow.
- `internal/httpapi/router.go`, `internal/httpapi/handlers.go`, `internal/httpapi/middleware.go`, `internal/httpapi/httpapi_test.go`: HTTP surface, errors, health, rate limiting, and metrics.
- `internal/observability/logging.go`, `internal/observability/metrics.go`: JSON logs and Prometheus collectors.
- `migrations/postgres/001_init.sql`: campaigns, targeting, and users tables.
- `migrations/clickhouse/001_events.sql`: event table and analytics indexes/order.
- `tests/integration/streamforge_test.go`: opt-in end-to-end testcontainers flow.
- `internal/processing/pool_benchmark_test.go`: one concrete benchmark.
- `README.md`: implemented behavior, quick start, examples, architecture, limits, security, and interview notes.

---

### Task 1: Create Go module and safe configuration

**Files:**
- Create: `go.mod`, `.gitignore`, `.dockerignore`, `.env.example`, `.golangci.yml`, `Makefile`
- Create: `internal/config/config.go`
- Test: `internal/config/config_test.go`

**Interfaces:**
- Produces `config.Config` with `Load(env func(string) string) (Config, error)`.
- `Config` contains `HTTPAddr`, `PostgresURL`, `RedisAddr`, `KafkaBrokers []string`, `KafkaTopic`, `KafkaDLQTopic`, `KafkaGroupID`, `ClickHouseURL`, `ClickHouseDatabase`, `WorkerCount`, `QueueCapacity`, `PublishTimeout`, `ProcessTimeout`, `ShutdownTimeout`, `RetryMaxAttempts`, `RetryBaseDelay`, `RateLimitRequests`, and `RateLimitWindow`.
- Later tasks use `config.Load(os.Getenv)` and do not read environment variables directly.

- [ ] **Step 1: Write failing configuration tests**

```go
func TestLoadRejectsMissingRequiredValues(t *testing.T) {
    _, err := Load(func(key string) string { return "" })
    if err == nil || !strings.Contains(err.Error(), "POSTGRES_URL") {
        t.Fatalf("expected missing POSTGRES_URL error, got %v", err)
    }
}

func TestLoadAppliesBoundedDefaults(t *testing.T) {
    env := map[string]string{
        "POSTGRES_URL": "postgres://app:app@postgres:5432/streamforge?sslmode=disable",
        "REDIS_ADDR": "redis:6379",
        "KAFKA_BROKERS": "kafka:9092",
        "CLICKHOUSE_URL": "clickhouse://clickhouse:9000",
    }
    cfg, err := Load(func(key string) string { return env[key] })
    if err != nil { t.Fatal(err) }
    if cfg.WorkerCount < 1 || cfg.QueueCapacity < cfg.WorkerCount { t.Fatalf("invalid defaults: %+v", cfg) }
}

func TestLoadRejectsInvalidConcurrencyAndTimeoutValues(t *testing.T) {
    env := validEnv()
    env["WORKER_COUNT"] = "0"
    if _, err := Load(func(key string) string { return env[key] }); err == nil { t.Fatal("expected worker validation error") }
}
```

- [ ] **Step 2: Run the focused tests and verify failure**

Run: `go test ./internal/config -run 'TestLoad' -count=1`
Expected: FAIL because `Load` and `validEnv` are not implemented.

- [ ] **Step 3: Implement typed parsing and validation**

Implement `Load` with explicit defaults, comma-separated broker parsing, `time.ParseDuration`, positive integer parsing, and validation that `QueueCapacity >= WorkerCount`, retry attempts are between 1 and 10, and all durations are positive. Return errors naming the environment variable and invalid value. Do not log configuration values.

- [ ] **Step 4: Run focused tests and static checks**

Run: `gofmt -w internal/config && go test ./internal/config -count=1 && go vet ./internal/config`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add go.mod .gitignore .dockerignore .env.example .golangci.yml Makefile internal/config
git commit -m "feat(config): add validated runtime configuration"
```

---

### Task 2: Define domain models and event validation

**Files:**
- Create: `internal/domain/campaign.go`, `internal/domain/event.go`
- Create: `internal/events/validate.go`
- Test: `internal/events/validate_test.go`

**Interfaces:**
- `domain.Campaign`, `domain.Targeting`, and `domain.Event` are JSON-safe transport/domain types.
- `events.ValidateEvent(event domain.Event, expectedType domain.EventType) error` validates an event independently of infrastructure.
- `events.ValidateCampaignInput(input domain.CampaignInput) error` validates campaign fields.

- [ ] **Step 1: Write validation tests for valid, invalid, and oversized inputs**

```go
func TestValidateEventRejectsInvalidIdentityAndTimestamp(t *testing.T) {
    event := validEvent()
    event.EventID = "not-a-uuid"
    if err := ValidateEvent(event, domain.EventTypeImpression); err == nil { t.Fatal("expected event ID error") }

    event = validEvent()
    event.OccurredAt = time.Now().Add(10 * time.Minute)
    if err := ValidateEvent(event, domain.EventTypeImpression); err == nil { t.Fatal("expected future timestamp error") }
}

func TestValidateEventRejectsTypeMismatchAndLargeMetadata(t *testing.T) {
    event := validEvent()
    if err := ValidateEvent(event, domain.EventTypeClick); err == nil { t.Fatal("expected type mismatch") }
    event = validEvent()
    event.Metadata = map[string]string{"payload": strings.Repeat("x", 8193)}
    if err := ValidateEvent(event, domain.EventTypeImpression); err == nil { t.Fatal("expected metadata size error") }
}

func TestValidateCampaignInputRequiresPositiveBudgetAndCurrency(t *testing.T) {
    input := domain.CampaignInput{Name: "", DailyBudget: 0, Currency: "US"}
    if err := ValidateCampaignInput(input); err == nil { t.Fatal("expected campaign validation error") }
}
```

- [ ] **Step 2: Run tests and verify failure**

Run: `go test ./internal/events -run 'TestValidate' -count=1`
Expected: FAIL because the domain types and validation functions do not exist.

- [ ] **Step 3: Implement deterministic validation**

Use UUID parsing for event and campaign IDs, UTC timestamps not more than five minutes in the future, non-empty bounded user IDs, a fixed event type enum, metadata size capped at 8 KiB, campaign names capped at 160 bytes, positive budgets, and three-letter uppercase currency codes. Return structured field errors through a small `ValidationError` type.

- [ ] **Step 4: Run focused tests and race check**

Run: `gofmt -w internal/domain internal/events && go test -race ./internal/domain ./internal/events -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/domain internal/events
git commit -m "feat(events): add domain models and validation"
```

---

### Task 3: Add PostgreSQL campaign storage and migrations

**Files:**
- Create: `migrations/postgres/001_init.sql`
- Create: `internal/storage/postgres/migrate.go`, `internal/storage/postgres/store.go`
- Create: `internal/campaigns/service.go`
- Test: `internal/storage/postgres/store_test.go`, `internal/campaigns/service_test.go`

**Interfaces:**
- `campaigns.Repository` exposes `Create(ctx context.Context, input domain.CampaignInput) (domain.Campaign, error)`, `Get(ctx context.Context, id uuid.UUID) (domain.Campaign, error)`, and `EnsureUser(ctx context.Context, id string) error`.
- `postgres.Store` implements `campaigns.Repository` using `pgxpool.Pool` and parameterized SQL.
- `campaigns.Service` exposes `Create` and `Get` and accepts a repository plus cache interface.
- `postgres.ApplyMigration(ctx, pool) error` executes the embedded SQL once during startup.

- [ ] **Step 1: Write repository contract tests**

```go
func TestCampaignRepositoryCreatesAndReadsCampaign(t *testing.T) {
    ctx := context.Background()
    db := startPostgresContainer(t)
    repo := NewStore(db)
    require.NoError(t, ApplyMigration(ctx, db))

    created, err := repo.Create(ctx, domain.CampaignInput{Name: "Spring", DailyBudget: 250, Currency: "USD", Targeting: domain.Targeting{Country: "US", Device: "mobile"}})
    require.NoError(t, err)
    got, err := repo.Get(ctx, created.ID)
    require.NoError(t, err)
    require.Equal(t, created.ID, got.ID)
    require.Equal(t, "Spring", got.Name)
}

func TestCampaignCreateRollsBackTargetingOnFailure(t *testing.T) {
    // Use a database constraint violation in the same transaction and assert no orphan targeting row remains.
}
```

- [ ] **Step 2: Run integration test and verify failure**

Run: `go test ./internal/storage/postgres ./internal/campaigns -run 'TestCampaign' -count=1`
Expected: FAIL because migration, repository, and test container helper are not implemented.

- [ ] **Step 3: Implement the PostgreSQL schema and adapter**

Create `campaigns` with UUID primary key, name, numeric daily budget, currency, active status, and timestamps; `campaign_targeting` with a one-to-one campaign key and country/device fields; and `users` with a text primary key and creation timestamp. Execute campaign and targeting writes inside one transaction. Map `pgx.ErrNoRows` to a repository-level not-found error. Use `INSERT ... ON CONFLICT DO NOTHING` for users.

- [ ] **Step 4: Implement campaign service and run tests**

The service validates input, calls the repository, and invalidates the cache after a successful commit. Reads use the cache interface first and fall back to the repository on misses or cache errors. Run: `gofmt -w migrations internal/storage/postgres internal/campaigns && go test -race ./internal/campaigns -count=1 && go test ./internal/storage/postgres -count=1`
Expected: PASS with PostgreSQL available.

- [ ] **Step 5: Commit**

```bash
git add migrations/postgres internal/storage/postgres internal/campaigns
git commit -m "feat(storage): add transactional campaign repository"
```

---

### Task 4: Add Redis cache and short-lived counters

**Files:**
- Create: `internal/storage/redis/cache.go`
- Test: `internal/storage/redis/cache_test.go`, `internal/campaigns/service_test.go`

**Interfaces:**
- `redis.Cache` exposes `GetCampaign(ctx, id uuid.UUID) (domain.Campaign, bool, error)`, `SetCampaign(ctx, campaign domain.Campaign, ttl time.Duration) error`, `DeleteCampaign(ctx, id uuid.UUID) error`, and `IncrementEvent(ctx, campaignID uuid.UUID, eventType domain.EventType, ttl time.Duration) error`.
- `campaigns.Service` consumes the interface without importing the concrete Redis client.

- [ ] **Step 1: Write cache behavior tests**

```go
func TestCampaignServiceUsesRepositoryWhenCacheMisses(t *testing.T) {
    repo := fakeCampaignRepository{campaign: validCampaign()}
    cache := fakeCampaignCache{found: false}
    service := NewService(repo, cache, time.Minute)
    got, err := service.Get(context.Background(), repo.campaign.ID)
    require.NoError(t, err)
    require.Equal(t, repo.campaign.ID, got.ID)
    require.Equal(t, 1, repo.getCalls)
    require.Equal(t, 1, cache.setCalls)
}

func TestCampaignServiceTreatsCacheFailureAsMiss(t *testing.T) {
    repo := fakeCampaignRepository{campaign: validCampaign()}
    cache := fakeCampaignCache{getErr: errors.New("redis unavailable")}
    _, err := NewService(repo, cache, time.Minute).Get(context.Background(), repo.campaign.ID)
    require.NoError(t, err)
}
```

- [ ] **Step 2: Run focused tests and verify failure**

Run: `go test ./internal/storage/redis ./internal/campaigns -run 'Cache' -count=1`
Expected: FAIL because the cache interface and service cache path are incomplete.

- [ ] **Step 3: Implement Redis adapter and cache policy**

Use JSON serialization with a key prefix `streamforge:campaign:` and a configured TTL. Treat `redis.Nil` as a cache miss. Log/cache metrics are emitted by the caller, not by the adapter. Counter keys include campaign ID and event type and expire automatically. Never store credentials or unrestricted metadata in Redis.

- [ ] **Step 4: Run focused tests and race check**

Run: `gofmt -w internal/storage/redis internal/campaigns && go test -race ./internal/storage/redis ./internal/campaigns -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/redis internal/campaigns
git commit -m "feat(cache): add Redis campaign cache and counters"
```

---

### Task 5: Add Kafka event producer and HTTP campaign/event API

**Files:**
- Create: `internal/messaging/kafka.go`, `internal/messaging/kafka_test.go`
- Create: `internal/httpapi/router.go`, `internal/httpapi/handlers.go`, `internal/httpapi/middleware.go`
- Test: `internal/httpapi/httpapi_test.go`

**Interfaces:**
- `messaging.EventProducer` exposes `Publish(ctx context.Context, event domain.Event) error`.
- `httpapi.Dependencies` contains campaign service, event producer, health checker, metrics, and rate-limit configuration.
- `httpapi.NewRouter(deps Dependencies) http.Handler` returns the complete HTTP handler.
- Event handlers return `202 Accepted` only after `EventProducer.Publish` succeeds.

- [ ] **Step 1: Write HTTP contract tests**

```go
func TestCreateCampaignReturnsCreatedCampaign(t *testing.T) {
    req := httptest.NewRequest(http.MethodPost, "/v1/campaigns", strings.NewReader(`{"name":"Spring","daily_budget":250,"currency":"USD"}`))
    req.Header.Set("Content-Type", "application/json")
    rec := httptest.NewRecorder()
    NewRouter(testDependencies()).ServeHTTP(rec, req)
    require.Equal(t, http.StatusCreated, rec.Code)
}

func TestEventEndpointReturnsAcceptedOnlyAfterPublish(t *testing.T) {
    req := httptest.NewRequest(http.MethodPost, "/v1/events/impression", strings.NewReader(validEventJSON()))
    rec := httptest.NewRecorder()
    NewRouter(testDependencies()).ServeHTTP(rec, req)
    require.Equal(t, http.StatusAccepted, rec.Code)
}

func TestEventEndpointMapsPublishFailureToServiceUnavailable(t *testing.T) {
    deps := testDependencies()
    deps.Producer = failingProducer{err: context.DeadlineExceeded}
    req := httptest.NewRequest(http.MethodPost, "/v1/events/impression", strings.NewReader(validEventJSON()))
    rec := httptest.NewRecorder()
    NewRouter(deps).ServeHTTP(rec, req)
    require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
```

- [ ] **Step 2: Run API tests and verify failure**

Run: `go test ./internal/httpapi ./internal/messaging -run 'Test(CreateCampaign|EventEndpoint)' -count=1`
Expected: FAIL because routes, producer, and error mapping are not implemented.

- [ ] **Step 3: Implement Kafka producer**

Create a `kafka.Writer` with explicit broker list, topic, batch timeout, and write timeout. Serialize a versioned JSON envelope containing event ID, type, campaign ID, user ID, occurrence time, metadata, and schema version. Use the caller context and never retry indefinitely. Add a separate writer for the DLQ topic with the same envelope plus failure metadata.

- [ ] **Step 4: Implement HTTP routes and structured errors**

Use `chi` routes for the five API groups. Decode JSON with a bounded body reader, reject unknown oversized payloads, validate domain input, map validation to `400`, not-found to `404`, conflict to `409`, dependency timeout/unavailability to `503`, and unexpected failures to `500` with a stable JSON error shape `{code,message,request_id}`. Generate a request ID when the client does not provide one and include it in JSON logs.

- [ ] **Step 5: Run tests and commit**

Run: `gofmt -w internal/messaging internal/httpapi && go test -race ./internal/messaging ./internal/httpapi -count=1`
Expected: PASS.

```bash
git add internal/messaging internal/httpapi
git commit -m "feat(api): add Kafka-backed campaign and event endpoints"
```

---

### Task 6: Add ClickHouse event storage and statistics

**Files:**
- Create: `migrations/clickhouse/001_events.sql`
- Create: `internal/storage/clickhouse/store.go`
- Test: `internal/storage/clickhouse/store_test.go`, `internal/httpapi/httpapi_test.go`

**Interfaces:**
- `analytics.Store` exposes `InsertEvent(ctx context.Context, event domain.Event, processedAt time.Time, attempt int) error` and `CampaignStats(ctx context.Context, campaignID uuid.UUID, from, to time.Time) (domain.CampaignStats, error)`.
- `clickhouse.Store` implements `analytics.Store`.

- [ ] **Step 1: Write statistics and idempotency tests**

```go
func TestCampaignStatsRejectsInvertedOrExcessiveRanges(t *testing.T) {
    deps := testDependencies()
    for _, query := range []string{
        "/v1/stats/campaign/7a4b3b5d-40ca-4d43-b3c4-2ed4bd4bced8?from=2026-08-31T02:00:00Z&to=2026-08-31T01:00:00Z",
        "/v1/stats/campaign/7a4b3b5d-40ca-4d43-b3c4-2ed4bd4bced8?from=2025-01-01T00:00:00Z&to=2026-08-31T01:00:00Z",
    } {
        rec := httptest.NewRecorder()
        NewRouter(deps).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, query, nil))
        require.Equal(t, http.StatusBadRequest, rec.Code)
    }
}

func TestClickHouseInsertUsesStableEventIdentity(t *testing.T) {
    // Insert the same event twice in an integration container and assert the deduplicating query returns one logical event.
}
```

- [ ] **Step 2: Run tests and verify failure**

Run: `go test ./internal/storage/clickhouse ./internal/httpapi -run 'Test(CampaignStats|ClickHouse)' -count=1`
Expected: FAIL because the ClickHouse schema, adapter, and statistics handler are not implemented.

- [ ] **Step 3: Implement ClickHouse schema and adapter**

Create an `events` table with event ID, event type, campaign ID, user ID, occurred-at timestamp, metadata JSON/string, processed-at timestamp, and processing attempt. Use an engine/order key that supports append-oriented writes and event ID deduplication queries. Bind all statistics parameters, aggregate impression/click counts, calculate click-through rate as zero when impressions are zero, and cap query windows at 31 days.

- [ ] **Step 4: Wire stats endpoint and run focused tests**

Use strict RFC3339 parsing, require both `from` and `to` together when either is present, default to the last 24 hours, and return a stable response with counts and rate. Run: `gofmt -w migrations/clickhouse internal/storage/clickhouse internal/httpapi && go test -race ./internal/storage/clickhouse ./internal/httpapi -count=1`
Expected: PASS with ClickHouse available for integration cases.

- [ ] **Step 5: Commit**

```bash
git add migrations/clickhouse internal/storage/clickhouse internal/httpapi
git commit -m "feat(analytics): add ClickHouse event storage and stats"
```

---

### Task 7: Implement retry policy and contiguous offset coordinator

**Files:**
- Create: `internal/processing/retry.go`, `internal/processing/offsets.go`
- Test: `internal/processing/retry_test.go`, `internal/processing/offsets_test.go`

**Interfaces:**
- `processing.RetryPolicy` exposes `Run(ctx context.Context, operation func(context.Context) error) (attempts int, err error)`.
- `processing.OffsetCoordinator` exposes `Observe(partition int, offset int64)`, `Complete(partition int, offset int64) (commitOffset int64, ok bool)`, and `Pending(partition int) int`.
- `Complete` returns the inclusive last contiguous offset; Kafka commit code will convert it to the next offset as required by `CommitMessages`.

- [ ] **Step 1: Write retry tests**

```go
func TestRetryStopsAtConfiguredAttemptLimit(t *testing.T) {
    calls := 0
    policy := RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, Classify: transientError}
    _, err := policy.Run(context.Background(), func(context.Context) error { calls++; return errTransient })
    require.ErrorIs(t, err, errTransient)
    require.Equal(t, 3, calls)
}

func TestRetryStopsImmediatelyOnPermanentError(t *testing.T) {
    calls := 0
    policy := RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, Classify: transientError}
    _, err := policy.Run(context.Background(), func(context.Context) error { calls++; return errPermanent })
    require.ErrorIs(t, err, errPermanent)
    require.Equal(t, 1, calls)
}
```

- [ ] **Step 2: Write offset ordering tests**

```go
func TestOffsetCoordinatorCommitsOnlyContiguousCompletions(t *testing.T) {
    c := NewOffsetCoordinator()
    c.Observe(0, 10); c.Observe(0, 11); c.Observe(0, 12)
    require.False(t, completeWithoutCommit(c, 0, 11))
    offset, ok := c.Complete(0, 10)
    require.True(t, ok); require.Equal(t, int64(11), offset)
    offset, ok = c.Complete(0, 11)
    require.True(t, ok); require.Equal(t, int64(12), offset)
    _, ok = c.Complete(0, 12)
    require.True(t, ok)
}
```

- [ ] **Step 3: Run focused tests and verify failure**

Run: `go test ./internal/processing -run 'Test(Retry|Offset)' -count=1`
Expected: FAIL because retry and offset implementations do not exist.

- [ ] **Step 4: Implement cancellation-safe retry and mutex-protected offsets**

Retry only errors classified transient, cap attempts, compute bounded exponential delays, and select every delay on `ctx.Done()` so cancellation interrupts sleep. Protect partition state with a mutex. `Observe` initializes the next expected offset to the lowest observed offset for a new partition. `Complete` records completion, advances through all adjacent completions, and returns the last advanced offset without exposing the internal map.

- [ ] **Step 5: Run race tests and commit**

Run: `gofmt -w internal/processing && go test -race ./internal/processing -count=1`
Expected: PASS.

```bash
git add internal/processing/retry.go internal/processing/retry_test.go internal/processing/offsets.go internal/processing/offsets_test.go
git commit -m "feat(processing): add retries and contiguous offset tracking"
```

---

### Task 8: Implement bounded worker pool, Kafka consumer, and DLQ

**Files:**
- Create: `internal/processing/pool.go`, `internal/processing/consumer.go`
- Test: `internal/processing/pool_test.go`, `internal/processing/consumer_test.go`

**Interfaces:**
- `processing.Job` contains `kafka.Message` and decoded `domain.Event`.
- `processing.Handler` is `func(context.Context, Job) error`.
- `processing.Pool` exposes `Run(ctx context.Context)`, `Submit(ctx context.Context, job Job) error`, `Results() <-chan Result`, and `CloseInput()`.
- `processing.Consumer` exposes `Run(ctx context.Context) error` and owns fetch, bounded submission, workers, DLQ publishing, and serialized Kafka commits.
- `processing.Result` contains source message, terminal status, attempt count, and processing error metadata.

- [ ] **Step 1: Write pool backpressure and cancellation tests**

```go
func TestPoolBlocksSubmitWhenQueueIsFull(t *testing.T) {
    entered := make(chan struct{})
    release := make(chan struct{})
    pool := NewPool(1, 1, func(context.Context, Job) error { close(entered); <-release; return nil })
    ctx, cancel := context.WithCancel(context.Background()); defer cancel()
    go pool.Run(ctx)
    require.NoError(t, pool.Submit(ctx, Job{}))
    <-entered
    require.NoError(t, pool.Submit(ctx, Job{}))
    blocked := make(chan error, 1)
    go func() { blocked <- pool.Submit(ctx, Job{}) }()
    select { case <-blocked: t.Fatal("third job bypassed bounded queue"); case <-time.After(20 * time.Millisecond): }
    close(release)
    require.NoError(t, <-blocked)
}

func TestPoolStopsSubmissionOnCancellation(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    pool := NewPool(1, 1, func(context.Context, Job) error { return nil })
    go pool.Run(ctx)
    cancel()
    require.ErrorIs(t, pool.Submit(context.Background(), Job{}), context.Canceled)
}
```

- [ ] **Step 2: Write consumer outcome tests**

```go
func TestConsumerCommitsAfterSuccessfulProcessing(t *testing.T) {
    broker := fakeKafkaReader{messages: []kafka.Message{{Partition: 0, Offset: 4}}}
    deps := consumerDependencies{reader: broker, analytics: successfulAnalytics{}, committer: fakeCommitter{}}
    require.NoError(t, NewConsumer(deps).Run(cancelAfterOneMessage()))
    require.Equal(t, int64(4), deps.committer.last.Offset)
}

func TestConsumerPublishesDLQAfterFinalRetry(t *testing.T) {
    deps := consumerDependencies{reader: oneMessageReader(), analytics: alwaysFailingAnalytics{}, dlq: fakeDLQ{}, retry: RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond}}
    require.NoError(t, NewConsumer(deps).Run(cancelAfterOneMessage()))
    require.Len(t, deps.dlq.messages, 1)
}
```

- [ ] **Step 3: Run tests and verify failure**

Run: `go test ./internal/processing -run 'Test(Pool|Consumer)' -count=1`
Expected: FAIL because the pool and consumer lifecycle are not implemented.

- [ ] **Step 4: Implement pool ownership and bounded dispatch**

Create exactly `WorkerCount` worker goroutines. The pool owns and closes the jobs channel only after `CloseInput`; callers never close it. `Submit` selects between the bounded jobs channel and the caller context. Each worker reports one result per job. `Run` waits for workers and closes the results channel once. No worker writes shared mutable state outside the result channel.

- [ ] **Step 5: Implement consumer fetch, process, DLQ, and commit ordering**

Fetch with `FetchMessage(ctx)`, decode and validate the envelope, call `Observe` before submission, and stop fetching when submission blocks on the bounded channel. Workers call analytics through the retry policy. Terminal failures publish a DLQ envelope containing original topic/partition/offset, event ID, error class, and attempts; a DLQ publish must succeed before the source result is considered complete. A single commit loop consumes results, calls `Complete`, and invokes `CommitMessages` for each newly contiguous range. On cancellation, stop fetch, close input, allow workers to drain until the configured shutdown context expires, and never commit unfinished jobs.

- [ ] **Step 6: Run race tests and commit**

Run: `gofmt -w internal/processing && go test -race ./internal/processing -count=1`
Expected: PASS.

```bash
git add internal/processing/pool.go internal/processing/pool_test.go internal/processing/consumer.go internal/processing/consumer_test.go
git commit -m "feat(processing): add bounded Kafka worker pool and DLQ"
```

---

### Task 9: Add observability, health checks, rate limiting, and application lifecycle

**Files:**
- Create: `internal/observability/logging.go`, `internal/observability/metrics.go`
- Modify: `internal/httpapi/router.go`, `internal/httpapi/handlers.go`, `internal/httpapi/middleware.go`
- Modify: `cmd/streamforge/main.go`
- Test: `internal/httpapi/httpapi_test.go`, `internal/observability/metrics_test.go`

**Interfaces:**
- `observability.Metrics` records HTTP, Kafka, queue, worker, retry, DLQ, and shutdown-drain outcomes.
- `httpapi.HealthChecker` exposes `Check(ctx context.Context) map[string]string`.
- `httpapi.NewRateLimiter(limit int, window time.Duration) Middleware` provides a race-safe per-client-IP fixed-window limiter.

- [ ] **Step 1: Write rate-limit and health contract tests**

```go
func TestRateLimiterRejectsRequestsAfterConfiguredWindowCount(t *testing.T) {
    middleware := NewRateLimiter(1, time.Minute)
    handler := middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
    for i := 0; i < 2; i++ {
        rec := httptest.NewRecorder()
        req := httptest.NewRequest(http.MethodGet, "/health", nil)
        req.RemoteAddr = "192.0.2.10:1234"
        handler.ServeHTTP(rec, req)
        if i == 0 { require.Equal(t, http.StatusOK, rec.Code) } else { require.Equal(t, http.StatusTooManyRequests, rec.Code) }
    }
}

func TestHealthEndpointReportsDependencyState(t *testing.T) {
    deps := testDependencies()
    deps.Health = fakeHealth{status: map[string]string{"postgres": "ok", "redis": "degraded"}}
    rec := httptest.NewRecorder()
    NewRouter(deps).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
    require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
```

- [ ] **Step 2: Run focused tests and verify failure**

Run: `go test ./internal/httpapi ./internal/observability -run 'Test(RateLimiter|Health|Metrics)' -count=1`
Expected: FAIL because metrics, health mapping, and rate limiting are not implemented.

- [ ] **Step 3: Implement structured logging and Prometheus metrics**

Use `log/slog` with JSON output and fields for request ID, route, status, duration, event ID, campaign ID, attempt, and error class only where safe. Register counters/histograms/gauges once and expose them through `promhttp.Handler()`. Queue depth is set from the bounded channel length; no user metadata is included in labels.

- [ ] **Step 4: Implement health/readiness and rate limiting**

`GET /health` returns liveness and dependency statuses. Return `200` when all readiness checks are healthy and `503` when any dependency is unavailable, while keeping error details limited to component names and states. Use a mutex around per-IP window state, bound the map by periodic expiry cleanup, and document that this limiter is process-local.

- [ ] **Step 5: Implement main lifecycle**

Load config, create clients, apply PostgreSQL and ClickHouse initialization, create Kafka writers/readers, build router, start HTTP and consumer goroutines, handle SIGINT/SIGTERM, stop accepting HTTP work, cancel fetching, drain workers under `ShutdownTimeout`, close writers/readers/pools, and return the first startup/runtime error. Use `http.Server.Shutdown` with a deadline and do not use `os.Exit` before deferred cleanup.

- [ ] **Step 6: Run race tests and commit**

Run: `gofmt -w cmd internal/observability internal/httpapi && go test -race ./internal/httpapi ./internal/observability ./cmd/streamforge -count=1`
Expected: PASS.

```bash
git add cmd internal/observability internal/httpapi
git commit -m "feat(ops): add metrics health checks and graceful shutdown"
```

---

### Task 10: Add Docker Compose and local initialization

**Files:**
- Create: `Dockerfile`, `docker-compose.yml`, `scripts/kafka-init.sh`
- Modify: `.env.example`, `Makefile`

**Interfaces:**
- `docker compose up --build` starts the API and all required dependencies without manual database commands.
- The API container listens on `:8080`; PostgreSQL, Redis, Kafka, and ClickHouse expose only documented local development ports.

- [ ] **Step 1: Define health checks and service dependencies**

Configure PostgreSQL, Redis, Kafka, and ClickHouse health checks. Configure the API to depend on healthy services and pass connection settings through environment variables. Use a pinned Kafka-compatible broker image and state the exact image choice in the README; do not silently call a different protocol implementation Apache Kafka.

- [ ] **Step 2: Build the non-root API image**

Use a Go builder stage with `CGO_ENABLED=0`, copy the static binary and embedded migrations into a minimal runtime image, create an unprivileged user, set `USER`, and define `ENTRYPOINT ["/streamforge"]`. Ensure `.dockerignore` excludes `.git`, local environment files, test output, and IDE files.

- [ ] **Step 3: Add broker topic initialization**

Create the event and DLQ topics idempotently with the broker CLI after Kafka health is ready. Use explicit partition and replication settings suitable for a single-node local stack and document that they are not production cluster settings.

- [ ] **Step 4: Exercise the stack before committing**

Run: `docker compose config` and `docker compose up --build -d`; wait for health checks; run `curl.exe http://localhost:8080/health`; run `docker compose down -v`.
Expected: configuration succeeds, all containers become healthy, health returns a JSON dependency report, and teardown removes the local stack.

- [ ] **Step 5: Commit**

```bash
git add Dockerfile docker-compose.yml scripts/kafka-init.sh .env.example Makefile
git commit -m "feat(infra): add reproducible StreamForge Compose stack"
```

---

### Task 11: Add testcontainers integration tests and benchmark

**Files:**
- Create: `tests/integration/streamforge_test.go`
- Create: `internal/processing/pool_benchmark_test.go`
- Modify: `go.mod`, `Makefile`

**Interfaces:**
- Integration tests run only with `STREAMFORGE_INTEGRATION=1` and require Docker; ordinary `go test ./...` never starts containers.
- `internal/processing` benchmark measures `Pool.Submit` plus a no-op handler under a declared worker/queue configuration.

- [ ] **Step 1: Write opt-in integration test skeleton with real assertions**

```go
func TestComposeContractEndToEnd(t *testing.T) {
    if os.Getenv("STREAMFORGE_INTEGRATION") != "1" { t.Skip("set STREAMFORGE_INTEGRATION=1") }
    ctx := context.Background()
    stack := startDependenciesWithTestcontainers(t, ctx)
    client := newAPIClient(t, stack.APIURL)
    campaign := client.CreateCampaign(t, domain.CampaignInput{Name: "Integration", DailyBudget: 100, Currency: "USD"})
    client.PostImpression(t, campaign.ID, uuid.New(), "integration-user")
    require.Eventually(t, func() bool { return client.Stats(t, campaign.ID).Impressions == 1 }, 10*time.Second, 250*time.Millisecond)
}
```

- [ ] **Step 2: Run the integration test and verify the expected environment failure/success**

Run: `STREAMFORGE_INTEGRATION=1 go test ./tests/integration -run TestComposeContractEndToEnd -count=1`
Expected with Docker running: PASS; expected with Docker unavailable: fail with a clear container-runtime error, not a skipped test.

- [ ] **Step 3: Implement testcontainers setup and cleanup**

Start PostgreSQL, Redis, Kafka, and ClickHouse using pinned images, wait for each readiness command, create topics, apply migrations, and run the API against the dynamically mapped ports. Use `t.Cleanup` for every container and set short test timeouts. Assert campaign transaction behavior, HTTP acceptance, eventual ClickHouse stats, health, metrics, and at least one DLQ failure path.

- [ ] **Step 4: Add a non-fake benchmark**

Benchmark a concrete operation with `b.ReportAllocs()`, `b.ResetTimer()`, and a bounded pool. Include benchmark configuration in the benchmark name or README command. Do not write a throughput number into documentation until the command is actually run on the current machine.

- [ ] **Step 5: Run race/unit/integration targets and commit**

Run: `go test -race ./... -count=1` and `STREAMFORGE_INTEGRATION=1 go test ./tests/integration -count=1`
Expected: unit/race tests pass; integration tests pass only when Docker is available.

```bash
git add tests/integration internal/processing/pool_benchmark_test.go go.mod Makefile
git commit -m "test: add StreamForge integration coverage and benchmark"
```

---

### Task 12: Add CI and professional README

**Files:**
- Create: `.github/workflows/ci.yml`, `README.md`
- Modify: `Makefile`, `.golangci.yml`

**Interfaces:**
- CI runs format check, lint, unit tests, race tests, integration tests, build, and Compose smoke in separate named steps.
- README commands and feature claims must match executable code and checked-in configuration.

- [ ] **Step 1: Add CI workflow**

Use a Go 1.24-or-newer matrix entry, cache Go modules, run `gofmt -l` as a failure condition, `go vet`, golangci-lint, `go test -race ./...`, the opt-in testcontainers suite, `go build ./cmd/streamforge`, and Compose smoke. Upload logs only on failure. Do not put credentials in workflow YAML; use service-local development values.

- [ ] **Step 2: Write README sections from verified behavior**

Include:

1. concise project summary and implemented functionality;
2. architecture Mermaid diagram;
3. Quick Start with `docker compose up --build`;
4. `curl` examples for campaign creation, event ingestion, stats, health, and metrics;
5. PostgreSQL vs Redis vs Kafka vs ClickHouse rationale;
6. goroutine vs OS thread, channel ownership, context cancellation, race protection, bounded backpressure, and contiguous offset commits;
7. retry, DLQ, at-least-once and idempotent-storage semantics;
8. test commands and benchmark command without invented results;
9. security considerations;
10. explicit limitations;
11. “What I would improve for production”;
12. interview questions the author should be ready to answer.

- [ ] **Step 3: Reconcile every README claim**

For each endpoint, environment variable, command, metric, and component named in README, point to the implementation or Compose file. Remove any wording that describes a feature not exercised by code or tests. State exact broker image and local-only assumptions.

- [ ] **Step 4: Run documentation and repository checks**

Run: `go test -race ./... -count=1`, `go vet ./...`, `golangci-lint run`, `go build ./cmd/streamforge`, and a README command smoke after Docker is available.
Expected: all checks pass; any unavailable Docker check is reported as blocked rather than marked successful.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml README.md Makefile .golangci.yml
git commit -m "docs(ci): document and automate StreamForge verification"
```

---

### Task 13: Perform security review, full verification, and clean repository

**Files:**
- Review all tracked source and configuration files.
- Modify only files with concrete review findings.

**Interfaces:**
- Deliverable is a verified repository state with no unfinished implementation.
- The evidence packet records exact commands and outcomes for tests, build, Compose, secret scan, and README reconciliation.

- [ ] **Step 1: Scan tracked content and history for secrets**

Run: `git grep -n -I -E '(AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9_]{20,}|BEGIN (RSA|OPENSSH|EC|DSA) PRIVATE KEY|password\s*=\s*[^$<{][^[:space:]]+)' -- . ':!go.sum'`
Expected: no matches outside clearly documented local Compose development credentials such as `app:app`.

- [ ] **Step 2: Run the complete verification matrix**

Run: `gofmt -l .`, `go vet ./...`, `golangci-lint run`, `go test -race ./... -count=1`, `go test ./internal/processing -bench . -benchmem -count=1`, `go build ./cmd/streamforge`, and `STREAMFORGE_INTEGRATION=1 go test ./tests/integration -count=1`.
Expected: formatting output is empty; static analysis, race tests, build, and integration tests pass. Record benchmark output with machine and Go version only if the benchmark completes.

- [ ] **Step 3: Run the real Compose smoke scenario**

Run: `docker compose up --build -d`; wait for health; create a campaign; post one impression and one click with `curl.exe`; poll campaign stats until both counts are visible; fetch `/metrics`; inspect API logs for JSON fields; stop with `docker compose down -v`.
Expected: campaign is created, both events are accepted and appear in stats, health is ready, metrics are non-empty, logs are structured, and teardown succeeds.

- [ ] **Step 4: Review changes and clean generated state**

Run: `git status --short`, `git diff --check`, and `git log --oneline --decorate -n 20`. Remove only generated local artifacts created by this project, keep all source/config/docs, and ensure `.env` is ignored and absent from the index.

- [ ] **Step 5: Commit concrete fixes and tag verification**

If review found a defect, commit it with a focused `fix:` message and rerun the affected verification. Then create the final local verification commit only when the tree is clean:

```bash
git add -A
git commit -m "fix: close verification findings"
```

Do not create a release tag until CI and Docker evidence are available.

---

## Plan Self-Review

- Configuration, domain validation, PostgreSQL, Redis, Kafka, ClickHouse, worker pool, retries, DLQ, context cancellation, graceful shutdown, health, metrics, rate limiting, integration tests, benchmark, Docker Compose, CI, README, security scan, and final verification each have an explicit task.
- No throughput claim is introduced without a completed benchmark command and recorded machine configuration.
- The pool and offset coordinator tests exercise backpressure, cancellation, concurrency ordering, and race detection.
- The integration test is opt-in and fails clearly when Docker is unavailable; it is not a fake substitute.
- No task relies on an undefined later symbol: repository, producer, analytics, pool, consumer, health, and metrics interfaces are introduced before consumers.
- The only intentional local credentials are non-secret Compose development values and are excluded from claims about production security.
