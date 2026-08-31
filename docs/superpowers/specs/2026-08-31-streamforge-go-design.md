# StreamForge Go Design Specification

## Goal

StreamForge is a small advertising-event ingestion and analytics system that demonstrates Go concurrency, Kafka event processing, PostgreSQL transactional storage, Redis short-lived state, and ClickHouse analytics without claiming unmeasured throughput.

## Scope

The repository is an independently runnable Go service stack. It exposes campaign management, impression/click ingestion, campaign statistics, health, and Prometheus metrics. HTTP ingestion validates the request and publishes an event to Kafka; a background consumer processes events into ClickHouse with bounded concurrency, retries, and a dead-letter topic.

The first version is intentionally single-node Compose infrastructure. It demonstrates delivery and cancellation mechanics, not production cluster operations, multi-region deployment, or ad-bidding algorithms.

## Architecture

```mermaid
flowchart LR
    Client[HTTP client] --> API[Go API]
    API --> PG[(PostgreSQL)]
    API --> Redis[(Redis cache/counters)]
    API --> Kafka[(Kafka)]
    Kafka --> Consumer[Consumer goroutine]
    Consumer --> Queue[Bounded job channel]
    Queue --> Pool[Worker pool]
    Pool --> Retry[Retry policy]
    Retry --> CH[(ClickHouse)]
    Retry --> DLQ[(Kafka DLQ)]
    API --> Metrics[/metrics]
    Pool --> Metrics
```

### Component boundaries

- `internal/config` loads environment variables, applies defaults, and rejects invalid durations, worker counts, or missing connection settings.
- `internal/httpapi` owns HTTP routing, request decoding, validation, response mapping, and request-scoped context deadlines.
- `internal/campaigns` owns campaign creation and reads from PostgreSQL, with Redis caching for campaign lookups.
- `internal/events` defines the event envelope, validates event payloads, and contains producer/consumer interfaces.
- `internal/processing` owns the bounded worker pool, retry/backoff policy, cancellation, and contiguous Kafka offset acknowledgement.
- `internal/storage/postgres` owns PostgreSQL schema access and transactions.
- `internal/storage/clickhouse` owns event inserts and statistics queries.
- `internal/storage/redis` owns cache and short-lived counters.
- `internal/observability` owns structured logging and Prometheus collectors.
- `cmd/streamforge` wires dependencies, starts the HTTP server and consumer, and performs graceful shutdown.

Dependencies point inward through small interfaces. Business logic does not import HTTP or concrete infrastructure clients, which keeps the concurrency and validation tests deterministic.

## Runtime flow

### Campaign creation

`POST /v1/campaigns` validates name, daily budget, currency, and optional targeting fields. The service inserts the campaign and targeting data in one PostgreSQL transaction, invalidates any previous Redis key, and returns the stored campaign with a generated UUID.

### Campaign lookup

`GET /v1/campaigns/:id` reads a Redis cache entry first. A miss reads PostgreSQL, stores a bounded-TTL JSON representation, and returns the result. A Redis failure does not make PostgreSQL data unavailable; it is logged and treated as a cache miss.

### Event ingestion

`POST /v1/events/impression` and `POST /v1/events/click` accept an event ID, campaign ID, user ID, event timestamp, and optional metadata. The service validates IDs, timestamps, event type, and payload size, verifies the campaign in PostgreSQL/cache, then publishes an envelope to Kafka using the request context and a configured timeout. A successful response means Kafka accepted the message, not that ClickHouse has already indexed it.

The event ID is preserved through every stage. ClickHouse uses a ReplacingMergeTree-compatible schema keyed by event ID and event type so redelivery is observable and query results can deduplicate by the latest version. The service does not claim exactly-once delivery; it implements at-least-once processing with idempotent storage semantics.

### Event processing

The consumer fetches messages with `FetchMessage`, places jobs into a bounded channel, and pauses fetching when the channel is full. A fixed worker pool handles jobs concurrently. Each worker creates a child context with the processing timeout and attempts the ClickHouse insert according to the retry policy. Retryable errors use bounded exponential backoff; cancellation stops waiting immediately.

A successful job is reported to an offset coordinator. The coordinator tracks completed offsets per Kafka partition and commits only the largest contiguous completed range. This prevents a later-completing job from committing past an earlier job that is still running. After the final retry, the original envelope and failure metadata are published to the DLQ topic. The DLQ publish also has a timeout; if it fails, the message remains uncommitted and can be replayed after restart.

On shutdown, the application cancels the root context, stops accepting HTTP work, stops fetching new Kafka messages, closes the job channel, waits for workers within the shutdown deadline, flushes the offset coordinator, and closes infrastructure clients. A forced timeout returns without pretending that unfinished work was committed.

## API contract

### `POST /v1/campaigns`

Request:

```json
{
  "name": "Spring campaign",
  "daily_budget": 250.00,
  "currency": "USD",
  "targeting": {"country": "US", "device": "mobile"}
}
```

Returns `201` with the stored campaign. Invalid JSON or business fields return `400`; database conflicts return `409`; unavailable dependencies return `503`.

### `GET /v1/campaigns/:id`

Returns `200` with the campaign, `404` when it does not exist, and `400` for an invalid UUID.

### `POST /v1/events/impression` and `POST /v1/events/click`

Request:

```json
{
  "event_id": "4a4b3b5d-40ca-4d43-b3c4-2ed4bd4bced8",
  "campaign_id": "7a4b3b5d-40ca-4d43-b3c4-2ed4bd4bced8",
  "user_id": "user-42",
  "occurred_at": "2026-08-31T07:00:00Z",
  "metadata": {"placement": "homepage"}
}
```

Returns `202` after Kafka accepts the message. Reusing the same event ID is safe for storage but does not claim that the broker suppresses duplicate messages.

### `GET /v1/stats/campaign/:id`

Returns ClickHouse aggregates for the campaign, including impression count, click count, and click-through rate. It accepts bounded `from` and `to` timestamps and rejects an inverted or excessively large range.

### `GET /health`

Returns dependency-aware status. Liveness is available without querying every dependency; readiness reports PostgreSQL, Redis, Kafka producer, and ClickHouse state separately.

### `GET /metrics`

Exposes request counters/latencies, Kafka publish failures, queue depth, worker processing results, retries, DLQ publishes, and shutdown-drain outcomes.

## Data stores

PostgreSQL is the source of truth for transactional entities because campaign creation, budgets, and targeting require transactions, constraints, and predictable point reads. Redis is a disposable acceleration layer for campaign cache and short-lived counters; invalidation and expiry are explicit. Kafka provides a durable boundary that absorbs bursts and decouples client latency from analytics writes. ClickHouse is used for append-heavy analytical reads over event data and is not used for campaign authorization or budget mutation.

The schema includes:

- PostgreSQL: `campaigns`, `campaign_targeting`, `users`.
- ClickHouse: `events` with event type, campaign ID, user ID, event time, metadata, event ID, processing version, and ingestion time.
- Redis: campaign cache keys with TTL and event counters with bounded expiry.

## Concurrency invariants

- Every goroutine has an owner and a cancellation path.
- The job channel has a configured finite capacity; no unbounded in-memory queue is allowed.
- Shared offset state is protected by a mutex and is never accessed without the coordinator API.
- Workers do not mutate shared request state.
- Kafka commits represent only contiguous completed offsets.
- Retry timers are stopped or released when contexts are cancelled.
- Shutdown does not close a channel that another goroutine may still send to.

The README will explain that a goroutine is a runtime-managed, initially small stack of execution state, while an OS thread is a kernel-scheduled execution resource. Channels make ownership and handoff explicit; they do not automatically make arbitrary shared memory safe. Mutexes protect offset tracking and lifecycle state where ownership transfer alone is insufficient.

## Failure handling

- Request deadlines propagate to PostgreSQL, Redis, Kafka, and ClickHouse calls.
- Kafka publish and ClickHouse writes use bounded retries only for classified transient failures.
- Permanent validation errors are returned to the caller or counted as rejected events; they are not retried.
- Processing failures after the final retry are written to a Kafka DLQ with the source topic, partition, offset, event ID, error class, and attempt count.
- Redis failures degrade to uncached PostgreSQL reads.
- ClickHouse unavailability creates backpressure rather than an unbounded memory queue.
- The consumer uses at-least-once processing. Idempotent event storage and deduplicating stats queries reduce duplicate effects.

## Security considerations

- Secrets are read only from environment variables; the repository contains `.env.example` values with no credentials.
- Request bodies, metadata size, event timestamps, IDs, and campaign fields are bounded.
- SQL uses parameterized queries.
- Redis and database failures do not leak connection strings or raw driver errors in HTTP responses.
- Metrics and logs do not include secret values or unrestricted user metadata.
- The Compose setup binds services to the internal network by default; only the API and explicitly documented local ports are exposed.
- HTTP event ingestion includes a configurable per-client rate limit to prevent a single client from exhausting the bounded queue.
- Production deployment would require authentication, TLS termination, broker ACLs, network policy, secret management, and durable DLQ retention; these are documented limitations of the local portfolio stack.

## Testing strategy

- Unit tests cover config validation, event validation, retry classification/backoff, offset coordinator ordering, queue backpressure, cancellation, and worker shutdown.
- PostgreSQL, Redis, Kafka, and ClickHouse integration tests use testcontainers-go and are enabled explicitly by an integration test flag.
- HTTP integration tests exercise campaign creation, cache miss/hit behavior, event acceptance, statistics validation, and structured error responses.
- `go test -race ./...` is required for the concurrency packages and the complete unit suite.
- A standard Go benchmark measures one concrete dispatcher/worker operation. Results include the Go version and machine configuration and are not converted into unsupported production throughput claims.
- Compose smoke testing creates a campaign, posts an impression and click, waits for processing, reads statistics, checks health/metrics, and forces a failed event into the DLQ path.

## Operational files

- `docker-compose.yml` runs the API, PostgreSQL, Redis, Kafka, ClickHouse, and the required initialization steps.
- `Dockerfile` produces a small non-root runtime image from a multi-stage build.
- `.github/workflows/ci.yml` runs formatting checks, static analysis, unit tests with race detection, integration tests, build, and a Compose smoke test.
- `.golangci.yml` enables the repository's supported linters without suppressing findings globally.
- SQL migrations and ClickHouse initialization are versioned in the repository.

## Limitations

This project does not implement real-time bidding, ad selection, budget reservation under contention, authentication/authorization, Kafka replication, schema registry, multi-node ClickHouse, exactly-once semantics, or a production-grade rate-limit service. These omissions are intentional and will be listed in the README instead of being implied by the architecture.

## Production improvements

A production deployment would add authenticated tenants, broker ACLs and replicated partitions, schema compatibility management, a durable replay workflow for the DLQ, stronger idempotency coordination, budget reservation with transactional guarantees, distributed rate limiting, OpenTelemetry traces, alerting, migration orchestration, load testing with real traffic data, and separate read/write scaling policies for analytical workloads.
