# StreamForge Go

[![CI](https://github.com/denfry/streamforge-go/actions/workflows/ci.yml/badge.svg)](https://github.com/denfry/streamforge-go/actions/workflows/ci.yml)

StreamForge is a small advertising-event ingestion and analytics service built to demonstrate Go concurrency and event-driven processing. HTTP requests validate campaigns and events, publish accepted events to Kafka, process them with a bounded worker pool, and write analytics to ClickHouse. PostgreSQL remains the transactional source of truth; Redis is a disposable cache and short-lived counter store.

## Implemented functionality

- Campaign creation and lookup with PostgreSQL transactions and Redis cache-aside reads.
- Impression and click ingestion with bounded request bodies, validation, campaign existence checks, user upsert, request timeouts, and Kafka publication.
- Kafka consumer with goroutines, channels, fixed workers, context cancellation, bounded backpressure, retry with cancellation-aware backoff, and a dead-letter topic.
- Contiguous per-partition offset tracking so an out-of-order completed job cannot commit past an earlier unfinished job.
- ClickHouse event storage with stable event IDs, `ReplacingMergeTree` versions, deduplicating `FINAL` statistics queries, and click-through rate calculation.
- Dependency-aware health endpoint, Prometheus metrics, JSON logs, process-local ingestion rate limiting, and graceful HTTP/consumer shutdown.
- Unit tests, race-detector CI coverage, Testcontainers integration coverage for PostgreSQL/Redis/ClickHouse/Kafka initialization, a concrete worker-pool benchmark, and a Docker Compose smoke scenario.

## Architecture

```mermaid
flowchart LR
    Client[HTTP client] --> API[Go API]
    API --> PG[(PostgreSQL)]
    API --> Redis[(Redis cache/counters)]
    API --> Kafka[(Apache Kafka)]
    Kafka --> Consumer[Consumer goroutine]
    Consumer --> Queue[Bounded job channel]
    Queue --> Pool[Worker pool]
    Pool --> Retry[Retry policy]
    Retry --> CH[(ClickHouse)]
    Retry --> DLQ[(Kafka DLQ)]
    API --> Metrics[/metrics]
    Pool --> Metrics
```

The service is intentionally a single-node local stack. It does not implement ad selection, real-time bidding, authentication, budget reservation under contention, or exactly-once delivery.

## Quick start

Requirements: Docker Desktop with Linux containers, Docker Compose v2, `curl`, and Python 3.11+ for the smoke script.

```bash
docker compose up --build
```

Compose starts PostgreSQL 16.4, Redis 7.4.1, Apache Kafka 4.0.0 in KRaft mode, ClickHouse 24.8, the topic initializer, and the API. The API applies the versioned SQL bootstrap files on startup. Local development values are embedded in Compose defaults; copy `.env.example` to `.env` only when overriding them.

The API is available at `http://localhost:8080`.

## API examples

Create a campaign:

```bash
curl -X POST http://localhost:8080/v1/campaigns \
  -H "Content-Type: application/json" \
  -d '{"name":"Spring campaign","daily_budget":250,"currency":"USD","targeting":{"country":"US","device":"mobile"}}'
```

Use the returned `id` for the next calls:

```bash
curl http://localhost:8080/v1/campaigns/<campaign-id>

curl -X POST http://localhost:8080/v1/events/impression \
  -H "Content-Type: application/json" \
  -d '{"event_id":"4a4b3b5d-40ca-4d43-b3c4-2ed4bd4bced8","campaign_id":"<campaign-id>","user_id":"user-42","occurred_at":"2026-08-31T07:00:00Z"}'

curl -X POST http://localhost:8080/v1/events/click \
  -H "Content-Type: application/json" \
  -d '{"event_id":"5b5c4c6e-51da-4e44-c4d5-3fe5ce5cdf90","campaign_id":"<campaign-id>","user_id":"user-42","occurred_at":"2026-08-31T07:00:00Z"}'

curl "http://localhost:8080/v1/stats/campaign/<campaign-id>"
curl http://localhost:8080/health
curl http://localhost:8080/metrics
```

Event endpoints return `202 Accepted` after Kafka accepts the envelope. They do not wait for ClickHouse processing. Statistics become visible after the consumer processes the event.

On Windows PowerShell, use `curl.exe` and pass the JSON body from a file with `-d @body.json`.

## Design decisions

### Why these stores

PostgreSQL stores campaigns, targeting, and users because these entities need constraints, transactions, and predictable point reads. Redis stores only cache entries and expiring counters; losing Redis must not lose business data. Kafka absorbs bursts and creates a durable boundary between HTTP latency and analytics writes. ClickHouse is optimized for append-heavy analytical event reads and is not used for campaign authorization or transactional updates.

ClickHouse stores an event ID and processing version in a `ReplacingMergeTree`. The consumer is at-least-once: a broker redelivery can cause another insert, while the `FINAL` statistics query and stable event identity produce one logical event in the normal deduplication path. This is not a claim of broker-level exactly-once processing.

### Concurrency and backpressure

A goroutine is Go runtime-managed execution state scheduled onto available OS threads. An OS thread is a kernel-scheduled execution resource and is substantially heavier to create and manage. StreamForge uses goroutines for the HTTP server, Kafka fetch loop, workers, and lifecycle supervision rather than creating a thread per event.

Channels make ownership transfer and lifecycle boundaries visible. The consumer sends jobs through a finite buffered channel. When the channel is full, the fetch loop blocks and stops fetching more messages; this is the backpressure boundary. The queue does not grow without limit.

The offset coordinator protects partition state with a mutex. Workers never update that map directly. A completed offset is committed only when all lower offsets observed for that partition are complete. The worker pool owns its input channel and closes it only after the fetch loop stops, which avoids a concurrent send/close panic.

`context.Context` carries request deadlines into Kafka publication and processing deadlines into ClickHouse writes. Retry timers select on context cancellation, so shutdown or a timed-out operation does not wait for a full backoff interval.

### Failure handling

- Invalid requests return structured `400` errors before database or broker side effects.
- Missing campaigns return `404`; dependency failures return sanitized `503` responses.
- Redis errors degrade to a PostgreSQL read.
- Kafka writes use bounded writer attempts and the request publish timeout.
- ClickHouse processing uses a bounded retry policy. The local adapter treats non-cancellation storage errors as retryable because the driver can return opaque network/driver errors; the operation still stops at the configured attempt limit.
- A terminal processing failure is published to `streamforge.events.dlq` with source topic, partition, offset, event ID envelope, error class, and attempt count. The source offset is not committed if the DLQ publish fails.
- Shutdown cancels fetching, closes the bounded input, waits for worker results within the application shutdown deadline, and does not claim unfinished jobs were committed.

## Testing

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
golangci-lint run
go build ./cmd/streamforge
go test ./internal/processing -bench BenchmarkPoolSubmit -benchmem -count=1
STREAMFORGE_INTEGRATION=1 go test ./tests/integration -count=1 -v
docker compose config --quiet
bash scripts/smoke.sh
```

The Testcontainers suite is opt-in because it requires Docker. Without `STREAMFORGE_INTEGRATION=1`, it skips without starting containers. With the flag set and Docker unavailable, it fails with the container-runtime error rather than hiding a missing integration run. The Compose smoke script creates a campaign, posts one impression and click, waits for both ClickHouse counts, and checks health and metrics.

### Benchmark observed locally

Command:

```bash
go test ./internal/processing -bench BenchmarkPoolSubmit -benchmem -count=1
```

Observed run: Windows `amd64`, AMD Ryzen 5 5600G with Radeon Graphics, Go `1.26.4`:

```text
BenchmarkPoolSubmit-12    1826887    792.0 ns/op    0 B/op    0 allocs/op
```

This measures the local bounded pool submission path with a no-op handler. It is not a production throughput claim and is not a substitute for traffic-shaped load testing.

## Security considerations

- No real credentials are committed. `.env` is ignored; `.env.example` and Compose defaults contain only local development values.
- HTTP bodies and metadata are bounded, campaign/event fields are validated, and SQL uses parameters.
- Client IP is taken from the socket address rather than an untrusted forwarding header for the process-local rate limiter.
- Database, broker, and ClickHouse errors are not returned verbatim to clients.
- Logs and metric labels exclude secrets and unrestricted metadata.
- The API image runs as an unprivileged user. The Compose stack is intended for local use and does not provide TLS, authentication, tenant isolation, broker ACLs, or production secret management.

## Limitations

The repository does not implement authentication, authorization, campaign budget reservation, real-time bidding, schema registry compatibility checks, replicated Kafka partitions, multi-node ClickHouse, distributed rate limiting, replay tooling for the DLQ, or exactly-once delivery. The Kafka setup and topic replication factor are deliberately single-node development settings.

## Next steps for production

A production deployment would need authenticated tenants and ownership checks, broker ACLs and replicated partitions, compatible event schemas, a durable DLQ replay workflow, transactional budget reservation, distributed rate limiting, OpenTelemetry traces, alerting, migration orchestration, deployment resource limits, traffic-shaped load tests, and separate scaling policies for ingestion and analytics.

## Project layout

- `cmd/streamforge` wires dependencies and owns process lifecycle.
- `internal/httpapi` owns HTTP contracts, errors, health, metrics, and rate limiting.
- `internal/campaigns` and `internal/storage/postgres` own transactional campaign behavior.
- `internal/messaging` owns Kafka envelopes, event writers, and DLQ publishing.
- `internal/processing` owns retry, backpressure, worker lifecycle, and offset safety.
- `internal/storage/clickhouse` owns event inserts and analytical queries.
- `migrations`, `docker-compose.yml`, and `scripts` define the reproducible local stack.
