# GoQueue

A distributed task queue in Go, backed only by Postgres. HTTP API for
enqueueing, workers that register capabilities and short-poll for work,
at-least-once delivery with priority ordering, delayed jobs, exponential
backoff, dead-lettering, and liveness sweeps.

## Quickstart

```bash
make up                        # postgres on :5462 (docker)
make migration action=up       # apply schema (needs DATABASE_URL, see .env.example)
make run-coordinator           # API on :8080 + liveness sweep
make run-worker                # register + heartbeat + poll loop (another shell)
```

Copy `.env.example` to `.env` and adjust. The worker additionally honors
`COORDINATOR_URL`, `WORKER_ID`, `HOSTNAME`, `CAPABILITIES` (comma-separated,
empty = any type), `POLL_INTERVAL`, and `API_KEY`.

## API

All responses are JSON. Errors are `{"error": "...", "code": "..."}` with
codes `invalid_request`, `unauthorized`, `not_found`, `method_not_allowed`,
`conflict`, `internal`. Auth: `X-API-Key` header on every route except
`GET /health` (disabled when `API_KEY` is empty). List endpoints paginate
with `?limit=` (default 20, max 100) and `?offset=` (default 0).

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/health` | `{"status":"ok"}`, no auth |
| GET | `/stats` | job counts by status + worker counts |
| POST | `/jobs` | `{type, payload, priority?=0, scheduled_at?=now (RFC3339), max_attempts?=3}` → `201`; honors `Idempotency-Key` (replay → `200` same job) |
| GET | `/jobs?status=&type=&limit=&offset=` | filter `status` comma-separated |
| GET | `/jobs/:id` | `200` / `404` |
| PATCH | `/jobs/:id` | `{priority?, max_attempts?, scheduled_at?}` → `200` / `400` / `404` |
| DELETE | `/jobs/:id` | `204`; `409` while running; `404` when missing |
| POST | `/jobs/:id/complete` | `{worker_id}` → `succeeded` |
| POST | `/jobs/:id/fail` | `{worker_id, error}` → `pending` (rescheduled with backoff) or `dead` after `max_attempts` |
| GET | `/jobs/:id/attempts` | attempt history with `duration_ms` |
| POST | `/workers/register` | `{id, hostname, capabilities?}` → `201` |
| GET | `/workers?status=&limit=&offset=` | |
| POST | `/workers/:id/heartbeat` | every 10s; missing 45s → `dead` via sweep |
| POST | `/workers/:id/poll` | claim one job → `200`, none → `204` |

Example:

```bash
curl -X POST localhost:8080/jobs -d '{"type":"email","payload":{"to":"a@b.com"},"priority":10}'
curl -X POST localhost:8080/workers/register -d '{"id":"w1","hostname":"h1","capabilities":["email"]}'
curl -X POST localhost:8080/workers/w1/poll
curl -X POST localhost:8080/jobs/<id>/complete -d '{"worker_id":"w1"}'
```

## How it works

- **Coordinator** owns the HTTP surface, atomic dispatch, and a 30s sweep.
- **Dispatch:** one transaction per poll — `SELECT ... FOR UPDATE SKIP LOCKED
  WHERE status='pending' AND scheduled_at <= now() AND type = ANY(capabilities)`
  ordered by `priority DESC, scheduled_at ASC, created_at ASC`, then mark
  `running` + record a `job_attempts` row. Empty capabilities = generic worker.
- **Retry:** `attempt_count++` per fail; below `max_attempts` the job goes back
  to `pending` with `scheduled_at = now + backoff` (exponential + jitter,
  `BACKOFF_BASE`/`BACKOFF_CAP`); at the limit it goes `dead` with `dead_at`.
  One fail cycle = one attempt row (the poll-opened row is closed in place).
- **Liveness:** workers heartbeat every 10s; the sweep marks workers `dead`
  after 45s silence and requeues their `running` jobs to `pending` without
  bumping attempts (at-least-once).
- **Workers** (`internal/worker`) execute jobs through a per-type `Registry`
  (`""` = default log-and-succeed; `"test"` = scripted sleep/fail via payload);
  unknown types fail the job loudly. Handler errors → `fail`, 404s (deleted
  jobs) are logged and skipped.

## Configuration

| Var | Default | Used by |
| --- | ------- | ------- |
| `DATABASE_URL` | — (required) | coordinator, migration |
| `SHARD_DATABASE_URLS` | empty (= `DATABASE_URL`) | coordinator, migration |
| `PORT` | `:8080` | coordinator |
| `BACKOFF_BASE` / `BACKOFF_CAP` | `5s` / `10m` | coordinator |
| `API_KEY` | empty (open) | coordinator, worker |
| `COORDINATOR_URL` | `http://localhost:8080` | worker |
| `WORKER_ID` / `HOSTNAME` | generated / hostname | worker |
| `CAPABILITIES` / `POLL_INTERVAL` | empty / `2s` | worker |

## Testing

```bash
go test ./...                                    # unit (DB-free fakes + httptest)
DATABASE_URL=... DATABASE_URL_2=... go test -p 1 -tags integration ./internal/store/ ./internal/shard/   # real Postgres (-p 1: packages share DBs)
```

CI (`.github/workflows/ci.yml`) runs `gofmt`, `go vet`, build, unit tests, plus
migrations + the integration suite against a Postgres service.

## Status

MVP (enqueue/dispatch/retry/sweep) + V2 (backoff, cancel/update, idempotency,
handler registry, auth, stats) are implemented and covered. Deliberately out of
scope: exactly-once delivery, auth beyond a shared key, rate limiting,
metrics/tracing, dashboard UI.

## Sharding

Set `SHARD_DATABASE_URLS` to a comma-separated list of Postgres DSNs to shard
across N databases (empty = single `DATABASE_URL`, unchanged behavior).
Routing is hash-modulo behind `store.Store`, so the HTTP contract is identical:

- jobs by `hash(jobID) % N`; keyed jobs by `hash(idempotencyKey) % N` (replays
  route to the same shard); workers by `hash(workerID) % N`.
- Poll peeks every shard's local top-1 and claims on the winner only, so
  cross-shard priority order is approximate under contention.
- The sweep marks dead workers per shard, then requeues their jobs on *all*
  shards (claims may live anywhere).
- Lists merge per-shard pages (approximate across shards); stats are summed.
- Run migrations against every shard: the migration tool loops `SHARD_DATABASE_URLS`.
- Changing N rehashes everything: resharding is offline (drain, migrate, switch).
