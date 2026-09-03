# Domain Docs

- **Layout:** single-context
- **Glossary:** `model.Job` (`type`, `payload JSONB`, `priority`, `scheduled_at`, `max_attempts`, `attempt_count`, `status` enum pending/running/succeeded/failed/dead), `model.Worker` (`capabilities TEXT[]`, `last_heartbeat`, `status` alive/dead), `model.JobAttempt` (`worker_id`, `started_at`, `finished_at`, `success`, `error`, `duration_ms`)
- **ADRs:** `docs/adr/` (none yet)
