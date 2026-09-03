-- +goose Up
-- Idempotency keys for POST /jobs: replay returns the original job instead
-- of duplicating. Nullable so keyless jobs are unaffected; the partial
-- unique index enforces one row per key while allowing many NULLs.

ALTER TABLE jobs ADD COLUMN idempotency_key TEXT;

CREATE UNIQUE INDEX idx_jobs_idempotency_key ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- +goose Down

DROP INDEX IF EXISTS idx_jobs_idempotency_key;

ALTER TABLE jobs DROP COLUMN IF EXISTS idempotency_key;
