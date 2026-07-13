DROP INDEX IF EXISTS idx_webhook_outbox_due_created;
CREATE INDEX idx_webhook_outbox_next_attempt_at ON webhook_outbox (next_attempt_at);
ALTER TABLE webhook_outbox DROP COLUMN lease_token;
