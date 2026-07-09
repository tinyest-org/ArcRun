-- Audit 2, D1/D7 (7.3a) — DB-enforced **Concurrency** rules via slot counters.
--
-- `rule_slot(lock_key PK, used)` is a per-rule shared counter. A claim increments
-- the slot of each of the candidate's Concurrency rules in the SAME transaction as
-- the Pending -> Claimed UPDATE:
--   INSERT INTO rule_slot (lock_key, used) VALUES ($k, 1)
--   ON CONFLICT (lock_key) DO UPDATE SET used = rule_slot.used + 1
--   WHERE rule_slot.used < $threshold RETURNING used
-- No row returned ⇒ the rule is at its limit ⇒ the whole claim transaction ROLLBACKs
-- (any already-applied increments are undone with it). Every exit from Claimed/Running
-- (success, failure, timeout, cancel, stop_batch, requeue-stale) decrements the slot
-- back down, so the counter is O(1) per claim, replica-safe by row locking, and
-- replaces both the COUNT(*) probe and the concurrency advisory-lock layer.
--
-- No `max` is stored: the threshold is the candidate rule's own `max_concurency`,
-- checked at claim time — preserving the "each candidate carries its own threshold
-- against the shared count" semantics.
CREATE TABLE rule_slot (
    lock_key TEXT PRIMARY KEY,
    used     INTEGER NOT NULL DEFAULT 0 CHECK (used >= 0)
);

-- The exact slot keys a task consumed at claim time, persisted so they can be
-- released precisely on any exit from Claimed/Running. They are read back from this
-- column for release and NEVER recomputed: `metadata` is mutable while Running (PATCH
-- full-replace), so recomputing a key at release time could diverge from the claim-time
-- key and leak the slot. NULL = the task holds no concurrency slots (never claimed with
-- a Concurrency rule, or already released).
ALTER TABLE task ADD COLUMN claimed_slot_keys TEXT[];
