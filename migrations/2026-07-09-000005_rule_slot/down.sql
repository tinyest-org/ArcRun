-- Reverse of up.sql (Audit 2, D1/D7 7.3a).
ALTER TABLE task DROP COLUMN claimed_slot_keys;
DROP TABLE rule_slot;
