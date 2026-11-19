-- Migration 0011: contract_data tombstones
--
-- Adds support for recording deleted/expired/archived contract-data entries
-- so that `/contracts/:id/data` no longer reports stale values for keys whose
-- entries have been removed or expired (issue #742).
--
-- Changes:
--   * `deleted`  - true when the entry is a tombstone (no longer present).
--   * `archived` - true when a persistent entry was archived (restorable)
--                  rather than deleted outright.
--   * `value`    - made nullable so tombstones can store NULL.
--   * index on (contract_id, key_hash, ledger DESC) for newest-row lookups.

ALTER TABLE contract_data
    ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE contract_data
    ADD COLUMN IF NOT EXISTS archived BOOLEAN NOT NULL DEFAULT false;

-- Allow tombstones to store a NULL value.
ALTER TABLE contract_data
    ALTER COLUMN value DROP NOT NULL;

-- Newest-row-per-key lookups (used by GET /contracts/:id/data and by the
-- batch snapshot loops to detect existing tombstones).
CREATE INDEX IF NOT EXISTS contract_data_contract_key_ledger_idx
    ON contract_data (contract_id, key_hash, ledger DESC);