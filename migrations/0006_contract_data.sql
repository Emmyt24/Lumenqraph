-- Per-key contract-data indexing: versioned snapshots of *individual*
-- persistent (or temporary) storage entries — a single ledger key/value pair
-- such as one token holder's `Balance(Address)`.
--
-- Unlike instance storage (see 0005), these entries are NOT enumerable from
-- RPC: you can only fetch an entry if you know its exact key. Lumenqraph
-- discovers the keys worth tracking from the events it already indexes (e.g. the
-- holders named in a token's `transfer`/`mint`/`burn` events) and snapshots each
-- one by key.
--
-- One row per (contract, key, ledger-at-which-the-entry-last-changed), so — like
-- contract_state — the table is a per-key time series: the newest row for a key
-- is its current value, older rows are history.

CREATE TABLE IF NOT EXISTS contract_data (
    contract_id  TEXT        NOT NULL,
    -- Stable id for the storage key: hex SHA-256 of the key's base64 XDR.
    key_hash     TEXT        NOT NULL,
    -- Decoded key as friendly JSON (same shape as events), e.g.
    -- ["Balance", "GABC…"]. What a human reads.
    key          JSONB       NOT NULL,
    -- Base64 XDR of the key, so the exact entry can always be refetched.
    key_xdr      TEXT        NOT NULL,
    -- 'persistent' | 'temporary' (the two Soroban contract-data durabilities).
    durability   TEXT        NOT NULL,
    -- lastModifiedLedgerSeq of the entry when this snapshot was taken.
    ledger       BIGINT      NOT NULL,
    -- Decoded value as friendly JSON.
    value        JSONB       NOT NULL,
    -- Optional discovery label, e.g. 'balance', for grouping/filtering.
    label        TEXT,
    captured_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (contract_id, key_hash, ledger),
    CONSTRAINT uq_contract_data_contract_key_ledger UNIQUE (contract_id, key_hash, ledger)
);

-- "latest value for a key" and "a key's history" both want newest-first.
CREATE INDEX IF NOT EXISTS idx_contract_data_latest
    ON contract_data (contract_id, key_hash, ledger DESC);

-- "all tracked keys of this label for a contract" (e.g. every holder balance).
CREATE INDEX IF NOT EXISTS idx_contract_data_label
    ON contract_data (contract_id, label, ledger DESC);

-- Query decoded values by content (e.g. balances over a threshold).
CREATE INDEX IF NOT EXISTS idx_contract_data_value
    ON contract_data USING GIN (value);
