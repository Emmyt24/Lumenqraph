-- Track ledger ranges missed due to mid-batch pagination RPC errors (#295).
CREATE TABLE IF NOT EXISTS missed_ranges (
    id BIGSERIAL PRIMARY KEY,
    from_ledger BIGINT NOT NULL,
    to_ledger BIGINT NOT NULL,
    reason TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    recovered_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_missed_ranges_unrecovered
    ON missed_ranges (detected_at)
    WHERE recovered_at IS NULL;
