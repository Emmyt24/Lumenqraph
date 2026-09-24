-- Add UNIQUE constraint on (contract_id, key_hash, ledger) to contract_data
-- to prevent duplicate snapshots for the same key at the same ledger.
--
-- Issue: #298

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_contract_data_contract_key_ledger'
    ) THEN
        ALTER TABLE contract_data
            ADD CONSTRAINT uq_contract_data_contract_key_ledger UNIQUE (contract_id, key_hash, ledger);
    END IF;
END $$;
