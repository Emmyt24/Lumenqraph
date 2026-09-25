# Database Schema

This document describes the intended index set per table so future index
audits start from a known baseline (see #149).

## Index baseline

### `events`

- `idx_events_contract_event_ledger (contract_id, event_name, ledger DESC, event_id DESC)`
  — covers the hot query path. `idx_events_name (contract_id, event_name)` was
  dropped in `0011` as a strict prefix of this index.

### `token_transfers`

- Composite transfer indexes added in `0010_hot_query_indexes.sql`.
- `idx_transfers_contract_ledger`, `idx_transfers_from`, and `idx_transfers_to`
  were dropped in `0011` as they overlap heavily with the composite indexes.

### `contract_data`

- `idx_contract_data_label` (from `0006`).
- `idx_contract_data_contract_label_ledger` was dropped in `0011` as an exact
  duplicate of `idx_contract_data_label`.

### `contract_spec_versions`

- `UNIQUE (contract_id, version)`.
- `idx_spec_versions_contract` (from `0008`).
- `idx_contract_spec_versions_contract_version_desc` was dropped in `0011` as an
  exact duplicate of `idx_spec_versions_contract`.
- `idx_contract_spec_versions_contract` was dropped in `0011` as a strict
  prefix of the unique index.

## Redundant index removal

Migration `0011_drop_redundant_indexes.sql` removes the duplicate and prefix
indexes introduced by `0010_hot_query_indexes.sql`. The remaining indexes are
sufficient for the route queries in `routes/*.rs`; verify with `EXPLAIN` and
`pg_stat_user_indexes` (idx_scan = 0) before dropping any further index.
