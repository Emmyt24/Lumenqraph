#!/usr/bin/env bash
# One-shot historical catch-up from a start ledger to the current tip.
#
# Usage: ./scripts/backfill.sh [--rpc-timeout <secs>] <start_ledger>
#
# Note: bounded by RPC retention (~7 days); older ledgers are clamped.
#
# Concurrency: `backfill` is a maintenance command and does NOT take the live
# indexer's leader advisory lock. It can run alongside a live poller without
# blocking; its writes are idempotent (ON CONFLICT DO NOTHING) and it never
# moves indexer_cursor.last_processed_ledger backwards. See
# docs/DEEP_BACKFILL.md → "Concurrency model".
#
# RPC_TIMEOUT_SECS defaults to 30s, which is fine for the public SDF RPC. Slow
# archive or paid RPC endpoints used for deep historical backfills often need
# more headroom — 120s is a good starting point. A timeout aborts the whole
# batch and the retry uses the same value, so an RPC that is consistently slow
# never completes until the timeout is raised. Override it with --rpc-timeout,
# or by exporting RPC_TIMEOUT_SECS / setting it in .env.
# See docs/DEEP_BACKFILL.md → "Archive RPC timeouts".
set -euo pipefail

START=""
while [ $# -gt 0 ]; do
  case "$1" in
    --rpc-timeout)
      export RPC_TIMEOUT_SECS="${2:?--rpc-timeout needs a value in seconds}"
      shift 2
      ;;
    --rpc-timeout=*)
      export RPC_TIMEOUT_SECS="${1#*=}"
      shift
      ;;
    -h|--help)
      echo "usage: backfill.sh [--rpc-timeout <secs>] <start_ledger>"
      exit 0
      ;;
    --)
      shift
      ;;
    *)
      if [ -n "$START" ]; then
        echo "backfill.sh: unexpected argument '$1'" >&2
        echo "usage: backfill.sh [--rpc-timeout <secs>] <start_ledger>" >&2
        exit 2
      fi
      START="$1"
      shift
      ;;
  esac
done

: "${START:?usage: backfill.sh [--rpc-timeout <secs>] <start_ledger>}"

cargo run -p lumenqraph-indexer --release -- backfill "$START"
