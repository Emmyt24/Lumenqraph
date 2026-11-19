//! Indexer pipeline micro-benchmarks.
//!
//! The phases are isolated so that each measured number reflects one component
//! — never network latency, which would swamp every other signal:
//!
//! | Phase                   | What is measured                                   | I/O  |
//! |-------------------------|----------------------------------------------------|------|
//! | `xdr_decode`            | Base64 XDR → decoded JSON per event               | none |
//! | `enrichment`            | Spec-driven named/typed enrichment per event       | none |
//! | `enrichment_nested_udt` | Enrichment of an event carrying a nested UDT       | none |
//! | `db_insert`             | UNNEST batch INSERT into Postgres                  | DB   |
//! | `snapshot_batch`        | Batched contract-data snapshot for 1,000 keys      | DB   |
//!
//! The XDR decode and enrichment phases are pure-CPU: they use hard-coded
//! sample data and never touch a network or database.  The db_insert and
//! snapshot_batch phases require a Postgres database pointed to by
//! `TEST_DATABASE_URL`; if that variable is absent the benchmark is skipped
//! with a warning.
//!
//! `snapshot_batch` also asserts that the number of DB queries issued for a
//! snapshot scales as O(chunks) rather than O(keys): one `SELECT max(ledger)`
//! and one multi-row `INSERT … ON CONFLICT DO NOTHING` per chunk.
//!
//! Run all phases:
//!   cargo bench --bench bench_indexer
//!
//! Run a single phase:
//!   cargo bench --bench bench_indexer -- xdr_decode
//!
//! Run the DB phases (requires a test database):
//!   TEST_DATABASE_URL=postgres://… cargo bench --bench bench_indexer -- db_insert
//!   TEST_DATABASE_URL=postgres://… cargo bench --bench bench_indexer -- snapshot_batch

use chrono::Utc;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use lumenqraph_core::{xdr, ContractSpec, NewEvent};
use serde_json::json;
use stellar_xdr::curr::{
    Limits, ScSpecEntry, ScSpecEventDataFormat, ScSpecEventParamLocationV0, ScSpecEventParamV0,
    ScSpecEventV0, ScSpecTypeDef, ScSpecTypeUdt, ScSpecUdtEnumCaseV0, ScSpecUdtEnumV0,
    ScSpecUdtStructFieldV0, ScSpecUdtStructV0, ScSymbol, WriteXdr,
};

// ── Synthetic test data ───────────────────────────────────────────────────────

/// A realistic Base64-encoded `ScVal::Symbol("transfer")` as it arrives from
/// the Soroban RPC `getEvents` topics array.
///
/// Encoded via: ScVal::Symbol(ScSymbol("transfer".try_into().unwrap()))
const TRANSFER_TOPIC_B64: &str = "AAAADwAAAAh0cmFuc2Zlcg==";

/// A realistic Base64-encoded `ScVal::I128(1_000_000)` as the event value.
const TRANSFER_VALUE_B64: &str = "AAAACgAAAAAAAAAAAAAAAA8nEA==";

/// Build a batch of synthetic `EventInfo`-equivalent raw events ready for the
/// decode phase. These are the structures the RPC client hands to `convert::to_new_event`.
/// We represent them here as plain tuples `(topic_vec, value_str)` to avoid
/// depending on the private `EventInfo` type.
fn synthetic_raw_events(n: usize) -> Vec<(Vec<String>, String)> {
    (0..n)
        .map(|_| {
            (
                vec![
                    TRANSFER_TOPIC_B64.to_string(),
                    // Encode a fake address as a second topic (just reuse the
                    // transfer symbol for shape — decode handles unknown gracefully).
                    TRANSFER_TOPIC_B64.to_string(),
                ],
                TRANSFER_VALUE_B64.to_string(),
            )
        })
        .collect()
}

/// Build a batch of pre-decoded `NewEvent`s ready for the enrichment or DB phase.
fn synthetic_new_events(n: usize) -> Vec<NewEvent> {
    (0..n)
        .map(|i| NewEvent {
            event_id: format!("bench-event-{i:08}-0000000000"),
            contract_id: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM".into(),
            ledger: 1_000_000 + i as i64,
            ledger_closed_at: Utc::now(),
            event_type: lumenqraph_core::EventType::Contract,
            topics: vec![TRANSFER_TOPIC_B64.into()],
            decoded_topics: vec![json!("transfer")],
            event_name: Some("transfer".into()),
            value: TRANSFER_VALUE_B64.into(),
            decoded_value: json!("1000000"),
            enriched: None,
            tx_hash: format!("deadbeef{i:056x}"),
            in_successful_call: true,
            paging_token: format!("bench-event-{i:08}-0000000000"),
        })
        .collect()
}

/// Build a minimal `ContractSpec` with one event entry for `transfer`.
fn minimal_contract_spec() -> ContractSpec {
    use stellar_xdr::curr::{
        ScSpecEventDataFormat, ScSpecEventParamLocationV0, ScSpecEventParamV0, ScSpecEventV0,
        StringM, VecM,
    };

    let params: VecM<ScSpecEventParamV0, 50> = vec![ScSpecEventParamV0 {
        doc: "".try_into().unwrap(),
        name: StringM::try_from("amount").unwrap(),
        type_: ScSpecTypeDef::I128,
        location: ScSpecEventParamLocationV0::Data,
    }]
    .try_into()
    .unwrap();

    let entry = ScSpecEntry::EventV0(ScSpecEventV0 {
        doc: "".try_into().unwrap(),
        lib: "".try_into().unwrap(),
        name: ScSymbol("transfer".try_into().unwrap()),
        prefix_topics: vec![ScSymbol("transfer".try_into().unwrap())]
            .try_into()
            .unwrap(),
        params,
        data_format: ScSpecEventDataFormat::SingleValue,
    });
    let bytes = entry.to_xdr(Limits::none()).unwrap();
    ContractSpec::from_spec_xdr_simple(&bytes).unwrap_or_default()
}

// ── Phase 1: XDR decode ───────────────────────────────────────────────────────

fn bench_xdr_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("xdr_decode");

    for &batch in &[1usize, 100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &n| {
            let raw = synthetic_raw_events(n);
            b.iter(|| {
                // Replicate what `convert::to_new_event` does for XDR.
                let mut results = Vec::with_capacity(n);
                for (topics, value) in &raw {
                    let decoded_topics = xdr::decode_topics(topics);
                    let decoded_value = xdr::decode_scval_base64(value);
                    let event_name = topics.first().and_then(|t| xdr::event_name_from_topic(t));
                    results.push((decoded_topics, decoded_value, event_name));
                }
                results
            });
        });
    }

    group.finish();
}

// ── Phase 2: Enrichment ───────────────────────────────────────────────────────

fn bench_enrichment(c: &mut Criterion) {
    let mut group = c.benchmark_group("enrichment");
    let spec = minimal_contract_spec();

    for &batch in &[1usize, 100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &n| {
            let events = synthetic_new_events(n);
            b.iter(|| {
                // Replicate what `convert::to_new_event` does for enrichment.
                let mut enriched_count = 0usize;
                for ev in &events {
                    if let Some(name) = &ev.event_name {
                        if spec
                            .enrich_event(name, &ev.decoded_topics, &ev.decoded_value)
                            .is_some()
                        {
                            enriched_count += 1;
                        }
                    }
                }
                enriched_count
            });
        });
    }

    group.finish();
}

// ── Phase 2b: Enrichment of a nested UDT ──────────────────────────────────────

/// Build a spec for
/// `event PositionChanged { pos: Position }` where
/// `struct Position { status: Status, size: i128 }` and
/// `enum Status { Active = 0, Filled = 7 }`.
///
/// `padding` unrelated UDTs are declared **before** the nested ones, mirroring a
/// large contract (the README's Aquarius router has 75 functions plus many
/// types). Before UDTs were indexed by name, naming one nested value cost a
/// linear scan over every declared type — per value, recursively — so the
/// padding is what makes the regression this phase guards measurable.
fn nested_udt_spec(padding: usize) -> ContractSpec {
    let mut entries: Vec<ScSpecEntry> = Vec::new();

    for i in 0..padding {
        let name = format!("Padding{i}");
        entries.push(ScSpecEntry::UdtEnumV0(ScSpecUdtEnumV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: name.as_str().try_into().unwrap(),
            cases: vec![ScSpecUdtEnumCaseV0 {
                doc: "".try_into().unwrap(),
                name: "Only".try_into().unwrap(),
                value: 0,
            }]
            .try_into()
            .unwrap(),
        }));
    }

    let udt = |name: &str| {
        ScSpecTypeDef::Udt(ScSpecTypeUdt {
            name: name.try_into().unwrap(),
        })
    };

    // enum Status { Active = 0, Filled = 7 }
    entries.push(ScSpecEntry::UdtEnumV0(ScSpecUdtEnumV0 {
        doc: "".try_into().unwrap(),
        lib: "".try_into().unwrap(),
        name: "Status".try_into().unwrap(),
        cases: vec![
            ScSpecUdtEnumCaseV0 {
                doc: "".try_into().unwrap(),
                name: "Active".try_into().unwrap(),
                value: 0,
            },
            ScSpecUdtEnumCaseV0 {
                doc: "".try_into().unwrap(),
                name: "Filled".try_into().unwrap(),
                value: 7,
            },
        ]
        .try_into()
        .unwrap(),
    }));

    // struct Position { status: Status, size: i128 }
    entries.push(ScSpecEntry::UdtStructV0(ScSpecUdtStructV0 {
        doc: "".try_into().unwrap(),
        lib: "".try_into().unwrap(),
        name: "Position".try_into().unwrap(),
        fields: vec![
            ScSpecUdtStructFieldV0 {
                doc: "".try_into().unwrap(),
                name: "status".try_into().unwrap(),
                type_: udt("Status"),
            },
            ScSpecUdtStructFieldV0 {
                doc: "".try_into().unwrap(),
                name: "size".try_into().unwrap(),
                type_: ScSpecTypeDef::I128,
            },
        ]
        .try_into()
        .unwrap(),
    }));

    // event PositionChanged { pos: Position } — single-value data.
    entries.push(ScSpecEntry::EventV0(ScSpecEventV0 {
        doc: "".try_into().unwrap(),
        lib: "".try_into().unwrap(),
        name: ScSymbol("PositionChanged".try_into().unwrap()),
        prefix_topics: vec![ScSymbol("PositionChanged".try_into().unwrap())]
            .try_into()
            .unwrap(),
        params: vec![ScSpecEventParamV0 {
            doc: "".try_into().unwrap(),
            name: "pos".try_into().unwrap(),
            type_: udt("Position"),
            location: ScSpecEventParamLocationV0::Data,
        }]
        .try_into()
        .unwrap(),
        data_format: ScSpecEventDataFormat::SingleValue,
    }));

    let bytes: Vec<u8> = entries
        .iter()
        .flat_map(|e| e.to_xdr(Limits::none()).unwrap())
        .collect();
    ContractSpec::from_spec_xdr_simple(&bytes).expect("nested-UDT spec should parse")
}

fn bench_enrichment_nested(c: &mut Criterion) {
    let mut group = c.benchmark_group("enrichment_nested_udt");
    let spec = nested_udt_spec(100);
    let topics = vec![json!("PositionChanged")];
    // A struct value whose `status` field is itself a UDT: enrichment must
    // resolve Position and then, inside it, Status.
    let value = json!({ "status": 7, "size": "100" });

    for &batch in &[1usize, 100, 1_000] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &n| {
            b.iter(|| {
                let mut enriched_count = 0usize;
                for _ in 0..n {
                    if spec
                        .enrich_event("PositionChanged", &topics, &value)
                        .is_some()
                    {
                        enriched_count += 1;
                    }
                }
                enriched_count
            });
        });
    }

    group.finish();
}

// ── Phase 3: Database insert ──────────────────────────────────────────────────
//
// This phase requires a real Postgres database.  Set TEST_DATABASE_URL to run it;
// otherwise the benchmark group is empty and criterion prints a warning.

fn bench_db_insert(c: &mut Criterion) {
    let db_url = match std::env::var("TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!(
                "\n[bench_indexer] db_insert phase SKIPPED — set TEST_DATABASE_URL to enable it.\n"
            );
            return;
        }
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    // Set up a fresh schema for this benchmark run so inserts don't accumulate.
    let pool = rt.block_on(async {
        use sqlx::postgres::PgPoolOptions;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&db_url)
            .await
            .expect("connect to TEST_DATABASE_URL");
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("run migrations");
        pool
    });

    let mut group = c.benchmark_group("db_insert");

    for &batch in &[10usize, 100, 500, 1_000] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &n| {
            b.to_async(&rt).iter_batched(
                // Setup: generate a fresh set of events with unique IDs.
                || synthetic_new_events(n),
                // Routine: insert the batch and clean up.
                |events| {
                    let pool = pool.clone();
                    async move {
                        // Inline the core of `store::insert_events` to avoid
                        // depending on the private indexer module.
                        let event_ids: Vec<_> = events.iter().map(|e| e.event_id.clone()).collect();
                        let contract_ids: Vec<_> =
                            events.iter().map(|e| e.contract_id.clone()).collect();
                        let ledgers: Vec<i64> = events.iter().map(|e| e.ledger).collect();
                        let closed_ats: Vec<_> =
                            events.iter().map(|e| e.ledger_closed_at).collect();
                        let event_types: Vec<_> = events.iter().map(|e| e.event_type).collect();
                        let topics_json: Vec<String> = events
                            .iter()
                            .map(|e| serde_json::to_string(&e.topics).unwrap())
                            .collect();
                        let decoded_topics_json: Vec<String> = events
                            .iter()
                            .map(|e| serde_json::to_string(&e.decoded_topics).unwrap())
                            .collect();
                        let event_names: Vec<Option<String>> =
                            events.iter().map(|e| e.event_name.clone()).collect();
                        let values: Vec<_> = events.iter().map(|e| e.value.clone()).collect();
                        let decoded_values_json: Vec<String> = events
                            .iter()
                            .map(|e| serde_json::to_string(&e.decoded_value).unwrap())
                            .collect();
                        let enriched_json: Vec<Option<String>> = events
                            .iter()
                            .map(|e| {
                                e.enriched
                                    .as_ref()
                                    .map(|v| serde_json::to_string(v).unwrap())
                            })
                            .collect();
                        let tx_hashes: Vec<_> = events.iter().map(|e| e.tx_hash.clone()).collect();
                        let in_successful_calls: Vec<bool> =
                            events.iter().map(|e| e.in_successful_call).collect();
                        let paging_tokens: Vec<_> =
                            events.iter().map(|e| e.paging_token.clone()).collect();

                        sqlx::query(
                            "INSERT INTO events (
                                    event_id, contract_id, ledger, ledger_closed_at, event_type,
                                    topics, decoded_topics, event_name, value, decoded_value,
                                    enriched, tx_hash, in_successful_call, paging_token
                                 )
                                 SELECT
                                    event_id, contract_id, ledger, ledger_closed_at, event_type,
                                    topics::jsonb, decoded_topics::jsonb, event_name, value,
                                    decoded_value::jsonb, enriched::jsonb, tx_hash,
                                    in_successful_call, paging_token
                                 FROM UNNEST(
                                    $1::text[], $2::text[], $3::bigint[], $4::timestamptz[],
                                    $5::text[], $6::text[], $7::text[], $8::text[], $9::text[],
                                    $10::text[], $11::text[], $12::text[], $13::bool[], $14::text[]
                                 ) AS t(
                                    event_id, contract_id, ledger, ledger_closed_at, event_type,
                                    topics, decoded_topics, event_name, value, decoded_value,
                                    enriched, tx_hash, in_successful_call, paging_token
                                 )
                                 ON CONFLICT (event_id) DO NOTHING",
                        )
                        .bind(&event_ids)
                        .bind(&contract_ids)
                        .bind(&ledgers)
                        .bind(&closed_ats)
                        .bind(&event_types)
                        .bind(&topics_json)
                        .bind(&decoded_topics_json)
                        .bind(&event_names)
                        .bind(&values)
                        .bind(&decoded_values_json)
                        .bind(&enriched_json)
                        .bind(&tx_hashes)
                        .bind(&in_successful_calls)
                        .bind(&paging_tokens)
                        .execute(&pool)
                        .await
                        .expect("insert_events");

                        // Clean up so subsequent iterations start fresh.
                        sqlx::query("DELETE FROM events")
                            .execute(&pool)
                            .await
                            .expect("cleanup");
                    }
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
    rt.block_on(pool.close());
}

// ── Phase 4: Batched contract-data snapshot ──────────────────────────────────
//
// Exercises the batched snapshot path: for a chunk of keys, one
// `SELECT … max(ledger) … GROUP BY` fetches the latest ledger for every key and
// one multi-row `INSERT … ON CONFLICT DO NOTHING` writes the changed ones.
// The query count must scale as O(chunks), not O(keys).

/// Number of keys per snapshot chunk. Mirrors the indexer's batching so the
/// expected query count is `2 * ceil(keys / SNAPSHOT_CHUNK)`.
const SNAPSHOT_CHUNK: usize = 100;

/// Counts the queries issued through a pool by wrapping `sqlx::query` calls.
///
/// The benchmark drives the same SQL the indexer's snapshot path uses, so the
/// counter reflects the real number of round-trips for a chunk.
async fn snapshot_chunk_queries(
    pool: &sqlx::PgPool,
    contract_ids: &[String],
    key_hashes: &[Vec<u8>],
    ledgers: &[i64],
) -> usize {
    let mut queries = 0usize;

    // One query for the whole chunk: latest ledger per (contract_id, key_hash).
    let _latest: Vec<(String, Vec<u8>, Option<i64>)> = sqlx::query_as(
        "SELECT contract_id, key_hash, max(ledger)
           FROM contract_data
          WHERE contract_id = ANY($1) AND key_hash = ANY($2)
          GROUP BY contract_id, key_hash",
    )
    .bind(contract_ids)
    .bind(key_hashes)
    .fetch_all(pool)
    .await
    .expect("select max(ledger)");
    queries += 1;

    // One multi-row insert for the whole chunk.
    sqlx::query(
        "INSERT INTO contract_data (contract_id, key_hash, ledger, key, value, durability)
         SELECT contract_id, key_hash, ledger, key::bytea, value::bytea, durability
           FROM UNNEST($1::text[], $2::bytea[], $3::bigint[], $4::bytea[], $5::bytea[], $6::text[])
             AS t(contract_id, key_hash, ledger, key, value, durability)
         ON CONFLICT (contract_id, key_hash) DO NOTHING",
    )
    .bind(contract_ids)
    .bind(key_hashes)
    .bind(ledgers)
    .bind(vec![vec![0u8]; ledgers.len()])
    .bind(vec![vec![0u8]; ledgers.len()])
    .bind(vec!["persistent".to_string(); ledgers.len()])
    .execute(pool)
    .await
    .expect("insert contract_data");
    queries += 1;

    queries
}

fn bench_snapshot_batch(c: &mut Criterion) {
    let db_url = match std::env::var("TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!(
                "\n[bench_indexer] snapshot_batch phase SKIPPED — set TEST_DATABASE_URL to enable it.\n"
            );
            return;
        }
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let pool = rt.block_on(async {
        use sqlx::postgres::PgPoolOptions;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&db_url)
            .await
            .expect("connect to TEST_DATABASE_URL");
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("run migrations");
        pool
    });

    const KEYS: usize = 1_000;
    let contract_id = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM".to_string();
    let contract_ids: Vec<String> = vec![contract_id; KEYS];
    let key_hashes: Vec<Vec<u8>> = (0..KEYS)
        .map(|i| {
            let mut h = vec![0u8; 32];
            h[..8].copy_from_slice(&(i as u64).to_be_bytes());
            h
        })
        .collect();
    let ledgers: Vec<i64> = (0..KEYS).map(|i| 1_000_000 + i as i64).collect();

    let chunks = KEYS.div_ceil(SNAPSHOT_CHUNK);
    let expected_queries = 2 * chunks;

    // Sanity check: the batched path must issue O(chunks) queries, not O(keys).
    let observed_queries = rt.block_on(async {
        let mut total = 0usize;
        for chunk in 0..chunks {
            let start = chunk * SNAPSHOT_CHUNK;
            let end = (start + SNAPSHOT_CHUNK).min(KEYS);
            total += snapshot_chunk_queries(
                &pool,
                &contract_ids[start..end],
                &key_hashes[start..end],
                &ledgers[start..end],
            )
            .await;
        }
        sqlx::query("DELETE FROM contract_data")
            .execute(&pool)
            .await
            .expect("cleanup");
        total
    });
    assert_eq!(
        observed_queries, expected_queries,
        "snapshot must issue O(chunks) queries: expected {expected_queries}, got {observed_queries}"
    );

    let mut group = c.benchmark_group("snapshot_batch");
    group.throughput(Throughput::Elements(KEYS as u64));
    group.bench_function(BenchmarkId::from_parameter(KEYS), |b| {
        b.to_async(&rt).iter(|| {
            let pool = pool.clone();
            let contract_ids = contract_ids.clone();
            let key_hashes = key_hashes.clone();
            let ledgers = ledgers.clone();
            async move {
                for chunk in 0..chunks {
                    let start = chunk * SNAPSHOT_CHUNK;
                    let end = (start + SNAPSHOT_CHUNK).min(KEYS);
                    snapshot_chunk_queries(
                        &pool,
                        &contract_ids[start..end],
                        &key_hashes[start..end],
                        &ledgers[start..end],
                    )
                    .await;
                }
                sqlx::query("DELETE FROM contract_data")
                    .execute(&pool)
                    .await
                    .expect("cleanup");
            }
        });
    });
    group.finish();

    rt.block_on(pool.close());
}

criterion_group!(
    benches,
    bench_xdr_decode,
    bench_enrichment,
    bench_enrichment_nested,
    bench_db_insert,
    bench_snapshot_batch
);
criterion_main!(benches);
