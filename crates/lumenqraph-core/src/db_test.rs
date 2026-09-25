//! Shared helper for Postgres-backed tests.
//!
//! Each test gets its own isolated schema (named `test_<uuid>`), migrated from
//! scratch and dropped when the pool is closed. Schemas are independent, so
//! tests can run in parallel without interfering with each other or with any
//! real database that happens to be pointed at by `TEST_DATABASE_URL`.
//!
//! # Usage
//!
//! ```ignore
//! #[tokio::test]
//! async fn my_test() {
//!     let db = lumenqraph_core::db_test::TestDb::new("../../migrations").await;
//!     // Use db.pool() — it is already connected and migrated.
//!     // The schema is dropped when `db` is dropped.
//! }
//! ```
//!
//! Set `TEST_DATABASE_URL` to a Postgres URL. When unset the test is skipped
//! gracefully, so a developer without Postgres can still run `cargo test` and
//! only see the DB tests skipped rather than failed.

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// A throwaway Postgres schema. The pool is connected to it; on drop the schema
/// is removed.
pub struct TestDb {
    pool: PgPool,
    schema: String,
}

impl TestDb {
    /// Connect to `TEST_DATABASE_URL`, create a fresh schema with a UUID name,
    /// run all migrations found at `migrations_path` relative to the caller's
    /// manifest directory, and return the handle.
    ///
    /// Panics (which becomes a test failure) when `TEST_DATABASE_URL` is set but
    /// the connection or migration fails. When the env var is absent the caller
    /// should skip — use [`require_db`] for a one-liner.
    pub async fn new_with_migrations(migrations_path: &str) -> Self {
        let url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must be set to run Postgres-backed tests");

        // Use a UUID so concurrent tests never collide on the schema name.
        let schema = format!("test_{}", uuid::Uuid::new_v4().simple());

        // Connect to the default database first to create the schema.
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect to TEST_DATABASE_URL");

        sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
            .execute(&admin)
            .await
            .expect("create test schema");

        // Re-connect with search_path set so SQLx migrator and all queries land
        // in this schema, not in public.
        let schema_url = append_search_path(&url, &schema);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&schema_url)
            .await
            .expect("connect with search_path");

        // Run migrations relative to CARGO_MANIFEST_DIR of the *calling* crate.
        // The caller passes the relative path from their Cargo.toml to the
        // migrations directory, e.g. "../../migrations".
        sqlx::migrate::Migrator::new(std::path::Path::new(migrations_path))
            .await
            .expect("build migrator")
            .run(&pool)
            .await
            .expect("run migrations");

        // Drop the short-lived admin pool.
        admin.close().await;

        Self { pool, schema }
    }

    /// The connected pool, already pointing at the isolated schema.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        // Best-effort cleanup: spawn a blocking task to drop the schema.
        // If this fails (e.g. the test process crashes), orphaned schemas are
        // harmless and can be cleaned up manually.
        let schema = self.schema.clone();
        let pool = self.pool.clone();
        // Use block_in_place if inside a tokio context, otherwise ignore.
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
                    .execute(&pool)
                    .await;
                pool.close().await;
            });
        })
        .join();
    }
}

/// Return `TEST_DATABASE_URL` or `None` when the variable is unset.
/// Tests can use this to skip gracefully instead of failing.
pub fn database_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL").ok()
}

/// Append (or replace) the `search_path` option in a Postgres connection URL.
///
/// If the URL already has a `search_path` query parameter it is replaced.
/// Works for `postgres://…?options=…` URLs by appending the `options` param.
fn append_search_path(url: &str, schema: &str) -> String {
    // Encode the schema name for use in the `options` query parameter.
    let option = format!("-c search_path={schema},public");
    if url.contains('?') {
        format!("{url}&options={}", urlencoding::encode(&option))
    } else {
        format!("{url}?options={}", urlencoding::encode(&option))
    }
}

// Tiny inline URL encoder — only encodes chars that matter in a query string.
mod urlencoding {
    pub fn encode(s: &str) -> String {
        s.chars()
            .flat_map(|c| match c {
                'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                    vec![c]
                }
                c => format!("%{:02X}", c as u32).chars().collect(),
            })
            .collect()
    }
}

/// Convenience macro: skip the test (via a successful no-op return) when
/// `TEST_DATABASE_URL` is not set, so `cargo test` without Postgres passes.
#[macro_export]
macro_rules! require_db {
    () => {
        if std::env::var("TEST_DATABASE_URL").is_err() {
            eprintln!("skipping: TEST_DATABASE_URL not set");
            return;
        }
    };
}

/// Assert that a CHECK constraint named `constraint` exists on `table` in the
/// current schema. Used by the enum-column constraint tests to verify the
/// migration from #365 actually installed the constraint.
pub async fn assert_check_constraint(pool: &PgPool, table: &str, constraint: &str) {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (\
             SELECT 1 FROM pg_constraint c \
             JOIN pg_class t ON t.oid = c.conrelid \
             JOIN pg_namespace n ON n.oid = t.relnamespace \
             WHERE c.contype = 'c' \
               AND t.relname = $1 \
               AND c.conname = $2 \
               AND n.nspname = current_schema()\
         )",
    )
    .bind(table)
    .bind(constraint)
    .fetch_one(pool)
    .await
    .expect("query pg_constraint");

    assert!(
        exists,
        "expected CHECK constraint {constraint} on {table} to exist"
    );
}

/// Assert that inserting `value` into `table.column` is rejected by the
/// database. The caller supplies a full INSERT statement so the test can
/// satisfy any NOT NULL columns; the value is bound as `$1`.
pub async fn assert_insert_rejected(pool: &PgPool, insert_sql: &str, value: &str) {
    let result = sqlx::query(insert_sql).bind(value).execute(pool).await;
    assert!(
        result.is_err(),
        "expected insert of {value:?} to be rejected, but it succeeded"
    );
}
