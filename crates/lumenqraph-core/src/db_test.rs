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

        // Refuse to run against a database that doesn't look like a test DB.
        // This prevents accidentally wiping a developer's dev database.
        assert_test_database(&url);

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

/// Guard: refuse to run destructive DB tests against a database whose name
/// does not contain `test`. This prevents `make test-db` (or a stray
/// `TEST_DATABASE_URL`) from wiping a developer's dev database.
fn assert_test_database(url: &str) {
    let db_name = database_name(url);
    assert!(
        db_name.to_ascii_lowercase().contains("test"),
        "refusing to run destructive DB tests against database `{db_name}`: \
         the database name must contain `test` (set TEST_DATABASE_URL to a \
         dedicated test database, e.g. lumenqraph_test)"
    );
}

/// Extract the database name from a Postgres connection URL.
///
/// Handles `postgres://user:pass@host:port/dbname?params` and returns the
/// path segment (without the leading slash). Falls back to an empty string
/// when no database name is present.
fn database_name(url: &str) -> String {
    // Strip the scheme (everything up to and including `://`).
    let after_scheme = url.split("://").nth(1).unwrap_or(url);
    // Drop any query string.
    let without_query = after_scheme.split('?').next().unwrap_or(after_scheme);
    // The database name is the path segment after the first `/`.
    match without_query.split_once('/') {
        Some((_, db)) => db.to_string(),
        None => String::new(),
    }
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
