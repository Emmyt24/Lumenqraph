//! Shared types and logic used by every Lumenqraph service. Defining the event
//! schema and decoding once here means services can never drift.

// Ban unaudited panics in library code; test code is explicitly exempted below.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used, clippy::panic))]
// Test helpers are full of intentional panics and unwraps — that's fine.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod codegen;
pub mod crypto;
/// Helpers for isolated-schema Postgres tests. Gated so it compiles only when
/// running tests (the `uuid` dep is always present in the workspace).
#[cfg(any(test, feature = "db-test"))]
pub mod db_test;
pub mod diff;
pub mod error;
pub mod models;
pub mod read;
pub mod sanitize;
pub mod spec;
pub mod url_validation;
pub mod xdr;

pub use diff::SpecDiff;
pub use error::{Error, Result};
pub use models::{
    ApiKey,
    AmmSwap,
    Contract,
    ContractDurability,
    EventRow,
    EventType,
    IndexerStatus,
    LiquidityEvent,
    LiquidityEventKind,
    NewEvent,
    NftEvent,
    NftEventKind,
    TokenTransfer,
    WebhookDeliveryStatus,
    WebhookSubscription,
    WebhookSubscriptionKind,
};
pub use spec::ContractSpec;
pub use xdr::is_valid_contract_id;
pub use xdr::parse_contract_ids;
