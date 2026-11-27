//! Cork stress tests (HANDOFF 11, item 13).
//!
//! Each scenario runs against a daemon over HTTP, then checks the log
//! invariants and prints throughput and p50 / p99 latency. By default the
//! daemon runs in this process over a fresh SQLite file; build with
//! `--release` for real numbers.

pub mod client;
pub mod daemon;
pub mod invariants;
pub mod lead;
pub mod scenarios;
pub mod stats;

pub use daemon::{Daemon, Sync};
pub use scenarios::{Report, Scenario};
