//! Worker fleets for Cork (HANDOFF 16).
//!
//! A fleet is a plain client. Models, pools and keywords live only here and
//! in the CLI: the board stores and filters ordinary labels, as in
//! Milestone 1, and knows nothing of models.

pub mod catalog;
pub mod config;
pub mod escalation;
pub mod routing;
pub mod supervisor;

pub use catalog::{Catalog, Model, Needs, UnknownKeyword};
pub use config::{Fleet, Pool};
pub use routing::{TaskFacts, Verdict};
