//! Domain model for Cork.
//!
//! This crate holds every business rule: validation, task state changes, lease
//! checks and rejection codes. It performs no I/O and never reads a clock.
//! Callers pass `now` (milliseconds since the Unix epoch) into every decision.

pub mod actor;
pub mod channel;
pub mod error;
pub mod labels;
pub mod message;
pub mod schema;
pub mod task;

pub use actor::Actor;
pub use channel::{ChannelSelector, normalize_channel};
pub use error::{BoardError, ErrorCode, Result};
pub use labels::Labels;
pub use message::{Message, NewMessage};
pub use task::{Claim, CreateTask, Decision, LastSettle, SettleOp, Task, TaskEvent, TaskState};

/// Milliseconds since the Unix epoch, as read by the daemon's clock.
pub type Millis = i64;

/// Stream id helpers. A task's thread and its state live in separate streams so
/// that replies never bump the task's version (and never fail a claim).
pub fn thread_stream(thread_id: &str) -> String {
    format!("thread:{thread_id}")
}
pub fn task_stream(task_id: &str) -> String {
    format!("task:{task_id}")
}
