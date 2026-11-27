//! The storage contract for Cork (HANDOFF section 5).
//!
//! An adapter stores events and projection rows and reads them back. It holds
//! no business rules. The server decides every change with `cork-core` inside
//! [`Store::write`]: the closure reads what it needs through [`WriteTx`],
//! decides, and returns a [`Commit`] of events plus the projection rows those
//! events produce. The adapter writes both in the same transaction.
//!
//! # Promises
//!
//! An adapter may assume there is exactly one writer process per board.
//!
//! 1. **Atomic write.** `write(f)` runs `f` inside one transaction. If `f`
//!    fails, or any part of the commit fails, nothing is written.
//! 2. **Conditional append, as a guard.** Each append names the version it
//!    expects. With one writer this always holds, so a mismatch is a bug: the
//!    adapter returns `internal` and writes nothing.
//! 3. **Global order.** Positions are unique and only go up. Versions within a
//!    stream are 1, 2, 3, … with no gaps, and so are positions: a commit's
//!    events get `head + 1`, `head + 2`, … in append order. A reader that has
//!    seen position N never later finds a new event below N.
//! 4. **Read after a position.** [`Store::read_events`] returns the events
//!    after position N that match a filter, in position order, from one read
//!    transaction, with the highest position that transaction saw.
//!
//! The suite in [`testkit`] (feature `testkit`) checks all of them.

use async_trait::async_trait;
use cork_core::{ChannelSelector, Message, Millis, Result, Task, TaskState};
use serde::{Deserialize, Serialize};

pub mod replay;
#[cfg(feature = "testkit")]
pub mod testkit;

pub type Position = u64;

/// The version a stream must be at for an append to go ahead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpectedVersion {
    /// Append whatever the version is.
    Any,
    /// The stream must not exist yet.
    NoStream,
    /// The stream must be at exactly this version (its event count).
    Exact(u64),
}

/// Index fields kept beside each event so filters never parse JSON.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventMeta {
    pub channel: Option<String>,
    pub thread_id: Option<String>,
    /// May repeat a key, such as one `board.mention` per handle.
    ///
    /// Events on a `task:{id}` stream store only `board.task`. When read back
    /// (and in what [`Store::write`] returns) they also carry the task's own
    /// labels, joined from the task, so `step=fix` matches task events
    /// without copying labels onto each one (HANDOFF 6.3).
    pub labels: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewEvent {
    pub event_type: String,
    /// The version of this type's `data` shape. Use `cork_core::schema::current`.
    pub schema: u32,
    pub data: serde_json::Value,
    pub actor: String,
    pub at: Millis,
    pub meta: EventMeta,
}

impl NewEvent {
    /// An event at the current schema for its type.
    pub fn new(
        event_type: impl Into<String>,
        data: serde_json::Value,
        actor: impl Into<String>,
        at: Millis,
        meta: EventMeta,
    ) -> Self {
        let event_type = event_type.into();
        Self {
            schema: cork_core::schema::current(&event_type),
            event_type,
            data,
            actor: actor.into(),
            at,
            meta,
        }
    }
}

/// An event as read back. `data` is always in the current shape: adapters run
/// `cork_core::schema::upcast` on every row they return.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredEvent {
    pub position: Position,
    pub stream_id: String,
    pub version: u64,
    pub event_type: String,
    pub schema: u32,
    pub data: serde_json::Value,
    pub actor: String,
    pub at: Millis,
    pub meta: EventMeta,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamAppend {
    pub stream_id: String,
    pub expected: ExpectedVersion,
    pub events: Vec<NewEvent>,
}

/// A stored answer to a request that carried an `Idempotency-Key`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    pub person: String,
    pub key: String,
    /// A hash of the method, path and body. Same key, different hash is
    /// `idempotency_mismatch`.
    pub request_hash: String,
    /// The response to replay.
    pub result: serde_json::Value,
    pub created_at: Millis,
}

/// A projection row written in the same transaction as the events.
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// Insert a message. Its position is that of the last event this commit
    /// appends to `thread:{thread_id}`.
    Message(Message),
    /// Insert or replace a task. On insert, its queue place
    /// (`created_position`) is the position of the first event this commit
    /// appends to `task:{id}`, and its labels and declared dependencies
    /// (`depends_on`, fixed at creation) are indexed.
    Task(Task),
    /// Set one label on a stored message, replacing any value for that key.
    SetMessageLabel {
        message_id: String,
        key: String,
        value: String,
    },
    /// Store an idempotent answer. Replaces a row with the same person and key.
    Idempotency(IdempotencyRecord),
}

/// What a write closure returns: the events to append and the rows they produce.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Commit {
    pub appends: Vec<StreamAppend>,
    pub projections: Vec<Projection>,
}

impl Commit {
    pub fn is_empty(&self) -> bool {
        self.appends.iter().all(|a| a.events.is_empty()) && self.projections.is_empty()
    }
}

/// Which events to read.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EventFilter {
    /// Events with a position greater than this.
    pub after: Position,
    /// 0 means the adapter's default page size.
    pub limit: usize,
    pub stream_id: Option<String>,
    pub channel: Option<ChannelSelector>,
    pub thread_id: Option<String>,
    /// Every pair must be on the event.
    pub labels: Vec<(String, String)>,
    /// If not empty, the event type must be one of these.
    pub types: Vec<String>,
}

impl EventFilter {
    pub fn after(after: Position, limit: usize) -> Self {
        Self {
            after,
            limit,
            ..Default::default()
        }
    }

    /// The same test an adapter's query makes, for events already in memory
    /// (the server's live fan-out uses this).
    pub fn matches(&self, e: &StoredEvent) -> bool {
        e.position > self.after
            && self.stream_id.as_ref().is_none_or(|s| *s == e.stream_id)
            && self
                .channel
                .as_ref()
                .is_none_or(|c| e.meta.channel.as_deref().is_some_and(|ch| c.matches(ch)))
            && self
                .thread_id
                .as_ref()
                .is_none_or(|t| e.meta.thread_id.as_ref() == Some(t))
            && self.labels.iter().all(|l| e.meta.labels.contains(l))
            && (self.types.is_empty() || self.types.contains(&e.event_type))
    }
}

/// A page of events and the highest position the read transaction saw.
/// `head` lets a reader resume after a page that matched nothing.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EventPage {
    pub events: Vec<StoredEvent>,
    pub head: Position,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MessageQuery {
    pub channel: Option<ChannelSelector>,
    pub thread_id: Option<String>,
    /// Every pair must be on the message. `board.task=<state>` matches the
    /// task's effective state at `now`, not the stored one.
    pub labels: Vec<(String, String)>,
    pub roots_only: bool,
    /// Messages with a position greater than this.
    pub after: Position,
    pub limit: usize,
    /// Newest first instead of oldest first.
    pub newest_first: bool,
    pub now: Millis,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredMessage {
    pub position: Position,
    /// On a task root, `board.task` is the effective state at the query's `now`.
    pub message: Message,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskQuery {
    pub channel: Option<ChannelSelector>,
    /// The effective state at `now`.
    pub state: Option<TaskState>,
    pub labels: Vec<(String, String)>,
    pub owner: Option<String>,
    /// If set, only tasks with no assignee or this assignee handle.
    pub claimable_by: Option<String>,
    /// Tasks created after this position. Results are in creation order.
    pub after: Position,
    pub limit: usize,
    pub now: Millis,
}

impl TaskQuery {
    /// The query `claim --ready` runs: the oldest task that is ready at `now`.
    pub fn ready(now: Millis) -> Self {
        Self {
            state: Some(TaskState::Ready),
            limit: 1,
            now,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelInfo {
    pub path: String,
    pub threads: u64,
    pub messages: u64,
    pub last_position: Position,
}

/// A stored task with its queue place.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredTask {
    pub created_position: Position,
    /// The stored state. Call `task.at(now)` for the state now.
    pub task: Task,
}

/// Reads available inside a write. They see the transaction's own view, which
/// is the latest committed state because there is one writer.
pub trait WriteTx {
    /// The highest committed position. This commit's events get `head + 1`,
    /// `head + 2`, … in append order (positions have no gaps).
    fn head(&mut self) -> Result<Position>;
    fn stream_version(&mut self, stream_id: &str) -> Result<u64>;
    fn get_message(&mut self, id: &str) -> Result<Option<Message>>;
    fn get_task(&mut self, id: &str) -> Result<Option<StoredTask>>;
    /// Tasks in creation order, filtered on effective state at `q.now`.
    fn list_tasks(&mut self, q: &TaskQuery) -> Result<Vec<StoredTask>>;
    /// Ids of tasks stored as claimed whose lease ended before `now`, soonest first.
    fn ended_leases(&mut self, now: Millis, limit: usize) -> Result<Vec<String>>;
    /// Ids of the tasks that declared a dependency on any of `ids`, oldest
    /// first (`created_position`), without repeats (HANDOFF 6.8.6).
    fn dependents(&mut self, ids: &[String]) -> Result<Vec<String>>;
    fn get_idempotency(&mut self, person: &str, key: &str) -> Result<Option<IdempotencyRecord>>;
}

#[async_trait]
pub trait Store: Send + Sync + 'static {
    /// Run `f` in one write transaction, then append and project its commit
    /// atomically. Returns the stored events in position order and `f`'s value.
    ///
    /// `f` runs on the writer's thread and must not wait on anything but the
    /// store. Writes run one at a time, in the order they were called.
    async fn write<T, F>(&self, f: F) -> Result<(Vec<StoredEvent>, T)>
    where
        T: Send + 'static,
        F: FnOnce(&mut dyn WriteTx) -> Result<(Commit, T)> + Send + 'static;

    async fn read_events(&self, filter: &EventFilter) -> Result<EventPage>;
    /// The highest committed position, or 0.
    async fn head(&self) -> Result<Position>;
    /// The largest `at` in the log, used to anchor the daemon clock.
    async fn max_at(&self) -> Result<Option<Millis>>;
    async fn stream_version(&self, stream_id: &str) -> Result<u64>;

    /// A message, with `board.task` as the effective state at `now` on a task root.
    async fn get_message(&self, id: &str, now: Millis) -> Result<Option<Message>>;
    async fn list_messages(&self, q: &MessageQuery) -> Result<Vec<StoredMessage>>;
    async fn list_channels(&self) -> Result<Vec<ChannelInfo>>;

    async fn get_task(&self, id: &str) -> Result<Option<StoredTask>>;
    async fn list_tasks(&self, q: &TaskQuery) -> Result<Vec<StoredTask>>;
    /// The earliest `expires_at` among tasks stored as claimed.
    async fn next_lease_end(&self) -> Result<Option<Millis>>;
    /// Ids of every task stored as claimed (restart grace walks these).
    async fn claimed_tasks(&self) -> Result<Vec<String>>;
    /// As [`WriteTx::dependents`], for reads.
    async fn dependents(&self, ids: &[String]) -> Result<Vec<String>>;

    /// Delete idempotency rows created before `before`. Returns how many.
    async fn prune_idempotency(&self, before: Millis) -> Result<u64>;
    /// Flush and shrink any write-ahead log. Called when idle.
    async fn checkpoint(&self) -> Result<()>;

    /// Load an exported log into an **empty** store, keeping every position,
    /// and build the projections from it (HANDOFF 13.4). The only write that
    /// core does not decide; the daemon runs it at start-up only.
    async fn import(&self, events: Vec<StoredEvent>) -> Result<()>;
}
