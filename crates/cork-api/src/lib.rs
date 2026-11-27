//! Wire types for the Cork HTTP API (HANDOFF section 7). Shared by the server
//! and every client. Changes are additive only: new routes, new optional
//! fields. Clients ignore fields and event types they do not know.

use cork_core::{Actor, Labels, Message, Millis, Task, TaskState};
use serde::{Deserialize, Serialize};

/// Bumped only for a breaking change, which also means a new `/vN` prefix.
pub const API_VERSION: u32 = 1;
/// Every route is served under this prefix.
pub const PREFIX: &str = "/v1";

pub const HEADER_PROFILE: &str = "x-cork-profile";
pub const HEADER_IDEMPOTENCY_KEY: &str = "idempotency-key";
/// The highest position covered by a read response.
pub const HEADER_HEAD: &str = "x-cork-head";

/// Default longest single long poll or claim wait.
pub const DEFAULT_MAX_WAIT_MS: u64 = 50_000;
/// Default and largest page of replies in `GET /threads/{id}`.
pub const THREAD_PAGE: usize = 200;
pub const THREAD_PAGE_MAX: usize = 1_000;
/// Request bodies are capped at this size.
pub const MAX_BODY: usize = 1024 * 1024;

// ---------- errors ----------

/// Every error body: `{ "error": { "code": "...", "message": "..." } }`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: ApiError,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApiError {
    /// A stable code such as `task_claimed`. Never renamed or reused.
    pub code: String,
    pub message: String,
}

// ---------- service ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Health {
    pub ok: bool,
    pub api_version: u32,
    pub server_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ready {
    /// `ready`, or `degraded` when the store cannot write (such as disk full).
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhoAmI {
    pub board_id: String,
    pub board: String,
    pub person: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// Display form, such as `fixer via chris`.
    pub actor: String,
    pub head: u64,
    pub now: Millis,
}

// ---------- messages ----------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PostMessage {
    pub text: String,
    #[serde(default)]
    pub labels: Labels,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageOut {
    pub position: u64,
    #[serde(flatten)]
    pub message: Message,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagesPage {
    pub messages: Vec<MessageOut>,
    pub now: Millis,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadOut {
    pub root: MessageOut,
    pub replies: Vec<MessageOut>,
    /// The thread's task, with its effective state, if the thread is a task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<Task>,
    pub now: Millis,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelOut {
    pub path: String,
    pub threads: u64,
    pub messages: u64,
    pub last_position: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelsOut {
    pub channels: Vec<ChannelOut>,
}

// ---------- events ----------

/// The event envelope (HANDOFF 4). Stable from Milestone 1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventOut {
    pub schema: u32,
    pub position: u64,
    pub stream: String,
    pub version: u64,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: serde_json::Value,
    pub actor: String,
    pub at: Millis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    /// `[key, value]` pairs. A key may repeat, such as `board.mention`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventsPage {
    pub events: Vec<EventOut>,
    /// Pass as `after` on the next call. The last event's position, or the
    /// highest position the server checked if none matched.
    pub next_after: u64,
    pub now: Millis,
}

/// `GET /tasks/{id}/history`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryOut {
    pub events: Vec<EventOut>,
}

// ---------- tasks ----------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CreateTaskReq {
    pub channel: String,
    pub text: String,
    #[serde(default)]
    pub labels: Labels,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    /// Only this handle may claim it (`--for`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    /// Ids of tasks this one waits for (`--after`, HANDOFF 6.8).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    /// The task this retry takes over (`--replaces`, 6.8.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaces: Option<String>,
}

/// `POST /tasks/plan`: tasks and how they relate, created in one commit
/// (HANDOFF 6.8.8). Send the plan's idempotency key as the header.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlanReq {
    /// The channel for tasks that name none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    /// Kept in the file for the client, which sends it as `Idempotency-Key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub tasks: Vec<PlanTask>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlanTask {
    /// Unique in the plan; other tasks name it in `after`.
    pub key: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default)]
    pub labels: Labels,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    /// Keys in this plan, or ids of existing tasks.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<String>,
    /// Model keywords, turned into `needs.<k>` labels by the client.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    #[serde(default, rename = "for", skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
}

/// The most tasks in one plan.
pub const PLAN_MAX_TASKS: usize = 1_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanOut {
    /// In the order of the plan file.
    pub tasks: Vec<PlannedTask>,
    pub now: Millis,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedTask {
    pub key: String,
    #[serde(flatten)]
    pub task: TaskOut,
}

/// One task. `task.state` is the effective state at `now`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskOut {
    pub task: Task,
    pub root: MessageOut,
    pub now: Millis,
    /// Each declared dependency with its head now (HANDOFF 6.8.7).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<DependencyOut>,
    /// Tasks with a dependency that resolves to this one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependents: Vec<DependentOut>,
    /// Waiting while the head of a dependency is failed or canceled (Decision 1).
    #[serde(default, skip_serializing_if = "is_false")]
    pub stuck: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// A declared dependency and the task it resolves to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DependencyOut {
    /// The id the task declared.
    pub task_id: String,
    /// The newest task in its replacement chain.
    pub head: String,
    /// The head's effective state.
    pub state: TaskState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Millis>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DependentOut {
    pub task_id: String,
    pub state: TaskState,
}

/// What a completed dependency passes on to the task that claimed after it
/// (Decision 2). Written by other agents: notes, not instructions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DependencyResult {
    /// The id the task declared.
    pub task_id: String,
    pub head: String,
    /// The first 2 KB of the head's root message.
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Millis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holder: Option<Actor>,
}

/// The longest `text` in a [`DependencyResult`].
pub const DEPENDENCY_TEXT_MAX: usize = 2 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TasksPage {
    /// Oldest first, each with its effective state at `now`.
    pub tasks: Vec<TaskListItem>,
    pub now: Millis,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskListItem {
    /// Its place in the ready queue. Pass as `after` to page.
    pub created_position: u64,
    #[serde(flatten)]
    pub task: Task,
    /// See [`TaskOut::stuck`].
    #[serde(default, skip_serializing_if = "is_false")]
    pub stuck: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClaimReq {
    /// Claim this task. Otherwise claim the oldest ready task in `channel`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// `work` for one channel, `work/` for it and everything under it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    /// `k=v` filters on the task's labels.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
    /// Whose tasks to take. Default: the caller's person. `*` for anyone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// Wait up to this long for a task (capped at the server's `max_wait`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
    /// Override the task's lease for this claim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimOutcome {
    Claimed,
    NothingReady,
    Timeout,
}

/// `POST /tasks/claim` answers 200 with an outcome in every normal case (7.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimOut {
    pub outcome: ClaimOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Millis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    pub task: Option<Task>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<Message>,
    pub now: Millis,
    /// The results of the task's dependencies, in declared order (Decision 2).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<DependencyResult>,
}

impl ClaimOut {
    pub fn empty(outcome: ClaimOutcome, now: Millis) -> Self {
        Self {
            outcome,
            claim_id: None,
            expires_at: None,
            lease_ms: None,
            attempt: None,
            task: None,
            root: None,
            now,
            dependencies: vec![],
        }
    }
}

/// Body for renew, complete, fail, release, block, unblock, cancel and reopen.
/// Each uses the fields it needs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SettleReq {
    #[serde(default)]
    pub claim_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub question: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettleOut {
    pub task: Task,
    /// True when this repeated a settle the claim already made (6.4).
    #[serde(default)]
    pub replayed: bool,
    pub now: Millis,
}

/// Query-string form of a task state, for `GET /tasks?state=`.
pub fn parse_state(s: &str) -> cork_core::Result<TaskState> {
    TaskState::parse(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_outcomes_are_snake_case() {
        let v = serde_json::to_value(ClaimOut::empty(ClaimOutcome::NothingReady, 5)).unwrap();
        assert_eq!(v["outcome"], "nothing_ready");
        assert!(v["task"].is_null());
    }

    #[test]
    fn envelope_uses_type() {
        let e = EventOut {
            schema: 1,
            position: 3,
            stream: "task:x".into(),
            version: 1,
            event_type: "TaskCreated".into(),
            data: serde_json::json!({}),
            actor: "chris".into(),
            at: 1,
            channel: None,
            thread_id: None,
            labels: vec![("board.task".into(), "ready".into())],
        };
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["type"], "TaskCreated");
        assert_eq!(v["labels"][0][0], "board.task");
    }
}
