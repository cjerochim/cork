//! Task state and every rule for changing it.
//!
//! A task is a thread with board-kept state. The state lives in its own
//! `task:{id}` stream, separate from the thread's messages, so a reply never
//! changes the task's version.
//!
//! Each command is a pure decision: `(&Task, now, input) -> Result<Decision>`.
//! The server runs the decision inside its single write transaction and
//! appends the decision's events (HANDOFF 3, 6.4).

use crate::actor::{Actor, normalize_name};
use crate::error::{BoardError, ErrorCode, Result};
use crate::labels::Labels;
use crate::{Millis, normalize_channel};
use serde::{Deserialize, Serialize};
use std::fmt;

pub const DEFAULT_LEASE_MS: i64 = 5 * 60 * 1000;
pub const MIN_LEASE_MS: i64 = 100;
pub const MAX_LEASE_MS: i64 = 60 * 60 * 1000;
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;
pub const MAX_MAX_ATTEMPTS: u32 = 100;
pub const MAX_RESULT: usize = 64 * 1024;
/// The `reason` on the `LeaseRenewed` the daemon writes at start (HANDOFF 6.4).
pub const RESTART_GRACE_REASON: &str = "daemon_restart";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    /// A task it needs has not completed. (Milestone 2.)
    Waiting,
    Ready,
    Claimed,
    Blocked,
    Completed,
    Failed,
    Canceled,
}

impl TaskState {
    pub const ALL: [TaskState; 7] = [
        TaskState::Waiting,
        TaskState::Ready,
        TaskState::Claimed,
        TaskState::Blocked,
        TaskState::Completed,
        TaskState::Failed,
        TaskState::Canceled,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            TaskState::Waiting => "waiting",
            TaskState::Ready => "ready",
            TaskState::Claimed => "claimed",
            TaskState::Blocked => "blocked",
            TaskState::Completed => "completed",
            TaskState::Failed => "failed",
            TaskState::Canceled => "canceled",
        }
    }
    pub fn parse(s: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|t| t.as_str() == s)
            .ok_or_else(|| BoardError::invalid(format!("unknown task state `{s}`")))
    }
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskState::Completed | TaskState::Failed | TaskState::Canceled
        )
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
    pub claim_id: String,
    pub holder: Actor,
    pub claimed_at: Millis,
    pub expires_at: Millis,
    /// Lease length for this claim. Each renew extends by this much from now.
    pub lease_ms: i64,
    /// 1 for the first claim. Released claims do not count.
    pub attempt: u32,
}

/// The four settles that end a claim. A repeat by the same claim is a replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettleOp {
    Complete,
    Fail,
    Release,
    Block,
}

/// The last settle, kept so a retried settle can be answered (HANDOFF 6.4).
/// Cleared when the task is claimed again or reopened.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastSettle {
    pub claim_id: String,
    pub op: SettleOp,
    pub at: Millis,
}

/// Events in a `task:{id}` stream. Every type is at schema version 1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TaskEvent {
    TaskCreated {
        task_id: String,
        channel: String,
        owner: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        assignee: Option<String>,
        lease_ms: i64,
        max_attempts: u32,
        #[serde(default)]
        labels: Labels,
    },
    TaskClaimed {
        claim_id: String,
        holder: Actor,
        expires_at: Millis,
        attempt: u32,
    },
    LeaseRenewed {
        claim_id: String,
        expires_at: Millis,
        /// Set when the daemon renews, such as `daemon_restart`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// A lease ended. Written by the first decision that finds it, or by the
    /// daemon's lease timer. The task is `ready` after it; if its attempts are
    /// used up, a `TaskFailed` follows in the same commit.
    LeaseExpired {
        claim_id: String,
    },
    TaskReleased {
        claim_id: String,
    },
    TaskCompleted {
        claim_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<String>,
    },
    TaskFailed {
        /// `None` is never written today; kept optional for owner-side fails later.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        claim_id: Option<String>,
        reason: String,
    },
    TaskBlocked {
        claim_id: String,
        question: String,
    },
    TaskUnblocked {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
    TaskCanceled {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    TaskReopened {},
    /// Milestone 2. Reserved so the log format is stable.
    NeedsChanged {
        needs: Vec<String>,
    },
}

impl TaskEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            TaskEvent::TaskCreated { .. } => "TaskCreated",
            TaskEvent::TaskClaimed { .. } => "TaskClaimed",
            TaskEvent::LeaseRenewed { .. } => "LeaseRenewed",
            TaskEvent::LeaseExpired { .. } => "LeaseExpired",
            TaskEvent::TaskReleased { .. } => "TaskReleased",
            TaskEvent::TaskCompleted { .. } => "TaskCompleted",
            TaskEvent::TaskFailed { .. } => "TaskFailed",
            TaskEvent::TaskBlocked { .. } => "TaskBlocked",
            TaskEvent::TaskUnblocked { .. } => "TaskUnblocked",
            TaskEvent::TaskCanceled { .. } => "TaskCanceled",
            TaskEvent::TaskReopened { .. } => "TaskReopened",
            TaskEvent::NeedsChanged { .. } => "NeedsChanged",
        }
    }
}

/// What a decision returns. A replay writes nothing and answers with the
/// task as it is.
#[derive(Debug, Clone, PartialEq)]
pub struct Decision {
    pub events: Vec<TaskEvent>,
    pub replayed: bool,
}

impl Decision {
    fn of(events: Vec<TaskEvent>) -> Self {
        Self {
            events,
            replayed: false,
        }
    }
    fn replay() -> Self {
        Self {
            events: vec![],
            replayed: true,
        }
    }
}

/// Input to create a task. The root message is posted alongside.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CreateTask {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    /// Only this handle may claim it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
}

/// A task's state, folded from its stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub channel: String,
    /// The person behind the actor who created the task.
    pub owner: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    pub lease_ms: i64,
    pub max_attempts: u32,
    pub labels: Labels,
    /// The stored state. Use [`Task::effective_state`] for what it is now.
    pub state: TaskState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim: Option<Claim>,
    /// Claims that did not end in a release.
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub question: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_settle: Option<LastSettle>,
    /// Stream version: the number of events folded in.
    pub version: u64,
    pub created_at: Millis,
    pub updated_at: Millis,
}

fn exhausted_reason(max: u32) -> String {
    format!("lease expired with all {max} attempts used")
}

fn check_lease(lease: i64) -> Result<()> {
    if (MIN_LEASE_MS..=MAX_LEASE_MS).contains(&lease) {
        Ok(())
    } else {
        Err(BoardError::invalid(format!(
            "lease must be between {MIN_LEASE_MS} ms and {MAX_LEASE_MS} ms"
        )))
    }
}

impl Task {
    /// Decide `TaskCreated`. `labels` must already be normalized (they come
    /// from the root message).
    pub fn create(
        task_id: String,
        channel: &str,
        owner: &Actor,
        labels: Labels,
        input: &CreateTask,
    ) -> Result<TaskEvent> {
        let lease_ms = input.lease_ms.unwrap_or(DEFAULT_LEASE_MS);
        check_lease(lease_ms)?;
        let max_attempts = input.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS);
        if !(1..=MAX_MAX_ATTEMPTS).contains(&max_attempts) {
            return Err(BoardError::invalid(format!(
                "max_attempts must be 1-{MAX_MAX_ATTEMPTS}"
            )));
        }
        let assignee = match &input.assignee {
            Some(a) => Some(normalize_name("assignee", a)?),
            None => None,
        };
        Ok(TaskEvent::TaskCreated {
            task_id,
            channel: normalize_channel(channel)?,
            owner: owner.person.clone(),
            assignee,
            lease_ms,
            max_attempts,
            labels,
        })
    }

    /// Fold a whole stream. The first event must be `TaskCreated`.
    pub fn from_events<'a>(
        events: impl IntoIterator<Item = (&'a TaskEvent, Millis)>,
    ) -> Result<Task> {
        let mut it = events.into_iter();
        let (first, at) = it
            .next()
            .ok_or_else(|| BoardError::internal("empty task stream"))?;
        let mut task = Task::genesis(first, at)?;
        for (e, at) in it {
            task.apply(e, at);
        }
        Ok(task)
    }

    pub fn genesis(e: &TaskEvent, at: Millis) -> Result<Task> {
        match e {
            TaskEvent::TaskCreated {
                task_id,
                channel,
                owner,
                assignee,
                lease_ms,
                max_attempts,
                labels,
            } => Ok(Task {
                id: task_id.clone(),
                channel: channel.clone(),
                owner: owner.clone(),
                assignee: assignee.clone(),
                lease_ms: *lease_ms,
                max_attempts: *max_attempts,
                labels: labels.clone(),
                state: TaskState::Ready,
                claim: None,
                attempts: 0,
                result: None,
                reason: None,
                question: None,
                last_settle: None,
                version: 1,
                created_at: at,
                updated_at: at,
            }),
            other => Err(BoardError::internal(format!(
                "task stream starts with {}",
                other.event_type()
            ))),
        }
    }

    fn settled(&mut self, claim_id: &str, op: SettleOp, at: Millis) {
        self.last_settle = Some(LastSettle {
            claim_id: claim_id.to_string(),
            op,
            at,
        });
        self.claim = None;
    }

    /// Apply an event. Never fails: events are facts.
    pub fn apply(&mut self, e: &TaskEvent, at: Millis) {
        self.version += 1;
        self.updated_at = at;
        match e {
            TaskEvent::TaskCreated { .. } | TaskEvent::NeedsChanged { .. } => {}
            TaskEvent::TaskClaimed {
                claim_id,
                holder,
                expires_at,
                attempt,
            } => {
                self.state = TaskState::Claimed;
                self.attempts = *attempt;
                self.last_settle = None;
                self.claim = Some(Claim {
                    claim_id: claim_id.clone(),
                    holder: holder.clone(),
                    claimed_at: at,
                    expires_at: *expires_at,
                    lease_ms: *expires_at - at,
                    attempt: *attempt,
                });
            }
            TaskEvent::LeaseRenewed { expires_at, .. } => {
                if let Some(c) = &mut self.claim {
                    c.expires_at = *expires_at;
                }
            }
            TaskEvent::LeaseExpired { .. } => {
                self.claim = None;
                self.state = TaskState::Ready;
            }
            TaskEvent::TaskReleased { claim_id } => {
                self.settled(claim_id, SettleOp::Release, at);
                self.state = TaskState::Ready;
                self.attempts = self.attempts.saturating_sub(1);
            }
            TaskEvent::TaskCompleted { claim_id, result } => {
                self.settled(claim_id, SettleOp::Complete, at);
                self.state = TaskState::Completed;
                self.result = result.clone();
            }
            TaskEvent::TaskFailed { claim_id, reason } => {
                if let Some(c) = claim_id
                    .as_deref()
                    .filter(|c| self.claim.as_ref().is_some_and(|k| k.claim_id == *c))
                {
                    self.settled(c, SettleOp::Fail, at);
                }
                self.claim = None;
                self.state = TaskState::Failed;
                self.reason = Some(reason.clone());
            }
            TaskEvent::TaskBlocked { claim_id, question } => {
                self.settled(claim_id, SettleOp::Block, at);
                self.state = TaskState::Blocked;
                self.question = Some(question.clone());
            }
            TaskEvent::TaskUnblocked { .. } => {
                self.state = TaskState::Ready;
            }
            TaskEvent::TaskCanceled { reason } => {
                self.claim = None;
                self.state = TaskState::Canceled;
                self.reason = reason.clone();
            }
            TaskEvent::TaskReopened {} => {
                self.state = TaskState::Ready;
                self.attempts = 0;
                self.result = None;
                self.reason = None;
                self.question = None;
                self.last_settle = None;
            }
        }
    }

    /// The claim, if it is still live at `now`.
    pub fn live_claim(&self, now: Millis) -> Option<&Claim> {
        match (&self.state, &self.claim) {
            (TaskState::Claimed, Some(c)) if now <= c.expires_at => Some(c),
            _ => None,
        }
    }

    /// True if the stored state is `claimed` but the lease is over.
    pub fn lease_ended(&self, now: Millis) -> bool {
        matches!((&self.state, &self.claim), (TaskState::Claimed, Some(c)) if now > c.expires_at)
    }

    /// The state at `now`. A claimed task whose lease ended is `ready` again,
    /// or `failed` if it has used all its attempts (HANDOFF 5.3).
    pub fn effective_state(&self, now: Millis) -> TaskState {
        if self.lease_ended(now) {
            if self.attempts >= self.max_attempts {
                TaskState::Failed
            } else {
                TaskState::Ready
            }
        } else {
            self.state
        }
    }

    /// The events that record an ended lease, or none (HANDOFF 6.4, 6.6).
    pub fn lapse_events(&self, now: Millis) -> Vec<TaskEvent> {
        match &self.claim {
            Some(c) if self.lease_ended(now) => {
                let mut out = vec![TaskEvent::LeaseExpired {
                    claim_id: c.claim_id.clone(),
                }];
                if self.attempts >= self.max_attempts {
                    out.push(TaskEvent::TaskFailed {
                        claim_id: None,
                        reason: exhausted_reason(self.max_attempts),
                    });
                }
                out
            }
            _ => vec![],
        }
    }

    /// The task as it is at `now`, for reads: an ended lease is folded in.
    pub fn at(&self, now: Millis) -> Task {
        let mut t = self.clone();
        let version = t.version;
        for e in self.lapse_events(now) {
            t.apply(&e, now);
        }
        // A read does not write, so the version stays the stored one.
        t.version = version;
        t
    }

    /// Record any ended lease, then decide on the result.
    fn decide(&self, now: Millis, f: impl FnOnce(&Task) -> Result<Decision>) -> Result<Decision> {
        let lapse = self.lapse_events(now);
        if lapse.is_empty() {
            return f(self);
        }
        let mut t = self.clone();
        for e in &lapse {
            t.apply(e, now);
        }
        let d = f(&t)?;
        if d.replayed {
            return Ok(d);
        }
        let mut events = lapse;
        events.extend(d.events);
        Ok(Decision::of(events))
    }

    /// Is `claim_id` the live claim, or a repeat of the settle it already made?
    fn check_settle(&self, now: Millis, claim_id: &str, op: Option<SettleOp>) -> Result<bool> {
        if claim_id.is_empty() {
            return Err(BoardError::new(
                ErrorCode::ClaimNotHeld,
                "no claim on this task",
            ));
        }
        if self.live_claim(now).is_some_and(|c| c.claim_id == claim_id) {
            return Ok(false);
        }
        if let (Some(op), Some(last)) = (op, &self.last_settle)
            && last.claim_id == claim_id
            && last.op == op
        {
            return Ok(true);
        }
        Err(BoardError::new(
            ErrorCode::ClaimLost,
            format!(
                "claim {claim_id} is not the live claim on task {} (now {})",
                self.id,
                self.effective_state(now)
            ),
        ))
    }

    fn require_owner(&self, actor: &Actor) -> Result<()> {
        if actor.person == self.owner {
            Ok(())
        } else {
            Err(BoardError::new(
                ErrorCode::TaskNotOwned,
                format!("task {} is owned by {}", self.id, self.owner),
            ))
        }
    }

    pub fn can_be_claimed_by(&self, holder: &Actor) -> bool {
        self.assignee
            .as_deref()
            .is_none_or(|a| a == holder.handle())
    }

    pub fn claim(
        &self,
        now: Millis,
        holder: &Actor,
        claim_id: String,
        lease_ms: Option<i64>,
    ) -> Result<Decision> {
        self.decide(now, |t| {
            match t.state {
                TaskState::Ready => {}
                TaskState::Claimed => {
                    let c = t.claim.as_ref().expect("claimed task has a claim");
                    return Err(BoardError::new(
                        ErrorCode::TaskClaimed,
                        format!(
                            "task {} is claimed by {} until {}",
                            t.id, c.holder, c.expires_at
                        ),
                    ));
                }
                s => {
                    return Err(BoardError::new(
                        ErrorCode::InvalidState,
                        format!("task {} is {s}", t.id),
                    ));
                }
            }
            if !t.can_be_claimed_by(holder) {
                return Err(BoardError::new(
                    ErrorCode::Forbidden,
                    format!(
                        "task {} is for {}",
                        t.id,
                        t.assignee.as_deref().unwrap_or_default()
                    ),
                ));
            }
            let lease = lease_ms.unwrap_or(t.lease_ms);
            check_lease(lease)?;
            Ok(Decision::of(vec![TaskEvent::TaskClaimed {
                claim_id,
                holder: holder.clone(),
                expires_at: now + lease,
                attempt: t.attempts + 1,
            }]))
        })
    }

    pub fn renew(&self, now: Millis, claim_id: &str) -> Result<Decision> {
        self.decide(now, |t| {
            t.check_settle(now, claim_id, None)?;
            let c = t.claim.as_ref().expect("live claim");
            Ok(Decision::of(vec![TaskEvent::LeaseRenewed {
                claim_id: claim_id.to_string(),
                expires_at: now + c.lease_ms.clamp(MIN_LEASE_MS, MAX_LEASE_MS),
                reason: None,
            }]))
        })
    }

    pub fn complete(
        &self,
        now: Millis,
        claim_id: &str,
        result: Option<String>,
    ) -> Result<Decision> {
        self.decide(now, |t| {
            if t.check_settle(now, claim_id, Some(SettleOp::Complete))? {
                return Ok(Decision::replay());
            }
            if result.as_ref().is_some_and(|r| r.len() > MAX_RESULT) {
                return Err(BoardError::invalid(format!(
                    "result is over {MAX_RESULT} bytes"
                )));
            }
            Ok(Decision::of(vec![TaskEvent::TaskCompleted {
                claim_id: claim_id.to_string(),
                result,
            }]))
        })
    }

    pub fn fail(&self, now: Millis, claim_id: &str, reason: String) -> Result<Decision> {
        self.decide(now, |t| {
            if t.check_settle(now, claim_id, Some(SettleOp::Fail))? {
                return Ok(Decision::replay());
            }
            if reason.trim().is_empty() {
                return Err(BoardError::invalid("a reason is required"));
            }
            if reason.len() > MAX_RESULT {
                return Err(BoardError::invalid(format!(
                    "reason is over {MAX_RESULT} bytes"
                )));
            }
            Ok(Decision::of(vec![TaskEvent::TaskFailed {
                claim_id: Some(claim_id.to_string()),
                reason,
            }]))
        })
    }

    pub fn release(&self, now: Millis, claim_id: &str) -> Result<Decision> {
        self.decide(now, |t| {
            if t.check_settle(now, claim_id, Some(SettleOp::Release))? {
                return Ok(Decision::replay());
            }
            Ok(Decision::of(vec![TaskEvent::TaskReleased {
                claim_id: claim_id.to_string(),
            }]))
        })
    }

    pub fn block(&self, now: Millis, claim_id: &str, question: String) -> Result<Decision> {
        self.decide(now, |t| {
            if t.check_settle(now, claim_id, Some(SettleOp::Block))? {
                return Ok(Decision::replay());
            }
            if question.trim().is_empty() {
                return Err(BoardError::invalid("a question is required"));
            }
            if question.len() > MAX_RESULT {
                return Err(BoardError::invalid(format!(
                    "question is over {MAX_RESULT} bytes"
                )));
            }
            Ok(Decision::of(vec![TaskEvent::TaskBlocked {
                claim_id: claim_id.to_string(),
                question,
            }]))
        })
    }

    pub fn unblock(&self, now: Millis, actor: &Actor, note: Option<String>) -> Result<Decision> {
        self.decide(now, |t| {
            t.require_owner(actor)?;
            match t.state {
                TaskState::Blocked => Ok(Decision::of(vec![TaskEvent::TaskUnblocked { note }])),
                s => Err(BoardError::new(
                    ErrorCode::InvalidState,
                    format!("task {} is {s}, not blocked", t.id),
                )),
            }
        })
    }

    pub fn cancel(&self, now: Millis, actor: &Actor, reason: Option<String>) -> Result<Decision> {
        self.decide(now, |t| {
            t.require_owner(actor)?;
            if t.state.is_terminal() {
                return Err(BoardError::new(
                    ErrorCode::InvalidState,
                    format!("task {} is already {}", t.id, t.state),
                ));
            }
            Ok(Decision::of(vec![TaskEvent::TaskCanceled { reason }]))
        })
    }

    pub fn reopen(&self, now: Millis, actor: &Actor) -> Result<Decision> {
        self.decide(now, |t| {
            t.require_owner(actor)?;
            if !t.state.is_terminal() {
                return Err(BoardError::new(
                    ErrorCode::InvalidState,
                    format!("task {} is {}; only finished tasks reopen", t.id, t.state),
                ));
            }
            Ok(Decision::of(vec![TaskEvent::TaskReopened {}]))
        })
    }

    /// For the daemon's lease timer: record a lease that has ended.
    pub fn expire(&self, now: Millis) -> Result<Decision> {
        let events = self.lapse_events(now);
        if events.is_empty() {
            return Err(BoardError::new(
                ErrorCode::InvalidState,
                format!("task {} has no ended lease", self.id),
            ));
        }
        Ok(Decision::of(events))
    }

    /// At daemon start: give a stored claim one lease of grace from `start`,
    /// so workers that were alive while the daemon was down keep their task.
    pub fn restart_grace(&self, start: Millis) -> Result<Decision> {
        match (&self.state, &self.claim) {
            (TaskState::Claimed, Some(c)) if start + c.lease_ms > c.expires_at => {
                Ok(Decision::of(vec![TaskEvent::LeaseRenewed {
                    claim_id: c.claim_id.clone(),
                    expires_at: start + c.lease_ms,
                    reason: Some(RESTART_GRACE_REASON.to_string()),
                }]))
            }
            _ => Err(BoardError::new(
                ErrorCode::InvalidState,
                format!("task {} needs no grace", self.id),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: Millis = 1_000_000;
    const LEASE: i64 = 1_000;

    fn owner() -> Actor {
        Actor::new("chris", Some("lead".into())).unwrap()
    }
    fn worker(n: &str) -> Actor {
        Actor::new("chris", Some(n.into())).unwrap()
    }
    fn stranger() -> Actor {
        Actor::person("dana")
    }

    fn new_task(max_attempts: u32) -> Task {
        let e = Task::create(
            "t1".into(),
            "work/fix",
            &owner(),
            Labels::new(),
            &CreateTask {
                lease_ms: Some(LEASE),
                max_attempts: Some(max_attempts),
                assignee: None,
            },
        )
        .unwrap();
        Task::genesis(&e, T0).unwrap()
    }

    /// Apply a decision; return its events.
    fn run(t: &mut Task, at: Millis, d: Result<Decision>) -> Result<Vec<TaskEvent>> {
        let d = d?;
        for e in &d.events {
            t.apply(e, at);
        }
        Ok(d.events)
    }

    fn code<T: std::fmt::Debug>(r: Result<T>) -> ErrorCode {
        r.unwrap_err().code
    }

    fn types(events: &[TaskEvent]) -> Vec<&'static str> {
        events.iter().map(|e| e.event_type()).collect()
    }

    fn claimed(max_attempts: u32) -> Task {
        let mut t = new_task(max_attempts);
        let d = t.claim(T0, &worker("w1"), "c1".into(), None);
        run(&mut t, T0, d).unwrap();
        t
    }

    #[test]
    fn create_defaults_and_validation() {
        let e = Task::create(
            "t".into(),
            "dev",
            &owner(),
            Labels::new(),
            &CreateTask::default(),
        )
        .unwrap();
        let t = Task::genesis(&e, T0).unwrap();
        assert_eq!(t.state, TaskState::Ready);
        assert_eq!(t.owner, "chris");
        assert_eq!(t.lease_ms, DEFAULT_LEASE_MS);
        assert_eq!(t.max_attempts, DEFAULT_MAX_ATTEMPTS);
        assert_eq!(t.version, 1);
        for bad in [
            CreateTask {
                lease_ms: Some(MAX_LEASE_MS + 1),
                ..Default::default()
            },
            CreateTask {
                lease_ms: Some(MIN_LEASE_MS - 1),
                ..Default::default()
            },
            CreateTask {
                max_attempts: Some(0),
                ..Default::default()
            },
            CreateTask {
                max_attempts: Some(MAX_MAX_ATTEMPTS + 1),
                ..Default::default()
            },
            CreateTask {
                assignee: Some("bad name".into()),
                ..Default::default()
            },
        ] {
            assert_eq!(
                code(Task::create(
                    "t".into(),
                    "dev",
                    &owner(),
                    Labels::new(),
                    &bad
                )),
                ErrorCode::InvalidInput
            );
        }
        assert_eq!(
            code(Task::create(
                "t".into(),
                "",
                &owner(),
                Labels::new(),
                &CreateTask::default()
            )),
            ErrorCode::InvalidInput
        );
        assert!(Task::genesis(&TaskEvent::TaskReopened {}, T0).is_err());
    }

    #[test]
    fn ready_to_claimed() {
        let t = claimed(3);
        assert_eq!(t.state, TaskState::Claimed);
        assert_eq!(t.attempts, 1);
        assert_eq!(t.version, 2);
        let c = t.live_claim(T0).unwrap();
        assert_eq!(c.expires_at, T0 + LEASE);
        assert_eq!(c.holder.to_string(), "w1 via chris");
    }

    #[test]
    fn claim_lease_override() {
        let t = new_task(3);
        let d = t.claim(T0, &worker("w"), "c".into(), Some(200)).unwrap();
        assert!(
            matches!(d.events[0], TaskEvent::TaskClaimed { expires_at, .. } if expires_at == T0 + 200)
        );
        assert_eq!(
            code(t.claim(T0, &worker("w"), "c".into(), Some(1))),
            ErrorCode::InvalidInput
        );
    }

    #[test]
    fn claim_live_is_task_claimed() {
        let t = claimed(3);
        assert_eq!(
            code(t.claim(T0 + 10, &worker("w2"), "c2".into(), None)),
            ErrorCode::TaskClaimed
        );
        // Live through its last millisecond.
        assert_eq!(
            code(t.claim(T0 + LEASE, &worker("w2"), "c2".into(), None)),
            ErrorCode::TaskClaimed
        );
    }

    #[test]
    fn claim_finished_or_blocked_is_invalid_state() {
        for (op, want) in [
            ("complete", TaskState::Completed),
            ("fail", TaskState::Failed),
            ("block", TaskState::Blocked),
        ] {
            let mut t = claimed(3);
            let d = match op {
                "complete" => t.complete(T0, "c1", None),
                "fail" => t.fail(T0, "c1", "nope".into()),
                _ => t.block(T0, "c1", "which db?".into()),
            };
            run(&mut t, T0, d).unwrap();
            assert_eq!(t.state, want);
            assert_eq!(
                code(t.claim(T0, &worker("w2"), "c2".into(), None)),
                ErrorCode::InvalidState
            );
        }
        let mut t = new_task(3);
        let d = t.cancel(T0, &owner(), None);
        run(&mut t, T0, d).unwrap();
        assert_eq!(
            code(t.claim(T0, &worker("w2"), "c2".into(), None)),
            ErrorCode::InvalidState
        );
    }

    #[test]
    fn lease_end_is_ready_on_read() {
        let t = claimed(3);
        let after = T0 + LEASE + 1;
        assert_eq!(t.state, TaskState::Claimed);
        assert_eq!(t.effective_state(after), TaskState::Ready);
        assert!(t.live_claim(after).is_none());
        let view = t.at(after);
        assert_eq!(view.state, TaskState::Ready);
        assert!(view.claim.is_none());
        assert_eq!(
            view.version, t.version,
            "a read does not change the version"
        );
    }

    #[test]
    fn reclaim_records_the_lease_end_first() {
        let mut t = claimed(3);
        let after = T0 + LEASE + 1;
        let d = t.claim(after, &worker("w2"), "c2".into(), None);
        let events = run(&mut t, after, d).unwrap();
        assert_eq!(types(&events), ["LeaseExpired", "TaskClaimed"]);
        assert!(matches!(
            &events[1],
            TaskEvent::TaskClaimed { attempt: 2, .. }
        ));
        assert_eq!(t.live_claim(after).unwrap().claim_id, "c2");
    }

    #[test]
    fn late_worker_gets_claim_lost() {
        let mut t = claimed(3);
        let after = T0 + LEASE + 1;
        // Ended but not yet reclaimed: still lost.
        assert_eq!(code(t.complete(after, "c1", None)), ErrorCode::ClaimLost);
        assert_eq!(code(t.renew(after, "c1")), ErrorCode::ClaimLost);
        assert_eq!(code(t.fail(after, "c1", "x".into())), ErrorCode::ClaimLost);
        assert_eq!(code(t.block(after, "c1", "q".into())), ErrorCode::ClaimLost);
        assert_eq!(code(t.release(after, "c1")), ErrorCode::ClaimLost);
        // Reclaimed by w2 and completed: w1 is still lost.
        let d = t.claim(after, &worker("w2"), "c2".into(), None);
        run(&mut t, after, d).unwrap();
        assert_eq!(code(t.complete(after, "c1", None)), ErrorCode::ClaimLost);
        let d = t.complete(after, "c2", Some("done".into()));
        run(&mut t, after, d).unwrap();
        assert_eq!(t.state, TaskState::Completed);
        assert_eq!(t.result.as_deref(), Some("done"));
        assert_eq!(code(t.complete(after, "c1", None)), ErrorCode::ClaimLost);
    }

    #[test]
    fn missing_claim_is_claim_not_held() {
        let t = claimed(3);
        assert_eq!(code(t.complete(T0, "", None)), ErrorCode::ClaimNotHeld);
        assert_eq!(code(t.renew(T0, "")), ErrorCode::ClaimNotHeld);
        assert_eq!(code(t.release(T0, "")), ErrorCode::ClaimNotHeld);
        assert_eq!(code(t.complete(T0, "wrong", None)), ErrorCode::ClaimLost);
    }

    #[test]
    fn repeated_settle_is_replayed() {
        for op in [
            SettleOp::Complete,
            SettleOp::Fail,
            SettleOp::Release,
            SettleOp::Block,
        ] {
            let mut t = claimed(3);
            let settle = |t: &Task, op: SettleOp| match op {
                SettleOp::Complete => t.complete(T0, "c1", Some("r".into())),
                SettleOp::Fail => t.fail(T0, "c1", "boom".into()),
                SettleOp::Release => t.release(T0, "c1"),
                SettleOp::Block => t.block(T0, "c1", "q?".into()),
            };
            let d = settle(&t, op);
            run(&mut t, T0, d).unwrap();
            let again = settle(&t, op).unwrap();
            assert!(again.replayed, "{op:?} repeat is a replay");
            assert!(again.events.is_empty());
            // A different settle by the same claim is not a replay.
            let other = if op == SettleOp::Complete {
                SettleOp::Fail
            } else {
                SettleOp::Complete
            };
            assert_eq!(code(settle(&t, other)), ErrorCode::ClaimLost);
        }
    }

    #[test]
    fn replay_ends_when_task_moves_on() {
        let mut t = claimed(3);
        let d = t.release(T0, "c1");
        run(&mut t, T0, d).unwrap();
        let d = t.claim(T0, &worker("w2"), "c2".into(), None);
        run(&mut t, T0, d).unwrap();
        assert_eq!(code(t.release(T0, "c1")), ErrorCode::ClaimLost);

        let mut t = claimed(3);
        let d = t.complete(T0, "c1", None);
        run(&mut t, T0, d).unwrap();
        let d = t.reopen(T0, &owner());
        run(&mut t, T0, d).unwrap();
        assert_eq!(code(t.complete(T0, "c1", None)), ErrorCode::ClaimLost);
    }

    #[test]
    fn renew_extends_from_now_by_one_lease() {
        let mut t = claimed(3);
        let d = t.renew(T0 + 500, "c1");
        run(&mut t, T0 + 500, d).unwrap();
        assert_eq!(t.claim.as_ref().unwrap().expires_at, T0 + 500 + LEASE);
        let d = t.renew(T0 + 1400, "c1");
        run(&mut t, T0 + 1400, d).unwrap();
        assert_eq!(t.claim.as_ref().unwrap().expires_at, T0 + 1400 + LEASE);
    }

    #[test]
    fn renew_keeps_claim_lease_override() {
        let mut t = new_task(3);
        let d = t.claim(T0, &worker("w"), "c".into(), Some(300));
        run(&mut t, T0, d).unwrap();
        let d = t.renew(T0 + 100, "c").unwrap();
        assert!(
            matches!(d.events[0], TaskEvent::LeaseRenewed { expires_at, .. } if expires_at == T0 + 100 + 300)
        );
    }

    #[test]
    fn release_returns_to_ready_and_refunds_attempt() {
        let mut t = claimed(1);
        let d = t.release(T0, "c1");
        run(&mut t, T0, d).unwrap();
        assert_eq!(t.state, TaskState::Ready);
        assert_eq!(t.attempts, 0);
        assert!(t.claim.is_none());
        let d = t.claim(T0, &worker("w2"), "c2".into(), None).unwrap();
        assert!(matches!(
            &d.events[0],
            TaskEvent::TaskClaimed { attempt: 1, .. }
        ));
    }

    #[test]
    fn fail_and_block_need_text() {
        let t = claimed(3);
        assert_eq!(code(t.fail(T0, "c1", " ".into())), ErrorCode::InvalidInput);
        assert_eq!(code(t.block(T0, "c1", "".into())), ErrorCode::InvalidInput);
        assert_eq!(
            code(t.complete(T0, "c1", Some("x".repeat(MAX_RESULT + 1)))),
            ErrorCode::InvalidInput
        );
        let mut t2 = t.clone();
        let d = t.fail(T0, "c1", "tests broke".into());
        run(&mut t2, T0, d).unwrap();
        assert_eq!(t2.state, TaskState::Failed);
        assert_eq!(t2.reason.as_deref(), Some("tests broke"));
    }

    #[test]
    fn attempts_run_out_with_lease_expired_then_failed() {
        let mut t = claimed(2);
        let t1 = T0 + LEASE + 1;
        let d = t.claim(t1, &worker("w2"), "c2".into(), None);
        run(&mut t, t1, d).unwrap();
        assert_eq!(t.attempts, 2);
        let t2 = t1 + LEASE + 1;
        assert_eq!(t.effective_state(t2), TaskState::Failed);
        let view = t.at(t2);
        assert_eq!(view.state, TaskState::Failed);
        assert_eq!(view.reason.as_deref(), Some(exhausted_reason(2).as_str()));
        assert_eq!(
            code(t.claim(t2, &worker("w3"), "c3".into(), None)),
            ErrorCode::InvalidState
        );
        let d = t.expire(t2);
        let events = run(&mut t, t2, d).unwrap();
        assert_eq!(types(&events), ["LeaseExpired", "TaskFailed"]);
        assert_eq!(t.state, TaskState::Failed);
        assert!(t.claim.is_none());
    }

    #[test]
    fn expire_rejects_live_and_unclaimed() {
        let mut t = claimed(3);
        assert_eq!(code(t.expire(T0)), ErrorCode::InvalidState);
        let d = t.expire(T0 + LEASE + 1);
        let events = run(&mut t, T0 + LEASE + 1, d).unwrap();
        assert_eq!(types(&events), ["LeaseExpired"]);
        assert_eq!(t.state, TaskState::Ready);
        assert_eq!(code(t.expire(T0 + LEASE + 2)), ErrorCode::InvalidState);
        assert_eq!(code(new_task(1).expire(T0)), ErrorCode::InvalidState);
    }

    #[test]
    fn restart_grace_extends_stored_claims() {
        let mut t = claimed(3);
        // The daemon was down past the lease end; it starts at `start`.
        let start = T0 + 10 * LEASE;
        let d = t.restart_grace(start);
        let events = run(&mut t, start, d).unwrap();
        assert!(
            matches!(&events[0], TaskEvent::LeaseRenewed { reason: Some(r), expires_at, .. } if r == RESTART_GRACE_REASON && *expires_at == start + LEASE)
        );
        // The worker that kept running can renew.
        let d = t.renew(start + 10, "c1");
        run(&mut t, start + 10, d).unwrap();
        // No grace for a task that is not claimed, or already has enough.
        assert_eq!(
            code(new_task(3).restart_grace(start)),
            ErrorCode::InvalidState
        );
        assert_eq!(
            code(claimed(3).restart_grace(T0 - 1)),
            ErrorCode::InvalidState
        );
    }

    #[test]
    fn block_and_unblock() {
        let mut t = claimed(3);
        let d = t.block(T0, "c1", "which db?".into());
        run(&mut t, T0, d).unwrap();
        assert_eq!(t.state, TaskState::Blocked);
        assert_eq!(t.question.as_deref(), Some("which db?"));
        assert_eq!(
            code(t.unblock(T0, &stranger(), None)),
            ErrorCode::TaskNotOwned
        );
        let d = t.unblock(T0, &owner(), Some("postgres".into()));
        run(&mut t, T0, d).unwrap();
        assert_eq!(t.state, TaskState::Ready);
        assert_eq!(code(t.unblock(T0, &owner(), None)), ErrorCode::InvalidState);
    }

    #[test]
    fn cancel_rules() {
        let t = claimed(3);
        assert_eq!(
            code(t.cancel(T0, &stranger(), None)),
            ErrorCode::TaskNotOwned
        );
        // Any profile of the owner counts as the owner.
        let mut t2 = t.clone();
        let d = t.cancel(T0, &worker("anyprofile"), Some("dup".into()));
        run(&mut t2, T0, d).unwrap();
        assert_eq!(t2.state, TaskState::Canceled);
        assert_eq!(code(t2.cancel(T0, &owner(), None)), ErrorCode::InvalidState);
        assert_eq!(code(t2.complete(T0, "c1", None)), ErrorCode::ClaimLost);
    }

    #[test]
    fn reopen_rules() {
        let mut t = claimed(1);
        assert_eq!(code(t.reopen(T0, &owner())), ErrorCode::InvalidState);
        let after = T0 + LEASE + 1;
        assert_eq!(code(t.reopen(after, &stranger())), ErrorCode::TaskNotOwned);
        // Exhausted by lease end, not yet recorded: reopen records it first.
        let d = t.reopen(after, &owner());
        let events = run(&mut t, after, d).unwrap();
        assert_eq!(
            types(&events),
            ["LeaseExpired", "TaskFailed", "TaskReopened"]
        );
        assert_eq!(t.state, TaskState::Ready);
        assert_eq!(t.attempts, 0);
        assert!(t.reason.is_none());
    }

    #[test]
    fn assignee_limits_claims() {
        let e = Task::create(
            "t".into(),
            "dev",
            &owner(),
            Labels::new(),
            &CreateTask {
                assignee: Some("Reviewer".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let t = Task::genesis(&e, T0).unwrap();
        assert_eq!(t.assignee.as_deref(), Some("reviewer"));
        assert_eq!(
            code(t.claim(T0, &worker("w1"), "c".into(), None)),
            ErrorCode::Forbidden
        );
        t.claim(T0, &worker("reviewer"), "c".into(), None).unwrap();
    }

    #[test]
    fn fold_matches_incremental() {
        let created = Task::create(
            "t1".into(),
            "work/fix",
            &owner(),
            Labels::new(),
            &CreateTask {
                lease_ms: Some(LEASE),
                max_attempts: Some(3),
                assignee: None,
            },
        )
        .unwrap();
        let mut t = Task::genesis(&created, T0).unwrap();
        let mut log = vec![(created, T0)];
        type Step = Box<dyn Fn(&Task, Millis) -> Result<Decision>>;
        let steps: Vec<(Millis, Step)> = vec![
            (
                T0 + 1,
                Box::new(|t, now| t.claim(now, &worker("w"), "c1".into(), None)),
            ),
            (T0 + 2, Box::new(|t, now| t.renew(now, "c1"))),
            // Lease ends; w2 reclaims (records LeaseExpired first).
            (
                T0 + 5_000,
                Box::new(|t, now| t.claim(now, &worker("w2"), "c2".into(), None)),
            ),
            (
                T0 + 5_001,
                Box::new(|t, now| t.complete(now, "c2", Some("ok".into()))),
            ),
        ];
        for (at, step) in steps {
            for e in step(&t, at).unwrap().events {
                t.apply(&e, at);
                log.push((e, at));
            }
        }
        let folded = Task::from_events(log.iter().map(|(e, at)| (e, *at))).unwrap();
        assert_eq!(folded, t);
        assert_eq!(folded.version, 6);
    }

    #[test]
    fn events_round_trip_json() {
        let e = TaskEvent::TaskClaimed {
            claim_id: "c".into(),
            holder: worker("w"),
            expires_at: 5,
            attempt: 1,
        };
        let j = serde_json::to_value(&e).unwrap();
        assert_eq!(j["type"], "TaskClaimed");
        assert_eq!(serde_json::from_value::<TaskEvent>(j).unwrap(), e);
        let t = claimed(3);
        let j = serde_json::to_value(&t).unwrap();
        assert_eq!(j["state"], "claimed");
        assert_eq!(serde_json::from_value::<Task>(j).unwrap(), t);
    }

    #[test]
    fn state_parse() {
        for s in TaskState::ALL {
            assert_eq!(TaskState::parse(s.as_str()).unwrap(), s);
        }
        assert!(TaskState::parse("nope").is_err());
    }
}
