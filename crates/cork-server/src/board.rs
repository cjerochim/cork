//! The board engine: every command reads, decides with core and commits
//! inside one store write (HANDOFF 2, v3 single writer).
//!
//! - Writes go through [`Board::write`]: the clock is read inside the write,
//!   the closure decides, and any task the commit makes ready is handed to the
//!   oldest matching waiting claim in the same transaction (6.5).
//! - After each commit the events go out on an in-process broadcast, in
//!   position order. Waiters and streams match them in memory and read the
//!   store only to catch up (7.4).
//! - The lease timer records ended leases as the system actor (6.4).

use crate::clock::Clock;
use cork_api::*;
use cork_core::labels::{MENTION_LABEL, TASK_LABEL, parse_filter};
use cork_core::message::{MESSAGE_POSTED, post_reply, post_root};
use cork_core::{
    Actor, BoardError, ChannelSelector, CreateTask, Decision, ErrorCode, Message, Millis,
    NewMessage, Result, Task, TaskEvent, TaskState, task_stream, thread_stream,
};
use cork_store::*;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, broadcast, oneshot};

/// Events kept in the in-process broadcast before a slow reader lags and
/// falls back to the store.
const BROADCAST_CAPACITY: usize = 4_096;
/// Ended leases recorded per timer write.
const EXPIRE_BATCH: usize = 256;
/// Default and largest page for event reads.
const EVENTS_PAGE: usize = 100;
const EVENTS_PAGE_MAX: usize = 1_000;
/// Recent committed events kept in memory, so a waiter that starts a little
/// behind the head catches up without reading the store.
const RECENT_CAPACITY: usize = 8_192;
/// With no write for this long, the daemon checkpoints the WAL.
const IDLE_CHECKPOINT: Duration = Duration::from_secs(10);
/// Idempotency rows are kept this long (7.5).
const IDEMPOTENCY_KEEP_MS: Millis = 24 * 60 * 60 * 1000;

#[derive(Debug, Clone)]
pub struct BoardConfig {
    pub id: String,
    pub name: String,
    pub max_wait_ms: u64,
    pub max_streams: usize,
}

impl Default for BoardConfig {
    fn default() -> Self {
        Self {
            id: "default".into(),
            name: "cork".into(),
            max_wait_ms: DEFAULT_MAX_WAIT_MS,
            max_streams: 1_000,
        }
    }
}

/// Who is calling: the person from the authenticator, the profile from the
/// client.
#[derive(Debug, Clone)]
pub struct Caller {
    pub actor: Actor,
}

/// An `Idempotency-Key` and the hash of the request it came with.
#[derive(Debug, Clone)]
pub struct Idem {
    pub key: String,
    pub hash: String,
}

/// A command that changes a task after it was claimed or created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settle {
    Renew,
    Complete,
    Fail,
    Release,
    Block,
    Unblock,
    Cancel,
    Reopen,
}

impl Settle {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "renew" => Self::Renew,
            "complete" => Self::Complete,
            "fail" => Self::Fail,
            "release" => Self::Release,
            "block" => Self::Block,
            "unblock" => Self::Unblock,
            "cancel" => Self::Cancel,
            "reopen" => Self::Reopen,
            _ => return None,
        })
    }
}

// ---------- waiting claims ----------

struct Waiter {
    id: u64,
    channel: ChannelSelector,
    labels: Vec<(String, String)>,
    owner: Option<String>,
    holder: Actor,
    lease_ms: Option<i64>,
    idem: Option<Idem>,
    tx: oneshot::Sender<ClaimOut>,
}

impl Waiter {
    fn wants(&self, t: &Task) -> bool {
        self.channel.matches(&t.channel)
            && self.labels.iter().all(|(k, v)| t.labels.get(k) == Some(v))
            && self.owner.as_ref().is_none_or(|o| *o == t.owner)
            && t.can_be_claimed_by(&self.holder)
    }
}

#[derive(Default)]
struct Waiters {
    queue: VecDeque<Waiter>,
}

impl Waiters {
    fn remove(&mut self, id: u64) -> bool {
        let before = self.queue.len();
        self.queue.retain(|w| w.id != id);
        self.queue.len() != before
    }
}

/// Removes a waiting claim if its request goes away (timeout or disconnect).
struct WaiterGuard {
    waiters: Arc<Mutex<Waiters>>,
    id: u64,
}

impl WaiterGuard {
    /// True if the waiter was still queued, so no task was handed to it.
    fn cancel(&self) -> bool {
        self.waiters.lock().unwrap().remove(self.id)
    }
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        self.cancel();
    }
}

struct Handoff {
    tx: oneshot::Sender<ClaimOut>,
    out: ClaimOut,
}

// ---------- building a commit ----------

/// The commit a write is building, with the positions its events will get.
pub struct Batch {
    pub now: Millis,
    next_pos: u64,
    commit: Commit,
    /// Messages posted in this batch, by id (not yet readable through the tx).
    posted: HashMap<String, Message>,
    /// Tasks this batch moved into `ready`, in order.
    ready: Vec<Task>,
    handoffs: Vec<Handoff>,
}

impl Batch {
    fn new(head: u64, now: Millis) -> Self {
        Self {
            now,
            next_pos: head + 1,
            commit: Commit::default(),
            posted: HashMap::new(),
            ready: vec![],
            handoffs: vec![],
        }
    }

    fn push(&mut self, stream: String, expected: ExpectedVersion, events: Vec<NewEvent>) -> u64 {
        let last = self.next_pos + events.len() as u64 - 1;
        self.next_pos += events.len() as u64;
        self.commit.appends.push(StreamAppend {
            stream_id: stream,
            expected,
            events,
        });
        last
    }

    /// Append `MessagePosted` and project the message. Returns its position.
    fn post(&mut self, m: &Message) -> u64 {
        let mut labels: Vec<(String, String)> = m
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for h in &m.mentions {
            labels.push((MENTION_LABEL.into(), h.clone()));
        }
        let event = NewEvent::new(
            MESSAGE_POSTED,
            serde_json::to_value(m).expect("message serializes"),
            m.actor.to_string(),
            m.at,
            EventMeta {
                channel: Some(m.channel.clone()),
                thread_id: Some(m.thread_id.clone()),
                labels,
            },
        );
        let expected = if m.is_root() {
            ExpectedVersion::NoStream
        } else {
            ExpectedVersion::Any
        };
        let pos = self.push(thread_stream(&m.thread_id), expected, vec![event]);
        self.commit.projections.push(Projection::Message(m.clone()));
        self.posted.insert(m.id.clone(), m.clone());
        pos
    }

    /// Append task events and project the result. `before` is `None` for a
    /// create. Each event carries `board.task=<state after it>` (6.3).
    fn task(&mut self, before: Option<&Task>, events: &[TaskEvent], actor: &Actor) -> Result<Task> {
        let mut state: Option<Task> = before.cloned();
        let mut out = Vec::with_capacity(events.len());
        for e in events {
            let next = match state.take() {
                None => Task::genesis(e, self.now)?,
                Some(mut t) => {
                    t.apply(e, self.now);
                    t
                }
            };
            out.push(NewEvent::new(
                e.event_type(),
                serde_json::to_value(e).expect("task event serializes"),
                actor.to_string(),
                self.now,
                EventMeta {
                    channel: Some(next.channel.clone()),
                    thread_id: Some(next.id.clone()),
                    labels: vec![(TASK_LABEL.into(), next.state.as_str().into())],
                },
            ));
            state = Some(next);
        }
        let after = state.ok_or_else(|| BoardError::internal("no task events"))?;
        let expected = match before {
            None => ExpectedVersion::NoStream,
            Some(t) => ExpectedVersion::Exact(t.version),
        };
        self.push(task_stream(&after.id), expected, out);
        self.commit
            .projections
            .push(Projection::Task(after.clone()));
        self.commit.projections.push(Projection::SetMessageLabel {
            message_id: after.id.clone(),
            key: TASK_LABEL.into(),
            value: after.state.as_str().into(),
        });
        let was_ready = before.is_some_and(|b| b.state == TaskState::Ready);
        if after.state == TaskState::Ready && !was_ready {
            self.ready.push(after.clone());
        }
        Ok(after)
    }

    fn remember(&mut self, person: &str, idem: &Idem, result: serde_json::Value) {
        self.commit
            .projections
            .push(Projection::Idempotency(IdempotencyRecord {
                person: person.into(),
                key: idem.key.clone(),
                request_hash: idem.hash.clone(),
                result,
                created_at: self.now,
            }));
    }

    fn message(&self, tx: &mut dyn WriteTx, id: &str) -> Result<Option<Message>> {
        match self.posted.get(id) {
            Some(m) => Ok(Some(m.clone())),
            None => tx.get_message(id),
        }
    }

    /// Hand each task this batch made ready to the oldest waiting claim that
    /// wants it (6.5). Runs at the end of every write, in its transaction.
    fn hand_out(&mut self, tx: &mut dyn WriteTx, waiters: &Mutex<Waiters>) -> Result<()> {
        if self.ready.is_empty() {
            return Ok(());
        }
        let mut w = waiters.lock().unwrap();
        w.queue.retain(|x| !x.tx.is_closed());
        let ready = std::mem::take(&mut self.ready);
        for task in ready {
            let Some(i) = w.queue.iter().position(|x| x.wants(&task)) else {
                continue;
            };
            let waiter = w.queue.remove(i).expect("index is in range");
            let d = task.claim(self.now, &waiter.holder, new_id(), waiter.lease_ms)?;
            let after = self.task(Some(&task), &d.events, &waiter.holder)?;
            let root = self.message(tx, &after.id)?;
            let out = claimed(&after, root, self.now);
            if let Some(idem) = &waiter.idem {
                let v = serde_json::to_value(&out).map_err(json_err)?;
                self.remember(&waiter.holder.person, idem, v);
            }
            self.handoffs.push(Handoff { tx: waiter.tx, out });
        }
        // Tasks that the hand-out itself made ready (none today) go round again.
        if !self.ready.is_empty() {
            drop(w);
            return self.hand_out(tx, waiters);
        }
        Ok(())
    }
}

fn json_err(e: serde_json::Error) -> BoardError {
    BoardError::internal(format!("json: {e}"))
}

pub fn new_id() -> String {
    ulid::Ulid::new().to_string()
}

fn claimed(task: &Task, root: Option<Message>, now: Millis) -> ClaimOut {
    let c = task.claim.as_ref();
    ClaimOut {
        outcome: ClaimOutcome::Claimed,
        claim_id: c.map(|c| c.claim_id.clone()),
        expires_at: c.map(|c| c.expires_at),
        lease_ms: c.map(|c| c.lease_ms),
        attempt: c.map(|c| c.attempt),
        task: Some(task.clone()),
        root,
        now,
    }
}

/// Return a stored answer for this key, refuse a reused key, or go ahead.
fn idem_check(
    tx: &mut dyn WriteTx,
    person: &str,
    idem: Option<&Idem>,
) -> Result<Option<serde_json::Value>> {
    let Some(idem) = idem else { return Ok(None) };
    match tx.get_idempotency(person, &idem.key)? {
        Some(r) if r.request_hash == idem.hash => Ok(Some(r.result)),
        Some(_) => Err(BoardError::new(
            ErrorCode::IdempotencyMismatch,
            format!(
                "idempotency key `{}` was used with a different request",
                idem.key
            ),
        )),
        None => Ok(None),
    }
}

pub fn event_out(e: &StoredEvent) -> EventOut {
    EventOut {
        schema: e.schema,
        position: e.position,
        stream: e.stream_id.clone(),
        version: e.version,
        event_type: e.event_type.clone(),
        data: e.data.clone(),
        actor: e.actor.clone(),
        at: e.at,
        channel: e.meta.channel.clone(),
        thread_id: e.meta.thread_id.clone(),
        labels: e.meta.labels.clone(),
    }
}

/// The result of starting a claim: done now, or queued for a task.
enum ClaimStep {
    Done(serde_json::Value),
    Wait(WaiterGuard, oneshot::Receiver<ClaimOut>),
}

// ---------- the board ----------

pub struct Board<S: Store> {
    store: Arc<S>,
    clock: Arc<dyn Clock>,
    cfg: BoardConfig,
    events: broadcast::Sender<Arc<StoredEvent>>,
    /// Holds a write and its broadcast together, so events go out in
    /// position order. Fair (FIFO), like the store's writer.
    write_lock: tokio::sync::Mutex<()>,
    waiters: Arc<Mutex<Waiters>>,
    next_waiter: AtomicU64,
    lease_changed: Notify,
    degraded: AtomicBool,
    streams: AtomicUsize,
    stopped: AtomicBool,
    /// The last committed position, and the events just before it.
    head: AtomicU64,
    recent: Mutex<VecDeque<Arc<StoredEvent>>>,
    last_write: Mutex<std::time::Instant>,
    store_reads: AtomicU64,
}

impl<S: Store> Board<S> {
    /// Open the board: grant restart grace to stored claims (6.4), then start
    /// the lease timer.
    pub async fn start(
        store: Arc<S>,
        clock: Arc<dyn Clock>,
        cfg: BoardConfig,
    ) -> Result<Arc<Self>> {
        let board = Arc::new(Self {
            store,
            clock,
            cfg,
            events: broadcast::channel(BROADCAST_CAPACITY).0,
            write_lock: tokio::sync::Mutex::new(()),
            waiters: Arc::default(),
            next_waiter: AtomicU64::new(1),
            lease_changed: Notify::new(),
            degraded: AtomicBool::new(false),
            streams: AtomicUsize::new(0),
            stopped: AtomicBool::new(false),
            head: AtomicU64::new(0),
            recent: Mutex::new(VecDeque::new()),
            last_write: Mutex::new(std::time::Instant::now()),
            store_reads: AtomicU64::new(0),
        });
        board
            .head
            .store(board.store.head().await?, Ordering::SeqCst);
        board.restart_grace().await?;
        tokio::spawn(board.clone().lease_timer());
        tokio::spawn(board.clone().maintenance());
        Ok(board)
    }

    pub fn store(&self) -> &Arc<S> {
        &self.store
    }
    pub fn now(&self) -> Millis {
        self.clock.now()
    }
    pub fn config(&self) -> &BoardConfig {
        &self.cfg
    }
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Relaxed)
    }
    /// Stop the lease timer, so the board (and its store) can be dropped.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.lease_changed.notify_one();
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<StoredEvent>> {
        self.events.subscribe()
    }

    /// Run `f` in one store write with the clock read inside it, hand out
    /// newly ready tasks, then broadcast the committed events in order.
    async fn write<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut dyn WriteTx, &mut Batch) -> Result<T> + Send + 'static,
    {
        let _order = self.write_lock.lock().await;
        let clock = self.clock.clone();
        let waiters = self.waiters.clone();
        let r = self
            .store
            .write(move |tx| {
                let mut b = Batch::new(tx.head()?, clock.now());
                let value = f(tx, &mut b)?;
                b.hand_out(tx, &waiters)?;
                Ok((b.commit, (value, b.handoffs)))
            })
            .await;
        match r {
            Ok((events, (value, handoffs))) => {
                self.degraded.store(false, Ordering::Relaxed);
                let touches_tasks = events.iter().any(|e| e.stream_id.starts_with("task:"));
                if !events.is_empty() {
                    *self.last_write.lock().unwrap() = std::time::Instant::now();
                    let events: Vec<Arc<StoredEvent>> = events.into_iter().map(Arc::new).collect();
                    {
                        let mut recent = self.recent.lock().unwrap();
                        recent.extend(events.iter().cloned());
                        while recent.len() > RECENT_CAPACITY {
                            recent.pop_front();
                        }
                    }
                    // Set before sending: a reader that subscribes and then
                    // reads `head` never misses an event at or below it.
                    if let Some(last) = events.last() {
                        self.head.store(last.position, Ordering::SeqCst);
                    }
                    for e in events {
                        let _ = self.events.send(e);
                    }
                }
                drop(_order);
                if touches_tasks {
                    self.lease_changed.notify_one();
                }
                for h in handoffs {
                    let _ = h.tx.send(h.out);
                }
                Ok(value)
            }
            Err(e) => {
                if e.code == ErrorCode::Unavailable {
                    self.degraded.store(true, Ordering::Relaxed);
                }
                Err(e)
            }
        }
    }

    /// A write that honors an `Idempotency-Key`: the same key and request
    /// returns the first answer and writes nothing (7.5).
    async fn write_idem<R, F>(
        &self,
        caller: &Caller,
        idem: Option<Idem>,
        f: F,
    ) -> Result<serde_json::Value>
    where
        R: Serialize,
        F: FnOnce(&mut dyn WriteTx, &mut Batch) -> Result<R> + Send + 'static,
    {
        let person = caller.actor.person.clone();
        self.write(move |tx, b| {
            if let Some(v) = idem_check(tx, &person, idem.as_ref())? {
                return Ok(v);
            }
            let v = serde_json::to_value(f(tx, b)?).map_err(json_err)?;
            if let Some(idem) = &idem {
                b.remember(&person, idem, v.clone());
            }
            Ok(v)
        })
        .await
    }

    // ---------- messages ----------

    pub async fn post(
        &self,
        caller: &Caller,
        idem: Option<Idem>,
        channel: String,
        req: PostMessage,
    ) -> Result<serde_json::Value> {
        let actor = caller.actor.clone();
        self.write_idem(caller, idem, move |_, b| {
            let m = post_root(new_id(), &channel, to_new(req), actor, b.now)?;
            let position = b.post(&m);
            Ok(MessageOut {
                position,
                message: m,
            })
        })
        .await
    }

    pub async fn reply(
        &self,
        caller: &Caller,
        idem: Option<Idem>,
        id: String,
        req: PostMessage,
    ) -> Result<serde_json::Value> {
        let actor = caller.actor.clone();
        self.write_idem(caller, idem, move |tx, b| {
            let m = tx
                .get_message(&id)?
                .ok_or_else(|| BoardError::not_found(format!("message {id}")))?;
            let root = if m.is_root() {
                m
            } else {
                tx.get_message(&m.thread_id)?.ok_or_else(|| {
                    BoardError::internal(format!("thread {} has no root", m.thread_id))
                })?
            };
            let r = post_reply(new_id(), &root, to_new(req), actor, b.now)?;
            let position = b.post(&r);
            Ok(MessageOut {
                position,
                message: r,
            })
        })
        .await
    }

    // ---------- tasks ----------

    pub async fn create_task(
        &self,
        caller: &Caller,
        idem: Option<Idem>,
        req: CreateTaskReq,
    ) -> Result<serde_json::Value> {
        let actor = caller.actor.clone();
        self.write_idem(caller, idem, move |_, b| {
            let mut root = post_root(
                new_id(),
                &req.channel,
                NewMessage {
                    text: req.text,
                    labels: req.labels,
                    data: req.data,
                },
                actor.clone(),
                b.now,
            )?;
            let created = Task::create(
                root.id.clone(),
                &root.channel,
                &actor,
                root.labels.clone(),
                &CreateTask {
                    lease_ms: req.lease_ms,
                    max_attempts: req.max_attempts,
                    assignee: req.assignee,
                },
            )?;
            let position = b.post(&root);
            let task = b.task(None, &[created], &actor)?;
            root.labels
                .insert(TASK_LABEL.into(), task.state.as_str().into());
            Ok(TaskOut {
                task,
                root: MessageOut {
                    position,
                    message: root,
                },
                now: b.now,
            })
        })
        .await
    }

    /// `POST /tasks/claim`: a named task, or the oldest ready one, optionally
    /// waiting in the first-come, first-served queue (6.5).
    pub async fn claim(
        &self,
        caller: &Caller,
        idem: Option<Idem>,
        req: ClaimReq,
    ) -> Result<serde_json::Value> {
        let holder = caller.actor.clone();
        let person = holder.person.clone();
        if let Some(task_id) = req.task_id {
            let lease = req.lease_ms;
            return self
                .write_idem(caller, idem, move |tx, b| {
                    let task = get_task(tx, &task_id)?;
                    let d = task.claim(b.now, &holder, new_id(), lease)?;
                    let after = b.task(Some(&task), &d.events, &holder)?;
                    let root = b.message(tx, &after.id)?;
                    Ok(claimed(&after, root, b.now))
                })
                .await;
        }

        let channel = ChannelSelector::parse(
            req.channel
                .as_deref()
                .ok_or_else(|| BoardError::invalid("claim needs task_id or channel"))?,
        )?;
        let labels = parse_filter(&req.labels)?;
        let owner = match req.from.as_deref() {
            None => Some(person.clone()),
            Some("*") => None,
            Some(p) => Some(cork_core::actor::normalize_name("person", p)?),
        };
        let wait_ms = req.wait_ms.unwrap_or(0).min(self.cfg.max_wait_ms);
        let lease = req.lease_ms;
        let waiter_id = self.next_waiter.fetch_add(1, Ordering::Relaxed);
        let waiters = self.waiters.clone();

        let step = self
            .write(move |tx, b| {
                if let Some(v) = idem_check(tx, &person, idem.as_ref())? {
                    return Ok(ClaimStep::Done(v));
                }
                let q = TaskQuery {
                    channel: Some(channel.clone()),
                    state: Some(TaskState::Ready),
                    labels: labels.clone(),
                    owner: owner.clone(),
                    claimable_by: Some(holder.handle().to_string()),
                    limit: 1,
                    now: b.now,
                    ..Default::default()
                };
                if let Some(st) = tx.list_tasks(&q)?.into_iter().next() {
                    let d = st.task.claim(b.now, &holder, new_id(), lease)?;
                    let after = b.task(Some(&st.task), &d.events, &holder)?;
                    let root = b.message(tx, &after.id)?;
                    let v = serde_json::to_value(claimed(&after, root, b.now)).map_err(json_err)?;
                    if let Some(idem) = &idem {
                        b.remember(&person, idem, v.clone());
                    }
                    return Ok(ClaimStep::Done(v));
                }
                if wait_ms == 0 {
                    let v =
                        serde_json::to_value(ClaimOut::empty(ClaimOutcome::NothingReady, b.now))
                            .map_err(json_err)?;
                    return Ok(ClaimStep::Done(v));
                }
                // Queue inside the write, so no task can become ready between
                // the check above and joining the queue.
                let (otx, orx) = oneshot::channel();
                waiters.lock().unwrap().queue.push_back(Waiter {
                    id: waiter_id,
                    channel,
                    labels,
                    owner,
                    holder,
                    lease_ms: lease,
                    idem,
                    tx: otx,
                });
                Ok(ClaimStep::Wait(
                    WaiterGuard {
                        waiters: waiters.clone(),
                        id: waiter_id,
                    },
                    orx,
                ))
            })
            .await?;

        match step {
            ClaimStep::Done(v) => Ok(v),
            ClaimStep::Wait(guard, mut rx) => {
                let out = match tokio::time::timeout(Duration::from_millis(wait_ms), &mut rx).await
                {
                    Ok(Ok(out)) => out,
                    Ok(Err(_)) => return Err(handoff_lost()),
                    Err(_) => {
                        if guard.cancel() {
                            ClaimOut::empty(ClaimOutcome::Timeout, self.now())
                        } else {
                            // A write handed us a task as the wait ran out.
                            tokio::time::timeout(Duration::from_secs(10), rx)
                                .await
                                .map_err(|_| handoff_lost())?
                                .map_err(|_| handoff_lost())?
                        }
                    }
                };
                serde_json::to_value(out).map_err(json_err)
            }
        }
    }

    pub async fn settle(
        &self,
        caller: &Caller,
        idem: Option<Idem>,
        task_id: String,
        op: Settle,
        req: SettleReq,
    ) -> Result<serde_json::Value> {
        let actor = caller.actor.clone();
        self.write_idem(caller, idem, move |tx, b| {
            let task = get_task(tx, &task_id)?;
            let now = b.now;
            let c = req.claim_id.as_str();
            let d: Decision = match op {
                Settle::Renew => task.renew(now, c)?,
                Settle::Complete => task.complete(now, c, req.result)?,
                Settle::Fail => task.fail(now, c, req.reason.unwrap_or_default())?,
                Settle::Release => task.release(now, c)?,
                Settle::Block => task.block(now, c, req.question.unwrap_or_default())?,
                Settle::Unblock => task.unblock(now, &actor, req.note)?,
                Settle::Cancel => task.cancel(now, &actor, req.reason)?,
                Settle::Reopen => task.reopen(now, &actor)?,
            };
            if d.replayed {
                return Ok(SettleOut {
                    task: task.at(now),
                    replayed: true,
                    now,
                });
            }
            let after = b.task(Some(&task), &d.events, &actor)?;
            Ok(SettleOut {
                task: after,
                replayed: false,
                now,
            })
        })
        .await
    }

    // ---------- the lease timer and restart grace ----------

    /// Record ended leases as the system actor. Returns how many, and whether
    /// more may be waiting.
    pub async fn expire_due(&self) -> Result<(usize, bool)> {
        self.write(|tx, b| {
            let ids = tx.ended_leases(b.now, EXPIRE_BATCH)?;
            let system = Actor::system();
            let mut n = 0;
            for id in &ids {
                let task = get_task(tx, id)?;
                if let Ok(d) = task.expire(b.now) {
                    b.task(Some(&task), &d.events, &system)?;
                    n += 1;
                }
            }
            Ok((n, ids.len() == EXPIRE_BATCH))
        })
        .await
    }

    async fn restart_grace(&self) -> Result<()> {
        let ids = self.store.claimed_tasks().await?;
        for chunk in ids.chunks(EXPIRE_BATCH) {
            let chunk = chunk.to_vec();
            self.write(move |tx, b| {
                let system = Actor::system();
                for id in &chunk {
                    let task = get_task(tx, id)?;
                    if let Ok(d) = task.restart_grace(b.now) {
                        b.task(Some(&task), &d.events, &system)?;
                    }
                }
                Ok(())
            })
            .await?;
        }
        Ok(())
    }

    async fn lease_timer(self: Arc<Self>) {
        let mut backoff = Duration::from_secs(1);
        loop {
            if self.stopped.load(Ordering::SeqCst) {
                return;
            }
            let notified = self.lease_changed.notified();
            match self.expire_due().await {
                Ok((_, true)) => continue,
                Ok(_) => backoff = Duration::from_secs(1),
                Err(e) => {
                    tracing::warn!(error = %e.message, "lease timer write failed");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                    continue;
                }
            }
            match self.store.next_lease_end().await {
                Ok(Some(end)) => {
                    let ms = (end + 1 - self.now()).max(0) as u64;
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(ms)) => {}
                        _ = notified => {}
                    }
                }
                Ok(None) => notified.await,
                Err(_) => tokio::time::sleep(backoff).await,
            }
        }
    }

    // ---------- reads ----------

    pub async fn head(&self) -> Result<u64> {
        self.store.head().await
    }

    pub async fn whoami(&self, caller: &Caller) -> Result<WhoAmI> {
        Ok(WhoAmI {
            board_id: self.cfg.id.clone(),
            board: self.cfg.name.clone(),
            person: caller.actor.person.clone(),
            profile: caller.actor.profile.clone(),
            actor: caller.actor.to_string(),
            head: self.store.head().await?,
            now: self.now(),
        })
    }

    pub async fn channels(&self) -> Result<ChannelsOut> {
        Ok(ChannelsOut {
            channels: self
                .store
                .list_channels()
                .await?
                .into_iter()
                .map(|c| ChannelOut {
                    path: c.path,
                    threads: c.threads,
                    messages: c.messages,
                    last_position: c.last_position,
                })
                .collect(),
        })
    }

    pub async fn messages(&self, mut q: MessageQuery) -> Result<MessagesPage> {
        q.now = self.now();
        q.limit = page(q.limit, EVENTS_PAGE, EVENTS_PAGE_MAX);
        let messages = self.store.list_messages(&q).await?;
        Ok(MessagesPage {
            messages: messages
                .into_iter()
                .map(|m| MessageOut {
                    position: m.position,
                    message: m.message,
                })
                .collect(),
            now: q.now,
        })
    }

    async fn root_out(&self, thread_id: &str, now: Millis) -> Result<Option<MessageOut>> {
        let r = self
            .store
            .list_messages(&MessageQuery {
                thread_id: Some(thread_id.into()),
                roots_only: true,
                limit: 1,
                now,
                ..Default::default()
            })
            .await?;
        Ok(r.into_iter().next().map(|m| MessageOut {
            position: m.position,
            message: m.message,
        }))
    }

    pub async fn thread(&self, id: &str, after: u64, limit: usize) -> Result<ThreadOut> {
        let now = self.now();
        let m = self
            .store
            .get_message(id, now)
            .await?
            .ok_or_else(|| BoardError::not_found(format!("message {id}")))?;
        let root = self
            .root_out(&m.thread_id, now)
            .await?
            .ok_or_else(|| BoardError::internal("thread has no root"))?;
        let limit = page(limit, THREAD_PAGE, THREAD_PAGE_MAX);
        let replies = self
            .store
            .list_messages(&MessageQuery {
                thread_id: Some(m.thread_id.clone()),
                after: after.max(root.position),
                limit,
                now,
                ..Default::default()
            })
            .await?
            .into_iter()
            .map(|m| MessageOut {
                position: m.position,
                message: m.message,
            })
            .collect();
        let task = self
            .store
            .get_task(&m.thread_id)
            .await?
            .map(|t| t.task.at(now));
        Ok(ThreadOut {
            root,
            replies,
            task,
            now,
        })
    }

    pub async fn task(&self, id: &str, at: Option<u64>) -> Result<TaskOut> {
        let now = self.now();
        let task = match at {
            None => self
                .store
                .get_task(id)
                .await?
                .ok_or_else(|| BoardError::not_found(format!("task {id}")))?
                .task
                .at(now),
            Some(pos) => {
                let events: Vec<StoredEvent> = self
                    .history_events(id)
                    .await?
                    .into_iter()
                    .filter(|e| e.position <= pos)
                    .collect();
                if events.is_empty() {
                    return Err(BoardError::not_found(format!(
                        "task {id} at position {pos}"
                    )));
                }
                fold(&events)?
            }
        };
        let root = self
            .root_out(id, now)
            .await?
            .ok_or_else(|| BoardError::not_found(format!("task {id}")))?;
        Ok(TaskOut { task, root, now })
    }

    async fn history_events(&self, id: &str) -> Result<Vec<StoredEvent>> {
        let mut out = vec![];
        let mut after = 0;
        loop {
            let page = self
                .store
                .read_events(&EventFilter {
                    after,
                    limit: EVENTS_PAGE_MAX,
                    stream_id: Some(task_stream(id)),
                    ..Default::default()
                })
                .await?;
            let n = page.events.len();
            if let Some(e) = page.events.last() {
                after = e.position;
            }
            out.extend(page.events);
            if n < EVENTS_PAGE_MAX {
                return Ok(out);
            }
        }
    }

    pub async fn history(&self, id: &str) -> Result<HistoryOut> {
        let events = self.history_events(id).await?;
        if events.is_empty() {
            return Err(BoardError::not_found(format!("task {id}")));
        }
        Ok(HistoryOut {
            events: events.iter().map(event_out).collect(),
        })
    }

    pub async fn tasks(&self, mut q: TaskQuery) -> Result<TasksPage> {
        q.now = self.now();
        q.limit = page(q.limit, EVENTS_PAGE, EVENTS_PAGE_MAX);
        Ok(TasksPage {
            tasks: self
                .store
                .list_tasks(&q)
                .await?
                .into_iter()
                .map(|t| TaskListItem {
                    created_position: t.created_position,
                    task: t.task.at(q.now),
                })
                .collect(),
            now: q.now,
        })
    }

    /// `GET /events`: events after `after` (default: the head), waiting up
    /// to `wait_ms` for the first match. Reads the store only to catch up.
    pub async fn wait_events(
        &self,
        mut f: EventFilter,
        after: Option<u64>,
        wait_ms: u64,
    ) -> Result<EventsPage> {
        let mut rx = self.events.subscribe();
        let limit = page(f.limit, EVENTS_PAGE, EVENTS_PAGE_MAX);
        let mut cursor = after.unwrap_or_else(|| self.last_position());
        let wait = Duration::from_millis(wait_ms.min(self.cfg.max_wait_ms));
        let deadline = tokio::time::Instant::now() + wait;
        'catch_up: loop {
            f.after = cursor;
            f.limit = limit;
            let (events, head) = self.catch_up(&f).await?;
            if let Some(last) = events.last() {
                return Ok(EventsPage {
                    next_after: last.position,
                    events: events.iter().map(|e| event_out(e)).collect(),
                    now: self.now(),
                });
            }
            cursor = cursor.max(head);
            loop {
                let e = match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Err(_) | Ok(Err(broadcast::error::RecvError::Closed)) => {
                        return Ok(EventsPage {
                            events: vec![],
                            next_after: cursor,
                            now: self.now(),
                        });
                    }
                    Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue 'catch_up,
                    Ok(Ok(e)) => e,
                };
                if e.position <= cursor {
                    continue;
                }
                if e.position > cursor + 1 {
                    continue 'catch_up; // A gap: read it from the store.
                }
                cursor = e.position;
                if f.matches(&e) {
                    let mut events = vec![event_out(&e)];
                    // Take what else is already here, up to the page size.
                    while events.len() < limit {
                        match rx.try_recv() {
                            Ok(e) if e.position == cursor + 1 => {
                                cursor = e.position;
                                if f.matches(&e) {
                                    events.push(event_out(&e));
                                }
                            }
                            _ => break,
                        }
                    }
                    return Ok(EventsPage {
                        next_after: cursor,
                        events,
                        now: self.now(),
                    });
                }
            }
        }
    }

    /// The last committed position, from memory.
    pub fn last_position(&self) -> u64 {
        self.head.load(Ordering::SeqCst)
    }

    /// How many times waits and streams have read the store to catch up.
    pub fn store_reads(&self) -> u64 {
        self.store_reads.load(Ordering::Relaxed)
    }

    /// Matching events after `f.after` (up to `f.limit`) and the position
    /// checked up to. Served from the in-memory window of recent events when
    /// it covers the range; otherwise one store read.
    pub async fn catch_up(&self, f: &EventFilter) -> Result<(Vec<Arc<StoredEvent>>, u64)> {
        let head = self.last_position();
        if f.after >= head {
            return Ok((vec![], head));
        }
        {
            let recent = self.recent.lock().unwrap();
            if recent.front().is_some_and(|e| e.position <= f.after + 1) {
                let mut out = vec![];
                let mut checked = f.after;
                for e in recent.iter().filter(|e| e.position > f.after) {
                    checked = e.position;
                    if f.matches(e) {
                        out.push(e.clone());
                        if out.len() >= f.limit.max(1) {
                            break;
                        }
                    }
                }
                return Ok((out, checked));
            }
        }
        self.store_reads.fetch_add(1, Ordering::Relaxed);
        let page = self.store.read_events(f).await?;
        Ok((page.events.into_iter().map(Arc::new).collect(), page.head))
    }

    /// Checkpoint the WAL when idle, and prune old idempotency rows.
    async fn maintenance(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        let mut checkpointed = false;
        let mut last_prune = std::time::Instant::now() - Duration::from_secs(3600);
        loop {
            tick.tick().await;
            if self.stopped.load(Ordering::SeqCst) {
                return;
            }
            let idle = self.last_write.lock().unwrap().elapsed() >= IDLE_CHECKPOINT;
            if idle && !checkpointed {
                checkpointed = self.store.checkpoint().await.is_ok();
            } else if !idle {
                checkpointed = false;
            }
            if last_prune.elapsed() >= Duration::from_secs(3600) {
                last_prune = std::time::Instant::now();
                let _ = self
                    .store
                    .prune_idempotency(self.now() - IDEMPOTENCY_KEEP_MS)
                    .await;
            }
        }
    }

    /// Count an open stream, or refuse past `max_streams`.
    pub fn open_stream(self: &Arc<Self>) -> Result<StreamSlot<S>> {
        let n = self.streams.fetch_add(1, Ordering::SeqCst);
        if n >= self.cfg.max_streams {
            self.streams.fetch_sub(1, Ordering::SeqCst);
            return Err(BoardError::new(
                ErrorCode::Unavailable,
                "too many open streams",
            ));
        }
        Ok(StreamSlot(self.clone()))
    }

    /// Every event in the log, in order, for `GET /export`.
    pub async fn export_page(&self, after: u64) -> Result<Vec<StoredEvent>> {
        Ok(self
            .store
            .read_events(&EventFilter::after(after, EVENTS_PAGE_MAX))
            .await?
            .events)
    }
}

/// Holds one of the `max_streams` slots until dropped.
pub struct StreamSlot<S: Store>(Arc<Board<S>>);

impl<S: Store> StreamSlot<S> {
    pub fn board(&self) -> &Arc<Board<S>> {
        &self.0
    }
}

impl<S: Store> Drop for StreamSlot<S> {
    fn drop(&mut self) {
        self.0.streams.fetch_sub(1, Ordering::SeqCst);
    }
}

fn handoff_lost() -> BoardError {
    BoardError::new(
        ErrorCode::Unavailable,
        "the write that claimed a task for this wait failed; retry",
    )
}

fn page(n: usize, default: usize, max: usize) -> usize {
    if n == 0 { default } else { n.min(max) }
}

fn to_new(p: PostMessage) -> NewMessage {
    NewMessage {
        text: p.text,
        labels: p.labels,
        data: p.data,
    }
}

fn get_task(tx: &mut dyn WriteTx, id: &str) -> Result<Task> {
    Ok(tx
        .get_task(id)?
        .ok_or_else(|| BoardError::not_found(format!("task {id}")))?
        .task)
}

/// Fold stored task events into the task as of the last one.
pub fn fold(events: &[StoredEvent]) -> Result<Task> {
    let parsed: Vec<(TaskEvent, Millis)> = events
        .iter()
        .map(|e| {
            Ok((
                serde_json::from_value(e.data.clone()).map_err(json_err)?,
                e.at,
            ))
        })
        .collect::<Result<_>>()?;
    Task::from_events(parsed.iter().map(|(e, at)| (e, *at)))
}
