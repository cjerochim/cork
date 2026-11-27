//! SQLite adapter for the Cork storage contract (HANDOFF 5.2).
//!
//! - WAL mode. One writer connection, owned by one writer thread. Writes queue
//!   on a channel and run one at a time in the order they were sent. Each is a
//!   `BEGIN IMMEDIATE` transaction in which the caller's closure reads,
//!   decides, and returns a commit; events and projections are then written in
//!   that same transaction.
//! - A small pool of read-only connections. Each read is one short read
//!   transaction. WAL lets readers run while the writer writes.
//! - With one writer committing in order, a reader that has seen position N
//!   never later finds a new event below N.
//! - `SQLITE_FULL` becomes `unavailable`; any other SQLite failure, and a
//!   version guard mismatch, is `internal`.

use async_trait::async_trait;
use cork_core::labels::{MENTION_LABEL, TASK_LABEL};
use cork_core::{
    BoardError, ChannelSelector, ErrorCode, Message, Millis, Result, Task, TaskState, task_stream,
    thread_stream,
};
use cork_store::*;
use rusqlite::types::Value;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS events (
  position   INTEGER PRIMARY KEY AUTOINCREMENT,
  stream_id  TEXT    NOT NULL,
  version    INTEGER NOT NULL,
  type       TEXT    NOT NULL,
  schema     INTEGER NOT NULL,
  data       TEXT    NOT NULL,
  actor      TEXT    NOT NULL,
  at         INTEGER NOT NULL,
  channel    TEXT,
  thread_id  TEXT,
  UNIQUE (stream_id, version)
);
CREATE INDEX IF NOT EXISTS events_channel ON events (channel, position);
CREATE INDEX IF NOT EXISTS events_thread  ON events (thread_id, position);
CREATE INDEX IF NOT EXISTS events_type    ON events (type, position);

CREATE TABLE IF NOT EXISTS event_labels (
  key      TEXT    NOT NULL,
  value    TEXT    NOT NULL,
  position INTEGER NOT NULL,
  PRIMARY KEY (key, value, position)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS event_labels_pos ON event_labels (position);

CREATE TABLE IF NOT EXISTS messages (
  id        TEXT PRIMARY KEY,
  position  INTEGER NOT NULL,
  thread_id TEXT    NOT NULL,
  channel   TEXT    NOT NULL,
  is_root   INTEGER NOT NULL,
  body      TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_channel ON messages (channel, position);
CREATE INDEX IF NOT EXISTS messages_thread  ON messages (thread_id, position);
CREATE INDEX IF NOT EXISTS messages_pos     ON messages (position);

CREATE TABLE IF NOT EXISTS message_labels (
  key        TEXT NOT NULL,
  value      TEXT NOT NULL,
  message_id TEXT NOT NULL,
  PRIMARY KEY (key, value, message_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS message_labels_msg ON message_labels (message_id, key);

CREATE TABLE IF NOT EXISTS channels (
  path          TEXT PRIMARY KEY,
  threads       INTEGER NOT NULL,
  messages      INTEGER NOT NULL,
  last_position INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
  id               TEXT PRIMARY KEY,
  created_position INTEGER NOT NULL,
  channel          TEXT    NOT NULL,
  owner            TEXT    NOT NULL,
  assignee         TEXT,
  state            TEXT    NOT NULL,
  expires_at       INTEGER,
  attempts         INTEGER NOT NULL,
  max_attempts     INTEGER NOT NULL,
  version          INTEGER NOT NULL,
  body             TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS tasks_state   ON tasks (state, created_position);
CREATE INDEX IF NOT EXISTS tasks_channel ON tasks (channel, state, created_position);
CREATE INDEX IF NOT EXISTS tasks_expiry  ON tasks (state, expires_at);

CREATE TABLE IF NOT EXISTS task_labels (
  key     TEXT NOT NULL,
  value   TEXT NOT NULL,
  task_id TEXT NOT NULL,
  PRIMARY KEY (key, value, task_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS idempotency (
  person       TEXT    NOT NULL,
  key          TEXT    NOT NULL,
  request_hash TEXT    NOT NULL,
  result       TEXT    NOT NULL,
  created_at   INTEGER NOT NULL,
  PRIMARY KEY (person, key)
);
CREATE INDEX IF NOT EXISTS idempotency_age ON idempotency (created_at);

CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', '1');
"#;

/// The writer checkpoints the WAL on its own after this many commits.
pub const CHECKPOINT_EVERY: u64 = 10_000;

/// How hard each commit pushes to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Durability {
    /// `synchronous=FULL`: a commit is on disk before it returns. The default.
    #[default]
    Full,
    /// `synchronous=NORMAL`: faster, never corrupt, but an acknowledged write
    /// can be lost on power loss.
    Normal,
}

#[derive(Debug, Clone)]
pub struct SqliteConfig {
    pub path: PathBuf,
    pub readers: usize,
    pub durability: Durability,
}

impl SqliteConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            readers: 4,
            durability: Durability::Full,
        }
    }
}

type Job = Box<dyn FnOnce(&mut Writer) + Send>;

struct Writer {
    conn: Connection,
    commits: u64,
}

pub struct SqliteStore {
    jobs: mpsc::Sender<Job>,
    readers: Arc<Vec<Mutex<Connection>>>,
    next_reader: AtomicUsize,
    path: PathBuf,
}

// ---------- errors ----------

fn sql_err(e: rusqlite::Error) -> BoardError {
    if let rusqlite::Error::SqliteFailure(f, _) = &e
        && f.code == rusqlite::ErrorCode::DiskFull
    {
        return BoardError::new(ErrorCode::Unavailable, format!("disk full: {e}"));
    }
    BoardError::internal(format!("sqlite: {e}"))
}

fn other_err<E: std::fmt::Display>(e: E) -> BoardError {
    BoardError::internal(e.to_string())
}

trait Sql<T> {
    fn sql(self) -> Result<T>;
}
impl<T> Sql<T> for rusqlite::Result<T> {
    fn sql(self) -> Result<T> {
        self.map_err(sql_err)
    }
}

fn to_json<T: serde::Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v).map_err(other_err)
}

fn from_json<T: serde::de::DeserializeOwned>(s: &str) -> Result<T> {
    serde_json::from_str(s).map_err(|e| BoardError::internal(format!("stored JSON: {e}")))
}

// ---------- open ----------

impl SqliteStore {
    pub async fn open(cfg: SqliteConfig) -> Result<Self> {
        tokio::task::spawn_blocking(move || Self::open_blocking(cfg))
            .await
            .map_err(other_err)?
    }

    pub fn open_blocking(cfg: SqliteConfig) -> Result<Self> {
        if let Some(dir) = cfg.path.parent()
            && !dir.as_os_str().is_empty()
        {
            std::fs::create_dir_all(dir).map_err(other_err)?;
        }
        let w = Connection::open(&cfg.path).sql()?;
        w.busy_timeout(std::time::Duration::from_secs(5)).sql()?;
        w.pragma_update(None, "journal_mode", "WAL").sql()?;
        let sync = match cfg.durability {
            Durability::Full => "FULL",
            Durability::Normal => "NORMAL",
        };
        w.pragma_update(None, "synchronous", sync).sql()?;
        w.pragma_update(None, "temp_store", "MEMORY").sql()?;
        w.execute_batch(SCHEMA).sql()?;

        let mut readers = Vec::new();
        for _ in 0..cfg.readers.max(1) {
            let r = Connection::open_with_flags(
                &cfg.path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .sql()?;
            r.busy_timeout(std::time::Duration::from_secs(5)).sql()?;
            readers.push(Mutex::new(r));
        }

        let (jobs, rx) = mpsc::channel::<Job>();
        std::thread::Builder::new()
            .name("cork-sqlite-writer".into())
            .spawn(move || {
                let mut w = Writer {
                    conn: w,
                    commits: 0,
                };
                // Ends when the store, and with it the sender, is dropped.
                while let Ok(job) = rx.recv() {
                    job(&mut w);
                }
            })
            .map_err(other_err)?;

        Ok(Self {
            jobs,
            readers: Arc::new(readers),
            next_reader: AtomicUsize::new(0),
            path: cfg.path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Run `f` on the writer thread and wait for its answer.
    async fn on_writer<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Writer) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let job: Job = Box::new(move |w| {
            let r = std::panic::catch_unwind(AssertUnwindSafe(|| f(w)))
                .unwrap_or_else(|_| Err(BoardError::internal("write panicked")));
            let _ = tx.send(r);
        });
        self.jobs
            .send(job)
            .map_err(|_| BoardError::internal("writer thread stopped"))?;
        rx.await
            .map_err(|_| BoardError::internal("writer thread stopped"))?
    }

    /// Run `f` in one read transaction on a pooled reader.
    async fn read<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let readers = self.readers.clone();
        let start = self.next_reader.fetch_add(1, Ordering::Relaxed);
        tokio::task::spawn_blocking(move || {
            let n = readers.len();
            let mut guard = (0..n).find_map(|i| readers[(start + i) % n].try_lock().ok());
            if guard.is_none() {
                guard = Some(readers[start % n].lock().map_err(other_err)?);
            }
            let mut conn = guard.unwrap();
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .sql()?;
            let out = f(&tx)?;
            tx.finish().sql()?;
            Ok(out)
        })
        .await
        .map_err(other_err)?
    }
}

// ---------- SQL building ----------

/// The one place effective task state is turned into SQL (HANDOFF 5.3).
///
/// A predicate on the `tasks` row aliased `t` that is true when the task's
/// effective state at `now` is `state`. It mirrors `Task::effective_state`:
/// a claim is live while `now <= expires_at`; once it ends the task is
/// `ready`, or `failed` if it has used all its attempts. Written so the
/// `(state, …)` indexes still apply. The contract suite checks it against core.
fn state_pred(t: &str, state: TaskState, now: &str) -> String {
    let ended = format!("({t}.state = 'claimed' AND {t}.expires_at < {now})");
    match state {
        TaskState::Ready => {
            format!("({t}.state = 'ready' OR ({ended} AND {t}.attempts < {t}.max_attempts))")
        }
        TaskState::Failed => {
            format!("({t}.state = 'failed' OR ({ended} AND {t}.attempts >= {t}.max_attempts))")
        }
        TaskState::Claimed => format!("({t}.state = 'claimed' AND {t}.expires_at >= {now})"),
        other => format!("({t}.state = '{}')", other.as_str()),
    }
}

struct Query {
    clauses: Vec<String>,
    params: Vec<Value>,
}

impl Query {
    fn new() -> Self {
        Self {
            clauses: vec![],
            params: vec![],
        }
    }
    fn p(&mut self, v: impl Into<Value>) -> String {
        self.params.push(v.into());
        format!("?{}", self.params.len())
    }
    fn and(&mut self, clause: String) {
        self.clauses.push(clause);
    }
    /// `work` or `work/…`, compared by prefix bytes, never `LIKE` (`_` is a
    /// legal channel character).
    fn channel(&mut self, col: &str, sel: &ChannelSelector) {
        match sel {
            ChannelSelector::Exact(p) => {
                let a = self.p(p.clone());
                self.and(format!("{col} = {a}"));
            }
            ChannelSelector::Subtree(p) => {
                let a = self.p(p.clone());
                let prefix = format!("{p}/");
                let n = self.p(prefix.len() as i64);
                let b = self.p(prefix);
                self.and(format!("({col} = {a} OR substr({col}, 1, {n}) = {b})"));
            }
        }
    }
    fn where_sql(&self) -> String {
        if self.clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", self.clauses.join(" AND "))
        }
    }
}

const DEFAULT_LIMIT: i64 = 1_000;
const MAX_LIMIT: i64 = 10_000;

fn limit(n: usize) -> i64 {
    if n == 0 {
        DEFAULT_LIMIT
    } else {
        (n as i64).min(MAX_LIMIT)
    }
}

fn label_value(v: &str) -> Result<TaskState> {
    TaskState::parse(v)
}

// ---------- reads shared by the writer and the readers ----------

fn stream_version(c: &Connection, stream_id: &str) -> Result<u64> {
    c.prepare_cached("SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?1")
        .sql()?
        .query_row(params![stream_id], |r| r.get::<_, i64>(0))
        .sql()
        .map(|v| v as u64)
}

fn head(c: &Connection) -> Result<Position> {
    c.query_row("SELECT COALESCE(MAX(position), 0) FROM events", [], |r| {
        r.get::<_, i64>(0)
    })
    .sql()
    .map(|v| v as u64)
}

fn task_body(c: &Connection, id: &str) -> Result<Option<Task>> {
    let body: Option<String> = c
        .prepare_cached("SELECT body FROM tasks WHERE id = ?1")
        .sql()?
        .query_row(params![id], |r| r.get(0))
        .optional()
        .sql()?;
    body.map(|b| from_json(&b)).transpose()
}

/// Show `board.task` as the task's effective state at `now`.
fn with_task_label(mut m: Message, task: Option<Task>, now: Millis) -> Message {
    if let Some(t) = task {
        m.labels
            .insert(TASK_LABEL.into(), t.effective_state(now).as_str().into());
    }
    m
}

fn get_message(c: &Connection, id: &str) -> Result<Option<Message>> {
    let body: Option<String> = c
        .prepare_cached("SELECT body FROM messages WHERE id = ?1")
        .sql()?
        .query_row(params![id], |r| r.get(0))
        .optional()
        .sql()?;
    body.map(|b| from_json(&b)).transpose()
}

fn get_task(c: &Connection, id: &str) -> Result<Option<StoredTask>> {
    let row: Option<(i64, String)> = c
        .prepare_cached("SELECT created_position, body FROM tasks WHERE id = ?1")
        .sql()?
        .query_row(params![id], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()
        .sql()?;
    row.map(|(p, b)| {
        Ok(StoredTask {
            created_position: p as u64,
            task: from_json(&b)?,
        })
    })
    .transpose()
}

fn list_tasks(c: &Connection, q: &TaskQuery) -> Result<Vec<StoredTask>> {
    let mut s = Query::new();
    let now = s.p(q.now);
    if let Some(st) = q.state {
        s.and(state_pred("t", st, &now));
    }
    if let Some(ch) = &q.channel {
        s.channel("t.channel", ch);
    }
    if let Some(o) = &q.owner {
        let a = s.p(o.clone());
        s.and(format!("t.owner = {a}"));
    }
    if let Some(h) = &q.claimable_by {
        let a = s.p(h.clone());
        s.and(format!("(t.assignee IS NULL OR t.assignee = {a})"));
    }
    if q.after > 0 {
        let a = s.p(q.after as i64);
        s.and(format!("t.created_position > {a}"));
    }
    for (k, v) in &q.labels {
        let a = s.p(k.clone());
        let b = s.p(v.clone());
        s.and(format!(
            "EXISTS (SELECT 1 FROM task_labels l WHERE l.task_id = t.id AND l.key = {a} AND l.value = {b})"
        ));
    }
    let lim = s.p(limit(q.limit));
    let sql = format!(
        "SELECT t.created_position, t.body FROM tasks t {} ORDER BY t.created_position ASC LIMIT {lim}",
        s.where_sql()
    );
    let mut st = c.prepare_cached(&sql).sql()?;
    let rows = st
        .query_map(rusqlite::params_from_iter(s.params.iter()), |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
        })
        .sql()?
        .collect::<rusqlite::Result<Vec<_>>>()
        .sql()?;
    rows.into_iter()
        .map(|(p, b)| {
            Ok(StoredTask {
                created_position: p as u64,
                task: from_json(&b)?,
            })
        })
        .collect()
}

fn ended_leases(c: &Connection, now: Millis, n: usize) -> Result<Vec<String>> {
    c.prepare_cached(
        "SELECT id FROM tasks WHERE state = 'claimed' AND expires_at < ?1 ORDER BY expires_at, created_position LIMIT ?2",
    )
    .sql()?
    .query_map(params![now, limit(n)], |r| r.get(0))
    .sql()?
    .collect::<rusqlite::Result<Vec<_>>>()
    .sql()
}

fn get_idempotency(c: &Connection, person: &str, key: &str) -> Result<Option<IdempotencyRecord>> {
    let row: Option<(String, String, i64)> = c
        .prepare_cached(
            "SELECT request_hash, result, created_at FROM idempotency WHERE person = ?1 AND key = ?2",
        )
        .sql()?
        .query_row(params![person, key], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .optional()
        .sql()?;
    row.map(|(h, res, at)| {
        Ok(IdempotencyRecord {
            person: person.into(),
            key: key.into(),
            request_hash: h,
            result: from_json(&res)?,
            created_at: at,
        })
    })
    .transpose()
}

fn read_events(c: &Connection, f: &EventFilter) -> Result<EventPage> {
    let mut s = Query::new();
    let a = s.p(f.after as i64);
    s.and(format!("e.position > {a}"));
    if let Some(st) = &f.stream_id {
        let a = s.p(st.clone());
        s.and(format!("e.stream_id = {a}"));
    }
    if let Some(ch) = &f.channel {
        s.channel("e.channel", ch);
    }
    if let Some(t) = &f.thread_id {
        let a = s.p(t.clone());
        s.and(format!("e.thread_id = {a}"));
    }
    if !f.types.is_empty() {
        let ps: Vec<String> = f.types.iter().map(|t| s.p(t.clone())).collect();
        s.and(format!("e.type IN ({})", ps.join(", ")));
    }
    for (k, v) in &f.labels {
        let a = s.p(k.clone());
        let b = s.p(v.clone());
        s.and(format!(
            "EXISTS (SELECT 1 FROM event_labels l WHERE l.key = {a} AND l.value = {b} AND l.position = e.position)"
        ));
    }
    let lim = s.p(limit(f.limit));
    let sql = format!(
        "SELECT e.position, e.stream_id, e.version, e.type, e.schema, e.data, e.actor, e.at, e.channel, e.thread_id \
         FROM events e {} ORDER BY e.position ASC LIMIT {lim}",
        s.where_sql()
    );
    type Row = (
        i64,
        String,
        i64,
        String,
        i64,
        String,
        String,
        i64,
        Option<String>,
        Option<String>,
    );
    let rows: Vec<Row> = c
        .prepare_cached(&sql)
        .sql()?
        .query_map(rusqlite::params_from_iter(s.params.iter()), |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
                r.get(9)?,
            ))
        })
        .sql()?
        .collect::<rusqlite::Result<_>>()
        .sql()?;
    let mut events = Vec::with_capacity(rows.len());
    for (position, stream_id, version, event_type, schema, data, actor, at, channel, thread_id) in
        rows
    {
        let data = cork_core::schema::upcast(&event_type, schema as u32, from_json(&data)?)?;
        events.push(StoredEvent {
            position: position as u64,
            stream_id,
            version: version as u64,
            schema: cork_core::schema::current(&event_type),
            event_type,
            data,
            actor,
            at,
            meta: EventMeta {
                channel,
                thread_id,
                labels: vec![],
            },
        });
    }
    load_event_labels(c, &mut events)?;
    Ok(EventPage {
        events,
        head: head(c)?,
    })
}

fn load_event_labels(c: &Connection, events: &mut [StoredEvent]) -> Result<()> {
    let (Some(first), Some(last)) = (events.first(), events.last()) else {
        return Ok(());
    };
    let (lo, hi) = (first.position as i64, last.position as i64);
    let idx: HashMap<u64, usize> = events
        .iter()
        .enumerate()
        .map(|(i, e)| (e.position, i))
        .collect();
    let rows: Vec<(i64, String, String)> = c
        .prepare_cached(
            "SELECT position, key, value FROM event_labels WHERE position BETWEEN ?1 AND ?2 ORDER BY position, key, value",
        )
        .sql()?
        .query_map(params![lo, hi], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .sql()?
        .collect::<rusqlite::Result<_>>()
        .sql()?;
    for (p, k, v) in rows {
        if let Some(&i) = idx.get(&(p as u64)) {
            events[i].meta.labels.push((k, v));
        }
    }
    Ok(())
}

fn list_messages(c: &Connection, q: &MessageQuery) -> Result<Vec<StoredMessage>> {
    let mut s = Query::new();
    let now = s.p(q.now);
    if let Some(ch) = &q.channel {
        s.channel("m.channel", ch);
    }
    if let Some(t) = &q.thread_id {
        let a = s.p(t.clone());
        s.and(format!("m.thread_id = {a}"));
    }
    if q.roots_only {
        s.and("m.is_root = 1".into());
    }
    if q.after > 0 {
        let a = s.p(q.after as i64);
        s.and(format!("m.position > {a}"));
    }
    for (k, v) in &q.labels {
        if k == TASK_LABEL {
            let st = label_value(v)?;
            s.and(format!(
                "EXISTS (SELECT 1 FROM tasks t WHERE t.id = m.id AND {})",
                state_pred("t", st, &now)
            ));
        } else {
            let a = s.p(k.clone());
            let b = s.p(v.clone());
            s.and(format!(
                "EXISTS (SELECT 1 FROM message_labels l WHERE l.message_id = m.id AND l.key = {a} AND l.value = {b})"
            ));
        }
    }
    let lim = s.p(limit(q.limit));
    let sql = format!(
        "SELECT m.position, m.body, tk.body FROM messages m LEFT JOIN tasks tk ON tk.id = m.id \
         {w} ORDER BY m.position {dir} LIMIT {lim}",
        w = s.where_sql(),
        dir = if q.newest_first { "DESC" } else { "ASC" },
    );
    let rows: Vec<(i64, String, Option<String>)> = c
        .prepare_cached(&sql)
        .sql()?
        .query_map(rusqlite::params_from_iter(s.params.iter()), |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .sql()?
        .collect::<rusqlite::Result<_>>()
        .sql()?;
    rows.into_iter()
        .map(|(pos, body, task)| {
            let task: Option<Task> = task.map(|b| from_json(&b)).transpose()?;
            Ok(StoredMessage {
                position: pos as u64,
                message: with_task_label(from_json(&body)?, task, q.now),
            })
        })
        .collect()
}

// ---------- the writer ----------

struct Tx<'a>(&'a Connection);

impl WriteTx for Tx<'_> {
    fn stream_version(&mut self, stream_id: &str) -> Result<u64> {
        stream_version(self.0, stream_id)
    }
    fn get_message(&mut self, id: &str) -> Result<Option<Message>> {
        get_message(self.0, id)
    }
    fn get_task(&mut self, id: &str) -> Result<Option<StoredTask>> {
        get_task(self.0, id)
    }
    fn list_tasks(&mut self, q: &TaskQuery) -> Result<Vec<StoredTask>> {
        list_tasks(self.0, q)
    }
    fn ended_leases(&mut self, now: Millis, limit: usize) -> Result<Vec<String>> {
        ended_leases(self.0, now, limit)
    }
    fn get_idempotency(&mut self, person: &str, key: &str) -> Result<Option<IdempotencyRecord>> {
        get_idempotency(self.0, person, key)
    }
}

impl Writer {
    fn write<T>(
        &mut self,
        f: impl FnOnce(&mut dyn WriteTx) -> Result<(Commit, T)>,
    ) -> Result<(Vec<StoredEvent>, T)> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .sql()?;
        // An error anywhere below drops `tx`, which rolls back.
        let (commit, value) = f(&mut Tx(&tx))?;
        if commit.is_empty() {
            tx.finish().sql()?;
            return Ok((vec![], value));
        }
        let events = apply_commit(&tx, &commit)?;
        tx.commit().sql()?;
        self.commits += 1;
        if self.commits.is_multiple_of(CHECKPOINT_EVERY) {
            // Best effort: a busy checkpoint is retried at the next one.
            let _ = self.checkpoint();
        }
        Ok((events, value))
    }

    fn checkpoint(&mut self) -> Result<()> {
        self.conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .sql()
    }
}

fn apply_commit(tx: &Connection, commit: &Commit) -> Result<Vec<StoredEvent>> {
    let mut out = Vec::new();
    let mut first_pos: HashMap<&str, u64> = HashMap::new();
    let mut last_pos: HashMap<&str, u64> = HashMap::new();

    for app in &commit.appends {
        let current = stream_version(tx, &app.stream_id)?;
        let ok = match app.expected {
            ExpectedVersion::Any => true,
            ExpectedVersion::NoStream => current == 0,
            ExpectedVersion::Exact(v) => current == v,
        };
        if !ok {
            // With one writer this cannot happen. If it does, it is a bug.
            return Err(BoardError::internal(format!(
                "version guard: stream {} is at {current}, expected {:?}",
                app.stream_id, app.expected
            )));
        }
        let mut version = current;
        for e in &app.events {
            version += 1;
            tx.prepare_cached(
                "INSERT INTO events (stream_id, version, type, schema, data, actor, at, channel, thread_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )
            .sql()?
            .execute(params![
                app.stream_id,
                version as i64,
                e.event_type,
                e.schema as i64,
                to_json(&e.data)?,
                e.actor,
                e.at,
                e.meta.channel,
                e.meta.thread_id
            ])
            .sql()?;
            let position = tx.last_insert_rowid() as u64;
            {
                let mut st = tx
                    .prepare_cached(
                        "INSERT OR IGNORE INTO event_labels (key, value, position) VALUES (?1, ?2, ?3)",
                    )
                    .sql()?;
                for (k, v) in &e.meta.labels {
                    st.execute(params![k, v, position as i64]).sql()?;
                }
            }
            first_pos.entry(app.stream_id.as_str()).or_insert(position);
            last_pos.insert(app.stream_id.as_str(), position);
            out.push(StoredEvent {
                position,
                stream_id: app.stream_id.clone(),
                version,
                event_type: e.event_type.clone(),
                schema: e.schema,
                data: e.data.clone(),
                actor: e.actor.clone(),
                at: e.at,
                meta: e.meta.clone(),
            });
        }
    }

    let head_now = match out.last() {
        Some(e) => e.position,
        None => head(tx)?,
    };
    for p in &commit.projections {
        match p {
            Projection::Message(m) => project_message(tx, m, &last_pos, head_now)?,
            Projection::Task(t) => project_task(tx, t, &first_pos, head_now)?,
            Projection::SetMessageLabel {
                message_id,
                key,
                value,
            } => set_message_label(tx, message_id, key, value)?,
            Projection::Idempotency(r) => {
                tx.prepare_cached(
                    "INSERT OR REPLACE INTO idempotency (person, key, request_hash, result, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )
                .sql()?
                .execute(params![
                    r.person,
                    r.key,
                    r.request_hash,
                    to_json(&r.result)?,
                    r.created_at
                ])
                .sql()?;
            }
        }
    }
    Ok(out)
}

fn project_message(
    tx: &Connection,
    m: &Message,
    last_pos: &HashMap<&str, u64>,
    head_now: u64,
) -> Result<()> {
    let stream = thread_stream(&m.thread_id);
    let pos = last_pos.get(stream.as_str()).copied().unwrap_or(head_now);
    tx.prepare_cached(
        "INSERT INTO messages (id, position, thread_id, channel, is_root, body) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )
    .sql()?
    .execute(params![
        m.id,
        pos as i64,
        m.thread_id,
        m.channel,
        m.is_root() as i64,
        to_json(m)?
    ])
    .sql()?;
    {
        let mut st = tx
            .prepare_cached(
                "INSERT OR REPLACE INTO message_labels (key, value, message_id) VALUES (?1, ?2, ?3)",
            )
            .sql()?;
        for (k, v) in &m.labels {
            st.execute(params![k, v, m.id]).sql()?;
        }
        for h in &m.mentions {
            st.execute(params![MENTION_LABEL, h, m.id]).sql()?;
        }
    }
    tx.prepare_cached(
        "INSERT INTO channels (path, threads, messages, last_position) VALUES (?1, ?2, 1, ?3) \
         ON CONFLICT(path) DO UPDATE SET threads = threads + excluded.threads, \
           messages = messages + 1, last_position = excluded.last_position",
    )
    .sql()?
    .execute(params![m.channel, m.is_root() as i64, pos as i64])
    .sql()?;
    Ok(())
}

fn project_task(
    tx: &Connection,
    t: &Task,
    first_pos: &HashMap<&str, u64>,
    head_now: u64,
) -> Result<()> {
    let exists: bool = tx
        .prepare_cached("SELECT EXISTS (SELECT 1 FROM tasks WHERE id = ?1)")
        .sql()?
        .query_row(params![t.id], |r| r.get(0))
        .sql()?;
    let expires = match (t.state, &t.claim) {
        (TaskState::Claimed, Some(c)) => Some(c.expires_at),
        _ => None,
    };
    let body = to_json(t)?;
    if exists {
        tx.prepare_cached(
            "UPDATE tasks SET assignee = ?2, state = ?3, expires_at = ?4, attempts = ?5, \
               max_attempts = ?6, version = ?7, body = ?8 WHERE id = ?1",
        )
        .sql()?
        .execute(params![
            t.id,
            t.assignee,
            t.state.as_str(),
            expires,
            t.attempts as i64,
            t.max_attempts as i64,
            t.version as i64,
            body
        ])
        .sql()?;
        return Ok(());
    }
    let stream = task_stream(&t.id);
    let created = first_pos.get(stream.as_str()).copied().unwrap_or(head_now);
    tx.prepare_cached(
        "INSERT INTO tasks (id, created_position, channel, owner, assignee, state, expires_at, \
           attempts, max_attempts, version, body) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
    )
    .sql()?
    .execute(params![
        t.id,
        created as i64,
        t.channel,
        t.owner,
        t.assignee,
        t.state.as_str(),
        expires,
        t.attempts as i64,
        t.max_attempts as i64,
        t.version as i64,
        body
    ])
    .sql()?;
    let mut st = tx
        .prepare_cached(
            "INSERT OR IGNORE INTO task_labels (key, value, task_id) VALUES (?1, ?2, ?3)",
        )
        .sql()?;
    for (k, v) in &t.labels {
        st.execute(params![k, v, t.id]).sql()?;
    }
    Ok(())
}

fn set_message_label(tx: &Connection, message_id: &str, key: &str, value: &str) -> Result<()> {
    tx.prepare_cached("DELETE FROM message_labels WHERE message_id = ?1 AND key = ?2")
        .sql()?
        .execute(params![message_id, key])
        .sql()?;
    tx.prepare_cached("INSERT INTO message_labels (key, value, message_id) VALUES (?1, ?2, ?3)")
        .sql()?
        .execute(params![key, value, message_id])
        .sql()?;
    if let Some(mut m) = get_message(tx, message_id)? {
        m.labels.insert(key.into(), value.into());
        tx.prepare_cached("UPDATE messages SET body = ?2 WHERE id = ?1")
            .sql()?
            .execute(params![message_id, to_json(&m)?])
            .sql()?;
    }
    Ok(())
}

// ---------- the trait ----------

#[async_trait]
impl Store for SqliteStore {
    async fn write<T, F>(&self, f: F) -> Result<(Vec<StoredEvent>, T)>
    where
        T: Send + 'static,
        F: FnOnce(&mut dyn WriteTx) -> Result<(Commit, T)> + Send + 'static,
    {
        self.on_writer(move |w| w.write(f)).await
    }

    async fn read_events(&self, filter: &EventFilter) -> Result<EventPage> {
        let f = filter.clone();
        self.read(move |c| read_events(c, &f)).await
    }

    async fn head(&self) -> Result<Position> {
        self.read(head).await
    }

    async fn max_at(&self) -> Result<Option<Millis>> {
        self.read(|c| {
            c.query_row("SELECT MAX(at) FROM events", [], |r| r.get(0))
                .sql()
        })
        .await
    }

    async fn stream_version(&self, stream_id: &str) -> Result<u64> {
        let id = stream_id.to_string();
        self.read(move |c| stream_version(c, &id)).await
    }

    async fn get_message(&self, id: &str, now: Millis) -> Result<Option<Message>> {
        let id = id.to_string();
        self.read(move |c| {
            let Some(m) = get_message(c, &id)? else {
                return Ok(None);
            };
            let task = if m.is_root() {
                task_body(c, &id)?
            } else {
                None
            };
            Ok(Some(with_task_label(m, task, now)))
        })
        .await
    }

    async fn list_messages(&self, q: &MessageQuery) -> Result<Vec<StoredMessage>> {
        let q = q.clone();
        self.read(move |c| list_messages(c, &q)).await
    }

    async fn list_channels(&self) -> Result<Vec<ChannelInfo>> {
        self.read(|c| {
            c.prepare_cached(
                "SELECT path, threads, messages, last_position FROM channels ORDER BY path",
            )
            .sql()?
            .query_map([], |r| {
                Ok(ChannelInfo {
                    path: r.get(0)?,
                    threads: r.get::<_, i64>(1)? as u64,
                    messages: r.get::<_, i64>(2)? as u64,
                    last_position: r.get::<_, i64>(3)? as u64,
                })
            })
            .sql()?
            .collect::<rusqlite::Result<_>>()
            .sql()
        })
        .await
    }

    async fn get_task(&self, id: &str) -> Result<Option<StoredTask>> {
        let id = id.to_string();
        self.read(move |c| get_task(c, &id)).await
    }

    async fn list_tasks(&self, q: &TaskQuery) -> Result<Vec<StoredTask>> {
        let q = q.clone();
        self.read(move |c| list_tasks(c, &q)).await
    }

    async fn next_lease_end(&self) -> Result<Option<Millis>> {
        self.read(|c| {
            c.query_row(
                "SELECT MIN(expires_at) FROM tasks WHERE state = 'claimed'",
                [],
                |r| r.get(0),
            )
            .sql()
        })
        .await
    }

    async fn claimed_tasks(&self) -> Result<Vec<String>> {
        self.read(|c| {
            c.prepare_cached(
                "SELECT id FROM tasks WHERE state = 'claimed' ORDER BY created_position",
            )
            .sql()?
            .query_map([], |r| r.get(0))
            .sql()?
            .collect::<rusqlite::Result<_>>()
            .sql()
        })
        .await
    }

    async fn prune_idempotency(&self, before: Millis) -> Result<u64> {
        self.on_writer(move |w| {
            w.conn
                .prepare_cached("DELETE FROM idempotency WHERE created_at < ?1")
                .sql()?
                .execute(params![before])
                .sql()
                .map(|n| n as u64)
        })
        .await
    }

    async fn checkpoint(&self) -> Result<()> {
        self.on_writer(|w| w.checkpoint()).await
    }
}
