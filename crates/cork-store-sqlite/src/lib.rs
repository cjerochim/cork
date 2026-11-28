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
use cork_core::channel::Shape;
use cork_core::labels::{MENTION_LABEL, TASK_LABEL};
use cork_core::{
    BoardError, ChannelSelector, ErrorCode, Message, Millis, Pattern, Result, Task, TaskState,
    task_stream, thread_stream,
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

-- Every channel that any event, message or task has used. Wildcard patterns
-- are matched against this list to get the exact channels to query (HANDOFF 19.5).
CREATE TABLE IF NOT EXISTS known_channels (path TEXT PRIMARY KEY) WITHOUT ROWID;

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

-- Declared dependencies, fixed at creation (HANDOFF 6.8.6). Replacement
-- links live in each task's body; chains are at most 16 long.
CREATE TABLE IF NOT EXISTS task_dependencies (
  task_id    TEXT NOT NULL,
  depends_on TEXT NOT NULL,
  PRIMARY KEY (task_id, depends_on)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS task_dependencies_by_dependency ON task_dependencies (depends_on, task_id);

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
        fill_known_channels(&w)?;
        register_functions(&w)?;

        let mut readers = Vec::new();
        for _ in 0..cfg.readers.max(1) {
            let r = Connection::open_with_flags(
                &cfg.path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .sql()?;
            r.busy_timeout(std::time::Duration::from_secs(5)).sql()?;
            register_functions(&r)?;
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

static PATTERN_LIST_MAX: AtomicUsize = AtomicUsize::new(500);

/// How many known channels a wildcard pattern may expand to before a query
/// falls back to scanning by prefix (HANDOFF 19.5). 500 by default.
pub fn pattern_list_max() -> usize {
    PATTERN_LIST_MAX.load(Ordering::Relaxed)
}

/// Change [`pattern_list_max`] for this process. For tests and the stress
/// run, which check that both paths give the same answer.
#[doc(hidden)]
pub fn set_pattern_list_max(n: usize) {
    PATTERN_LIST_MAX.store(n, Ordering::Relaxed);
}

/// Record that a channel exists. Cheap: a primary-key probe.
fn know_channel(tx: &Connection, channel: &str) -> Result<()> {
    tx.prepare_cached("INSERT OR IGNORE INTO known_channels (path) VALUES (?1)")
        .sql()?
        .execute(params![channel])
        .sql()?;
    Ok(())
}

/// A database made before `known_channels` existed gets it filled once.
fn fill_known_channels(c: &Connection) -> Result<()> {
    let done: bool = c
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM meta WHERE key = 'known_channels')",
            [],
            |r| r.get(0),
        )
        .sql()?;
    if done {
        return Ok(());
    }
    c.execute_batch(
        "BEGIN IMMEDIATE;
         INSERT OR IGNORE INTO known_channels (path)
           SELECT DISTINCT channel FROM events WHERE channel IS NOT NULL;
         INSERT OR IGNORE INTO known_channels (path) SELECT DISTINCT channel FROM messages;
         INSERT OR IGNORE INTO known_channels (path) SELECT DISTINCT channel FROM tasks;
         INSERT OR IGNORE INTO meta (key, value) VALUES ('known_channels', '1');
         COMMIT;",
    )
    .sql()
}

/// The known channels a wildcard pattern matches, by the core matcher, or
/// `None` if there are more than [`pattern_list_max`].
fn known_matching(c: &Connection, prefix: &str, pat: &Pattern) -> Result<Option<Vec<String>>> {
    let max = pattern_list_max();
    let mut out = Vec::new();
    let mut push = |ch: String| {
        if pat.matches(&ch) {
            out.push(ch);
        }
        out.len() <= max
    };
    if prefix.is_empty() {
        let mut st = c
            .prepare_cached("SELECT path FROM known_channels ORDER BY path")
            .sql()?;
        let mut rows = st.query([]).sql()?;
        while let Some(r) = rows.next().sql()? {
            if !push(r.get(0).sql()?) {
                return Ok(None);
            }
        }
    } else {
        // `prefix` itself, then everything from `prefix/` up to `prefix0`
        // (`0` is the byte after `/`): exactly the channels under it.
        let mut st = c
            .prepare_cached(
                "SELECT path FROM known_channels WHERE path = ?1 OR (path > ?2 AND path < ?3) ORDER BY path",
            )
            .sql()?;
        let mut rows = st
            .query(params![prefix, format!("{prefix}/"), format!("{prefix}0")])
            .sql()?;
        while let Some(r) = rows.next().sql()? {
            if !push(r.get(0).sql()?) {
                return Ok(None);
            }
        }
    }
    Ok(Some(out))
}

/// `cork_match(pattern, channel)`: the core matcher, for the fallback scan.
fn register_functions(c: &Connection) -> Result<()> {
    use rusqlite::functions::FunctionFlags;
    c.create_scalar_function(
        "cork_match",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let pat =
                ctx.get_or_create_aux(0, |v| -> std::result::Result<Pattern, BoardError> {
                    Pattern::parse(
                        v.as_str()
                            .map_err(|e| BoardError::internal(e.to_string()))?,
                    )
                })?;
            Ok(match ctx.get_raw(1).as_str_or_null() {
                Ok(Some(ch)) => pat.matches(ch),
                _ => false,
            })
        },
    )
    .sql()
}

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
    /// The channel test for a selector (HANDOFF 19.5): any ordinary pattern
    /// and no `!` pattern. Plain channels and subtrees compare bytes, never
    /// `LIKE` (`_` is a legal channel character). A wildcard pattern is
    /// first matched against `known_channels` in this same transaction, and
    /// the query filters on that exact list, so the `(channel, …)` indexes
    /// still apply. A list longer than [`pattern_list_max`] falls back to a
    /// prefix scan filtered by the core matcher.
    fn channel(&mut self, c: &Connection, col: &str, sel: &ChannelSelector) -> Result<()> {
        let mut any = Vec::new();
        for p in sel.includes() {
            any.push(self.pattern(c, col, p)?);
        }
        let mut clause = format!("({})", any.join(" OR "));
        for p in sel.excludes() {
            let not = self.pattern(c, col, p)?;
            clause = format!("{clause} AND NOT {not}");
        }
        self.and(format!("({clause})"));
        Ok(())
    }
    fn subtree(&mut self, col: &str, p: &str) -> String {
        let a = self.p(p.to_string());
        let prefix = format!("{p}/");
        let n = self.p(prefix.len() as i64);
        let b = self.p(prefix);
        format!("({col} = {a} OR substr({col}, 1, {n}) = {b})")
    }
    fn pattern(&mut self, c: &Connection, col: &str, pat: &Pattern) -> Result<String> {
        Ok(match pat.shape() {
            Shape::Exact(p) => {
                let a = self.p(p);
                format!("({col} = {a})")
            }
            Shape::Subtree(p) => self.subtree(col, &p),
            Shape::All => format!("({col} IS NOT NULL)"),
            Shape::Wild { prefix, pattern } => {
                let list = known_matching(c, &prefix, pattern)?;
                match list {
                    Some(list) if list.is_empty() => "(0)".to_string(),
                    Some(list) => {
                        let ps: Vec<String> = list.into_iter().map(|ch| self.p(ch)).collect();
                        format!("({col} IN ({}))", ps.join(", "))
                    }
                    None => {
                        let m = self.p(pattern.to_string());
                        let test = format!("cork_match({m}, {col})");
                        if prefix.is_empty() {
                            format!("({test})")
                        } else {
                            let under = self.subtree(col, &prefix);
                            format!("({under} AND {test})")
                        }
                    }
                }
            }
        })
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
        s.channel(c, "t.channel", ch)?;
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

fn dependents(c: &Connection, ids: &[String]) -> Result<Vec<String>> {
    let mut found: Vec<(i64, String)> = vec![];
    let mut st = c
        .prepare_cached(
            "SELECT t.created_position, d.task_id FROM task_dependencies d \
             JOIN tasks t ON t.id = d.task_id WHERE d.depends_on = ?1",
        )
        .sql()?;
    for id in ids {
        for r in st
            .query_map(params![id], |r| Ok((r.get(0)?, r.get(1)?)))
            .sql()?
        {
            found.push(r.sql()?);
        }
    }
    found.sort();
    found.dedup();
    Ok(found.into_iter().map(|(_, id)| id).collect())
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
        s.channel(c, "e.channel", ch)?;
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
            "(EXISTS (SELECT 1 FROM event_labels l WHERE l.key = {a} AND l.value = {b} AND l.position = e.position) \
             OR (substr(e.stream_id, 1, 5) = 'task:' AND EXISTS (SELECT 1 FROM task_labels tl \
                 WHERE tl.task_id = e.thread_id AND tl.key = {a} AND tl.value = {b})))"
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
    add_task_labels(c, events)
}

/// Task events store only `board.task`; join the task's own labels on read.
fn add_task_labels(c: &Connection, events: &mut [StoredEvent]) -> Result<()> {
    let mut cache: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for e in events.iter_mut() {
        if !e.stream_id.starts_with("task:") {
            continue;
        }
        let Some(task_id) = e.meta.thread_id.clone() else {
            continue;
        };
        if !cache.contains_key(&task_id) {
            let rows: Vec<(String, String)> = c
                .prepare_cached(
                    "SELECT key, value FROM task_labels WHERE task_id = ?1 ORDER BY key",
                )
                .sql()?
                .query_map(params![task_id], |r| Ok((r.get(0)?, r.get(1)?)))
                .sql()?
                .collect::<rusqlite::Result<_>>()
                .sql()?;
            cache.insert(task_id.clone(), rows);
        }
        for l in &cache[&task_id] {
            if !e.meta.labels.contains(l) {
                e.meta.labels.push(l.clone());
            }
        }
    }
    Ok(())
}

fn list_messages(c: &Connection, q: &MessageQuery) -> Result<Vec<StoredMessage>> {
    let mut s = Query::new();
    let now = s.p(q.now);
    if let Some(ch) = &q.channel {
        s.channel(c, "m.channel", ch)?;
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
    fn head(&mut self) -> Result<Position> {
        head(self.0)
    }
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
    fn dependents(&mut self, ids: &[String]) -> Result<Vec<String>> {
        dependents(self.0, ids)
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
    // Positions are explicit so they never have gaps (see `WriteTx::head`).
    let mut position = head(tx)?;

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
                "INSERT INTO events (position, stream_id, version, type, schema, data, actor, at, channel, thread_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            )
            .sql()?
            .execute(params![
                (position + 1) as i64,
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
            if let Some(ch) = &e.meta.channel {
                know_channel(tx, ch)?;
            }
            position += 1;
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

    let head_now = position;
    for p in &commit.projections {
        match p {
            Projection::Message(m) => {
                let stream = thread_stream(&m.thread_id);
                let pos = last_pos.get(stream.as_str()).copied().unwrap_or(head_now);
                project_message(tx, m, pos)?
            }
            Projection::Task(t) => {
                let stream = task_stream(&t.id);
                let created = first_pos.get(stream.as_str()).copied().unwrap_or(head_now);
                project_task(tx, t, created)?
            }
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
    add_task_labels(tx, &mut out)?;
    Ok(out)
}

/// Store one event as given, position included (import).
fn insert_event(tx: &Connection, e: &StoredEvent) -> Result<()> {
    tx.prepare_cached(
        "INSERT INTO events (position, stream_id, version, type, schema, data, actor, at, channel, thread_id) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )
    .sql()?
    .execute(params![
        e.position as i64,
        e.stream_id,
        e.version as i64,
        e.event_type,
        e.schema as i64,
        to_json(&e.data)?,
        e.actor,
        e.at,
        e.meta.channel,
        e.meta.thread_id
    ])
    .sql()?;
    if let Some(ch) = &e.meta.channel {
        know_channel(tx, ch)?;
    }
    let task_event = e.stream_id.starts_with("task:");
    let mut st = tx
        .prepare_cached(
            "INSERT OR IGNORE INTO event_labels (key, value, position) VALUES (?1, ?2, ?3)",
        )
        .sql()?;
    for (k, v) in &e.meta.labels {
        // Task events store only board labels; their task's labels are joined on read.
        if task_event && !cork_core::labels::is_reserved(k) {
            continue;
        }
        st.execute(params![k, v, e.position as i64]).sql()?;
    }
    Ok(())
}

/// Write every projection row the log implies.
fn write_derived(tx: &Connection, events: &[StoredEvent]) -> Result<()> {
    for e in events {
        if let Some(ch) = &e.meta.channel {
            know_channel(tx, ch)?;
        }
    }
    let d = cork_store::replay::derive(events)?;
    for (m, pos) in &d.messages {
        project_message(tx, m, *pos)?;
    }
    for (t, created) in &d.tasks {
        project_task(tx, t, *created)?;
    }
    Ok(())
}

const PROJECTION_TABLES: [&str; 7] = [
    "messages",
    "message_labels",
    "channels",
    "known_channels",
    "tasks",
    "task_labels",
    "task_dependencies",
];

impl SqliteStore {
    /// Drop every projection row and build them again from the log
    /// (`cork debug rebuild`, on a copy of the database).
    pub async fn rebuild_projections(&self) -> Result<()> {
        self.on_writer(|w| {
            let tx = w
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .sql()?;
            for t in PROJECTION_TABLES {
                tx.execute(&format!("DELETE FROM {t}"), []).sql()?;
            }
            let mut events = vec![];
            let mut after = 0;
            loop {
                let page = read_events(&tx, &EventFilter::after(after, MAX_LIMIT as usize))?;
                let n = page.events.len();
                if let Some(e) = page.events.last() {
                    after = e.position;
                }
                events.extend(page.events);
                if n < MAX_LIMIT as usize {
                    break;
                }
            }
            write_derived(&tx, &events)?;
            tx.commit().sql()
        })
        .await
    }

    /// Every projection row as a sorted line of text, for diffing.
    pub async fn projection_dump(&self) -> Result<Vec<String>> {
        self.read(|c| {
            let mut out = vec![];
            for t in PROJECTION_TABLES {
                let mut st = c.prepare(&format!("SELECT * FROM {t}")).sql()?;
                let cols = st.column_count();
                let rows = st
                    .query_map([], |r| {
                        let mut parts = vec![t.to_string()];
                        for i in 0..cols {
                            let v: Value = r.get(i)?;
                            parts.push(format!("{v:?}"));
                        }
                        Ok(parts.join(" | "))
                    })
                    .sql()?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .sql()?;
                out.extend(rows);
            }
            out.sort();
            Ok(out)
        })
        .await
    }
}

fn project_message(tx: &Connection, m: &Message, pos: u64) -> Result<()> {
    know_channel(tx, &m.channel)?;
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

/// Insert or update a task row. `created` is its queue place, used on insert.
fn project_task(tx: &Connection, t: &Task, created: u64) -> Result<()> {
    know_channel(tx, &t.channel)?;
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
    let mut st = tx
        .prepare_cached(
            "INSERT OR IGNORE INTO task_dependencies (task_id, depends_on) VALUES (?1, ?2)",
        )
        .sql()?;
    for d in &t.depends_on {
        st.execute(params![t.id, d]).sql()?;
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

    async fn dependents(&self, ids: &[String]) -> Result<Vec<String>> {
        let ids = ids.to_vec();
        self.read(move |c| dependents(c, &ids)).await
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

    async fn import(&self, events: Vec<StoredEvent>) -> Result<()> {
        self.on_writer(move |w| {
            let tx = w
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .sql()?;
            if head(&tx)? != 0 {
                return Err(BoardError::new(
                    ErrorCode::InvalidState,
                    "import needs an empty store",
                ));
            }
            for (i, e) in events.iter().enumerate() {
                if e.position != i as u64 + 1 {
                    return Err(BoardError::invalid(format!(
                        "import: position {} at line {}: positions must start at 1 with no gaps",
                        e.position,
                        i + 1
                    )));
                }
                if e.version != stream_version(&tx, &e.stream_id)? + 1 {
                    return Err(BoardError::invalid(format!(
                        "import: {} version {} is out of order",
                        e.stream_id, e.version
                    )));
                }
                insert_event(&tx, e)?;
            }
            write_derived(&tx, &events)?;
            tx.commit().sql()
        })
        .await
    }
}
