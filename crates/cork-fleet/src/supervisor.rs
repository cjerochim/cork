//! `cork workers up`: the supervisor and dispatcher (HANDOFF 16.4, 16.5).
//!
//! The supervisor is a plain client. It follows `/events` for its pools'
//! channels, lists ready tasks oldest first, and gives each to a free worker
//! of the cheapest pool that covers it. A worker is one child,
//! `cork --as <fleet>-<pool>-<n> tasks run <id> -- <command>`, so draining
//! needs nothing from `tasks run`: a worker that is not given a new task
//! simply stops. Leases protect the work; the supervisor only keeps the
//! head count.

use crate::config::{Fleet, Pool};
use crate::routing::{TaskFacts, choose, overlaps, rank, verdict};
use cork_api::{ChannelsOut, TasksPage};
use cork_client::home::{Connect, reconnect};
use cork_client::{Client, query};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, Notify};

/// The channel the fleet reports paused pools in (16.4).
pub const FLEET_CHANNEL: &str = "cork/fleet";
/// Crashes in this window pause a pool.
pub const CRASH_LIMIT: usize = 5;
pub const CRASH_WINDOW: Duration = Duration::from_secs(60);
/// A command that fails sooner than this after its task started counts as
/// a crash: it is broken, not the task.
pub const QUICK_FAIL: Duration = Duration::from_secs(2);
const FIRST_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const DAEMON_FIRST_BACKOFF: Duration = Duration::from_secs(1);
/// How long a task that just went wrong is left alone before it is tried again.
const COOLDOWN: Duration = Duration::from_secs(5);
/// A round runs at least this often, even with no event.
const TICK: Duration = Duration::from_secs(2);
const STDERR_KEEP: usize = 2048;

/// Error codes a child can exit 1 with when another client got there first.
/// Those are races, not crashes.
const RACE_CODES: [&str; 4] = ["task_claimed", "invalid_state", "not_found", "forbidden"];

pub struct Options {
    pub fleet: Fleet,
    /// The `cork` binary that children run.
    pub cork_exe: PathBuf,
    pub conn: Connect,
    /// Where the control socket goes: `<home>/workers/<fleet>.sock`.
    pub home: PathBuf,
}

pub fn socket_path(home: &std::path::Path, fleet: &str) -> PathBuf {
    home.join("workers").join(format!("{fleet}.sock"))
}

#[derive(Debug)]
struct Busy {
    task_id: String,
    pid: Option<u32>,
    started: Instant,
    /// We sent the signal, so its death is not a crash.
    stopping: bool,
}

#[derive(Debug)]
struct Worker {
    slot: usize,
    profile: String,
    /// The pool generation this worker runs (16.5: reload drains old ones).
    generation: u64,
    busy: Option<Busy>,
    draining: bool,
    not_before: Instant,
    backoff: Duration,
    idle_since: Instant,
}

#[derive(Debug)]
struct PoolState {
    spec: Arc<Pool>,
    generation: u64,
    target: usize,
    workers: Vec<Worker>,
    paused: Option<String>,
    crashes: VecDeque<Instant>,
    restarts: u64,
    last_error: Option<String>,
    /// Gone from the config: drain, then drop.
    removed: bool,
}

impl PoolState {
    fn new(spec: Pool) -> Self {
        Self {
            target: spec.count,
            spec: Arc::new(spec),
            generation: 0,
            workers: vec![],
            paused: None,
            crashes: VecDeque::new(),
            restarts: 0,
            last_error: None,
            removed: false,
        }
    }

    fn live(&self) -> impl Iterator<Item = &Worker> {
        self.workers.iter().filter(|w| !w.draining)
    }

    fn free_worker(&self, now: Instant) -> Option<usize> {
        if self.paused.is_some() {
            return None;
        }
        self.workers
            .iter()
            .enumerate()
            .filter(|(_, w)| !w.draining && w.busy.is_none() && w.not_before <= now)
            .min_by_key(|(_, w)| w.slot)
            .map(|(i, _)| i)
    }

    /// Bring the workers in line with the target and the generation.
    fn reconcile(&mut self, fleet: &str, name: &str) {
        let now = Instant::now();
        let generation = self.generation;
        for w in &mut self.workers {
            if w.generation != generation {
                w.draining = true;
            }
        }
        let mut live = self.live().count();
        while live > self.target {
            // Idle workers stop first, then the busy one with the highest slot.
            let pick = self
                .workers
                .iter()
                .enumerate()
                .filter(|(_, w)| !w.draining)
                .max_by_key(|(_, w)| (w.busy.is_none(), w.slot))
                .map(|(i, _)| i)
                .expect("a live worker");
            self.workers[pick].draining = true;
            live -= 1;
        }
        // Drop stopped workers first, so their slots can be reused.
        self.workers.retain(|w| !(w.draining && w.busy.is_none()));
        while live < self.target {
            let used: BTreeSet<usize> = self.workers.iter().map(|w| w.slot).collect();
            let slot = (1..).find(|n| !used.contains(n)).expect("a free slot");
            self.workers.push(Worker {
                slot,
                profile: format!("{fleet}-{name}-{slot}"),
                generation,
                busy: None,
                draining: false,
                not_before: now,
                backoff: FIRST_BACKOFF,
                idle_since: now,
            });
            live += 1;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Shutdown {
    Drain,
    Now,
}

struct State {
    fleet: Fleet,
    person: String,
    pools: BTreeMap<String, PoolState>,
    /// Tasks given to a child that has not exited yet.
    inflight: HashMap<String, (String, usize)>,
    cooldown: HashMap<String, Instant>,
    unmatched: Vec<Value>,
    shutdown: Option<Shutdown>,
    daemon_wait_until: Option<Instant>,
    daemon_backoff: Duration,
    /// Pools whose pause was already posted to `cork/fleet`.
    announced: BTreeSet<String>,
    started: Instant,
    /// Usage the wrappers reported, by (pool, model).
    usage: BTreeMap<(String, String), UsageTotals>,
    /// Times each pool was paused for a crash loop.
    crash_loops: BTreeMap<String, u64>,
}

/// Usage totals from the records wrappers leave in `$CORK_USAGE_REPORT`.
#[derive(Debug, Clone, Default, PartialEq)]
struct UsageTotals {
    attempts: u64,
    input_tokens: u64,
    output_tokens: u64,
    cost_usd: f64,
    /// Attempts whose record had no cost.
    cost_unknown: u64,
}

impl UsageTotals {
    fn add(&mut self, r: &Value) {
        self.attempts += 1;
        self.input_tokens += r["input_tokens"].as_u64().unwrap_or(0);
        self.output_tokens += r["output_tokens"].as_u64().unwrap_or(0);
        match r["cost_usd"].as_f64() {
            Some(c) => self.cost_usd += c,
            None => self.cost_unknown += 1,
        }
    }
}

struct Shared {
    state: Mutex<State>,
    kick: Notify,
    client: Mutex<Client>,
    conn: Connect,
    cork_exe: PathBuf,
    home: PathBuf,
    done: Notify,
}

/// Run the fleet until it is drained or stopped. Returns the exit code.
pub async fn run(o: Options) -> Result<i32, String> {
    let client = cork_client::home::connect(&o.conn)
        .await
        .map_err(|e| e.to_string())?;
    let me = client.whoami().await.map_err(|e| e.to_string())?;
    let sock = socket_path(&o.home, &o.fleet.name);
    let listener = bind(&sock).await?;
    let usage_dir = o
        .home
        .join("workers")
        .join(format!("{}-usage", o.fleet.name));
    std::fs::create_dir_all(&usage_dir).map_err(|e| format!("{}: {e}", usage_dir.display()))?;

    let mut pools = BTreeMap::new();
    for (name, p) in &o.fleet.pools {
        let mut ps = PoolState::new(p.clone());
        ps.reconcile(&o.fleet.name, name);
        pools.insert(name.clone(), ps);
    }
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            fleet: o.fleet,
            person: me.person.clone(),
            pools,
            inflight: HashMap::new(),
            cooldown: HashMap::new(),
            unmatched: vec![],
            shutdown: None,
            daemon_wait_until: None,
            daemon_backoff: DAEMON_FIRST_BACKOFF,
            announced: BTreeSet::new(),
            started: Instant::now(),
            usage: BTreeMap::new(),
            crash_loops: BTreeMap::new(),
        }),
        kick: Notify::new(),
        client: Mutex::new(client),
        conn: o.conn,
        cork_exe: o.cork_exe,
        home: o.home.clone(),
        done: Notify::new(),
    });
    {
        let st = shared.state.lock().await;
        eprintln!(
            "cork workers: fleet {} up as {} with {} pools; control socket {}",
            st.fleet.name,
            me.person,
            st.pools.len(),
            sock.display()
        );
    }

    {
        let channels = board_channels(&shared).await;
        let st = shared.state.lock().await;
        for o in pool_overlaps(&st, &channels) {
            eprintln!("cork workers: {o}");
        }
    }

    tokio::spawn(follow(shared.clone(), me.head));
    tokio::spawn(control(shared.clone(), listener));
    let port = shared.state.lock().await.fleet.metrics_port;
    if let Some(port) = port {
        let l = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .map_err(|e| format!("metrics on 127.0.0.1:{port}: {e}"))?;
        eprintln!(
            "cork workers: metrics on http://{}/metrics",
            l.local_addr().map_err(|e| e.to_string())?
        );
        tokio::spawn(serve_metrics(shared.clone(), l));
    }
    tokio::spawn(signals(shared.clone()));

    loop {
        tokio::select! {
            _ = shared.kick.notified() => {}
            _ = tokio::time::sleep(TICK) => {}
            _ = shared.done.notified() => {}
        }
        if finished(&shared).await {
            break;
        }
        round(&shared).await;
    }
    let _ = std::fs::remove_file(&sock);
    eprintln!("cork workers: fleet stopped");
    Ok(0)
}

async fn finished(shared: &Shared) -> bool {
    let st = shared.state.lock().await;
    st.shutdown.is_some()
        && st
            .pools
            .values()
            .all(|p| p.workers.iter().all(|w| w.busy.is_none()))
}

async fn bind(sock: &std::path::Path) -> Result<tokio::net::UnixListener, String> {
    if let Some(dir) = sock.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
        }
    }
    if sock.exists() {
        if tokio::net::UnixStream::connect(sock).await.is_ok() {
            return Err(format!(
                "a fleet is already running on {}; stop it with `cork workers down`",
                sock.display()
            ));
        }
        let _ = std::fs::remove_file(sock);
    }
    let l = tokio::net::UnixListener::bind(sock).map_err(|e| format!("{}: {e}", sock.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(sock, std::fs::Permissions::from_mode(0o600));
    }
    Ok(l)
}

// ---------- following the board ----------

/// Long-poll `/events` for each distinct selector, and kick the dispatcher
/// on any change in a pool's channels.
async fn follow(shared: Arc<Shared>, head: u64) {
    let mut cursors: HashMap<String, u64> = HashMap::new();
    let mut tasks: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    loop {
        let selectors: BTreeSet<String> = {
            let st = shared.state.lock().await;
            st.pools
                .values()
                .filter(|p| !p.removed)
                .map(|p| p.spec.select.to_param())
                .collect()
        };
        for sel in &selectors {
            if !tasks.contains_key(sel) {
                let start = *cursors.entry(sel.clone()).or_insert(head);
                tasks.insert(
                    sel.clone(),
                    tokio::spawn(follow_one(shared.clone(), sel.clone(), start)),
                );
            }
        }
        tasks.retain(|sel, h| {
            let keep = selectors.contains(sel);
            if !keep {
                h.abort();
            }
            keep
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn follow_one(shared: Arc<Shared>, sel: String, mut after: u64) {
    let wait = Duration::from_millis(cork_api::DEFAULT_MAX_WAIT_MS);
    loop {
        let client = shared.client.lock().await.clone();
        let p = vec![
            ("channel", sel.clone()),
            ("after", after.to_string()),
            ("wait_ms", wait.as_millis().to_string()),
        ];
        match client.events(&p, wait).await {
            Ok(page) => {
                if !page.events.is_empty() {
                    shared.kick.notify_one();
                }
                after = page.next_after;
            }
            Err(e) => {
                if e.is_transient()
                    && let Some(c) = reconnect(&shared.conn).await
                {
                    *shared.client.lock().await = c;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

// ---------- dispatch ----------

struct Ready {
    position: u64,
    task: cork_core::Task,
}

/// List ready tasks for every distinct (selector, from), oldest first.
async fn ready_tasks(
    client: &Client,
    wants: &BTreeSet<(String, Option<String>)>,
) -> Option<Vec<Ready>> {
    let mut all: BTreeMap<u64, cork_core::Task> = BTreeMap::new();
    for (sel, owner) in wants {
        let mut after = 0u64;
        loop {
            let mut p = vec![
                ("channel", sel.clone()),
                ("state", "ready".to_string()),
                ("limit", "1000".to_string()),
                ("after", after.to_string()),
            ];
            if let Some(o) = owner {
                p.push(("owner", o.clone()));
            }
            let v = client.get(&format!("/tasks{}", query(&p))).await.ok()?;
            let page: TasksPage = serde_json::from_value(v).ok()?;
            let n = page.tasks.len();
            for t in page.tasks {
                after = after.max(t.created_position);
                all.insert(t.created_position, t.task);
            }
            if n < 1000 {
                break;
            }
        }
    }
    Some(
        all.into_iter()
            .map(|(position, task)| Ready { position, task })
            .collect(),
    )
}

async fn round(shared: &Arc<Shared>) {
    let (wants, person) = {
        let st = shared.state.lock().await;
        if st.shutdown.is_some() {
            return;
        }
        if st.daemon_wait_until.is_some_and(|t| Instant::now() < t) {
            return;
        }
        let wants: BTreeSet<(String, Option<String>)> = st
            .pools
            .values()
            .filter(|p| !p.removed)
            .map(|p| {
                let owner = match p.spec.from.as_deref() {
                    Some("*") => None,
                    Some(o) => Some(o.to_string()),
                    None => Some(st.person.clone()),
                };
                (p.spec.select.to_param(), owner)
            })
            .collect();
        (wants, st.person.clone())
    };
    if wants.is_empty() {
        return;
    }
    let client = shared.client.lock().await.clone();
    let Some(ready) = ready_tasks(&client, &wants).await else {
        if let Some(c) = reconnect(&shared.conn).await {
            *shared.client.lock().await = c;
        }
        return;
    };

    let mut st = shared.state.lock().await;
    if st.shutdown.is_some() {
        return;
    }
    let now = Instant::now();
    st.cooldown.retain(|_, t| *t > now);
    autoscale(&mut st, &ready, &person, now);

    let mut unmatched = vec![];
    let fleet_name = st.fleet.name.clone();
    for r in &ready {
        let id = &r.task.id;
        if st.inflight.contains_key(id) || st.cooldown.contains_key(id) {
            continue;
        }
        let facts = TaskFacts::of(&r.task, &st.fleet.catalog);
        let specs: Vec<Arc<Pool>> = st
            .pools
            .values()
            .filter(|p| !p.removed)
            .map(|p| p.spec.clone())
            .collect();
        if rank(specs.iter().map(|p| p.as_ref()), &facts, &person, true).is_empty() {
            // Only tasks with needs count as unmatched: a task without needs
            // that no pool takes is somebody else's (another owner, say).
            if !facts.needs.keywords.is_empty() || !facts.needs.unknown.is_empty() {
                let why: Vec<String> = specs
                    .iter()
                    .map(|p| format!("{}: {}", p.name, verdict(p, &facts, &person, true)))
                    .collect();
                unmatched.push(json!({
                    "task": id,
                    "channel": r.task.channel,
                    "position": r.position,
                    "needs": facts.needs.keywords,
                    "unknown": facts.needs.unknown,
                    "why": why,
                }));
            }
            continue;
        }
        let free: BTreeSet<String> = st
            .pools
            .iter()
            .filter(|(_, p)| !p.removed && p.free_worker(now).is_some())
            .map(|(n, _)| n.clone())
            .collect();
        let Some(pool) = choose(specs.iter().map(|p| p.as_ref()), &facts, &person, |p| {
            free.contains(&p.name)
        }) else {
            continue;
        };
        let pool_name = pool.name.clone();
        start(shared, &mut st, &fleet_name, &pool_name, id);
    }
    st.unmatched = unmatched;
}

/// Grow or shrink pools with `[autoscale]` (16.5), within min and max.
fn autoscale(st: &mut State, ready: &[Ready], person: &str, now: Instant) {
    let cap = st.fleet.max_workers;
    let names: Vec<String> = st.pools.keys().cloned().collect();
    for name in names {
        let total: usize = st.pools.values().map(|p| p.target).sum();
        let catalog = &st.fleet.catalog;
        let p = st.pools.get(&name).expect("pool");
        let Some(a) = p.spec.autoscale.clone() else {
            continue;
        };
        if p.removed || p.paused.is_some() {
            continue;
        }
        let covered = ready
            .iter()
            .filter(|r| !st.inflight.contains_key(&r.task.id))
            .filter(|r| verdict(&p.spec, &TaskFacts::of(&r.task, catalog), person, true).covers())
            .count();
        let live = p.live().count();
        let idle_long = p
            .live()
            .filter(|w| w.busy.is_none() && now.duration_since(w.idle_since) >= a.idle_after)
            .count();
        let grow =
            covered > 0 && (live == 0 || covered as f64 / live as f64 > a.target_ready_per_worker);
        let fleet = st.fleet.name.clone();
        let p = st.pools.get_mut(&name).expect("pool");
        if grow && p.target < p.spec.max && total < cap {
            p.target += 1;
            p.reconcile(&fleet, &name);
        } else if !grow && idle_long > 0 && p.target > p.spec.min {
            p.target -= 1;
            p.reconcile(&fleet, &name);
        }
    }
}

/// Start a child on the pool's free worker for this task.
fn start(shared: &Arc<Shared>, st: &mut State, fleet: &str, pool_name: &str, task_id: &str) {
    let now = Instant::now();
    let p = st.pools.get_mut(pool_name).expect("pool");
    let Some(i) = p.free_worker(now) else {
        return;
    };
    let spec = p.spec.clone();
    let w = &mut p.workers[i];
    let report = usage_report_path(&shared.home, fleet, &w.profile);
    let _ = std::fs::remove_file(&report);
    let mut cmd = tokio::process::Command::new(&shared.cork_exe);
    cmd.arg("--as")
        .arg(&w.profile)
        .args(["tasks", "run", task_id]);
    if let Some(l) = spec.lease {
        cmd.arg("--lease").arg(format!("{}ms", l.as_millis()));
    }
    if let Some(m) = spec.max_runtime {
        cmd.arg("--max-runtime").arg(format!("{}ms", m.as_millis()));
    }
    cmd.arg("--").args(&spec.command);
    cmd.envs(&spec.env)
        .env("CORK_MODEL", &spec.model.name)
        .env("CORK_POOL", &spec.name)
        .env("CORK_TIER", spec.model.tier.to_string())
        .env("CORK_FLEET", fleet)
        .env("CORK_USAGE_REPORT", &report)
        .env_remove("CORK_CLAIM")
        .env_remove("CORK_TASK")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(false);
    if let Some(d) = &spec.cwd {
        cmd.current_dir(d);
    }
    #[cfg(unix)]
    {
        // Its own group, so a Ctrl-C at the terminal reaches only the
        // supervisor, which drains. On Linux the child also dies with us.
        cmd.process_group(0);
        #[cfg(target_os = "linux")]
        unsafe {
            cmd.pre_exec(|| {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // The parent may have died before prctl took effect.
                if libc::getppid() == 1 {
                    return Err(std::io::Error::other("supervisor gone"));
                }
                Ok(())
            });
        }
    }
    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("could not start {}: {e}", shared.cork_exe.display());
            eprintln!("cork workers: {msg}");
            crashed(st, pool_name, i, msg);
            return;
        }
    };
    let pid = child.id();
    let profile = w.profile.clone();
    let slot = w.slot;
    w.busy = Some(Busy {
        task_id: task_id.to_string(),
        pid,
        started: now,
        stopping: false,
    });
    st.inflight
        .insert(task_id.to_string(), (pool_name.to_string(), slot));
    eprintln!(
        "cork workers: {profile} ({}) takes {task_id}",
        spec.model.name
    );
    tokio::spawn(watch(
        shared.clone(),
        child,
        pool_name.to_string(),
        slot,
        profile,
        task_id.to_string(),
    ));
}

/// Forward a child's output with its profile in front, keep its stderr
/// tail, and handle its exit by code (16.4).
async fn watch(
    shared: Arc<Shared>,
    mut child: tokio::process::Child,
    pool: String,
    slot: usize,
    profile: String,
    task_id: String,
) {
    let tail = Arc::new(std::sync::Mutex::new(String::new()));
    let out = child.stdout.take().map(|s| {
        let profile = profile.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(s).lines();
            let mut so = tokio::io::stdout();
            while let Ok(Some(l)) = lines.next_line().await {
                let _ = so.write_all(format!("[{profile}] {l}\n").as_bytes()).await;
            }
        })
    });
    let err = child.stderr.take().map(|s| {
        let profile = profile.clone();
        let tail = tail.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(s).lines();
            let mut se = tokio::io::stderr();
            while let Ok(Some(l)) = lines.next_line().await {
                let _ = se.write_all(format!("[{profile}] {l}\n").as_bytes()).await;
                let mut t = tail.lock().unwrap();
                t.push_str(&l);
                t.push('\n');
                if t.len() > STDERR_KEEP {
                    let cut = t.len() - STDERR_KEEP;
                    let cut = t.ceil_char_boundary(cut);
                    t.drain(..cut);
                }
            }
        })
    });
    let status = child.wait().await;
    for h in [out, err].into_iter().flatten() {
        let _ = h.await;
    }
    let tail = tail.lock().unwrap().clone();
    let code = status.as_ref().ok().and_then(|s| s.code());
    #[cfg(unix)]
    let signal = {
        use std::os::unix::process::ExitStatusExt;
        status.as_ref().ok().and_then(|s| s.signal())
    };
    #[cfg(not(unix))]
    let signal: Option<i32> = None;

    let mut st = shared.state.lock().await;
    st.inflight.remove(&task_id);
    let fleet_name = st.fleet.name.clone();
    let report = usage_report_path(&shared.home, &fleet_name, &profile);
    if let Some(r) = std::fs::read_to_string(&report)
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
    {
        let model = r["model"].as_str().map(str::to_string).unwrap_or_else(|| {
            st.pools
                .get(&pool)
                .map(|p| p.spec.model.name.clone())
                .unwrap_or_default()
        });
        st.usage.entry((pool.clone(), model)).or_default().add(&r);
    }
    let _ = std::fs::remove_file(&report);
    let Some(p) = st.pools.get_mut(&pool) else {
        return;
    };
    let Some(i) = p.workers.iter().position(|w| w.slot == slot) else {
        return;
    };
    let busy = p.workers[i].busy.take().expect("busy worker");
    let ran = busy.started.elapsed();
    let now = Instant::now();
    p.workers[i].idle_since = now;
    let race = code == Some(1)
        && RACE_CODES
            .iter()
            .any(|c| tail.contains(&format!("cork: {c}:")));
    let outcome = match (code, signal) {
        _ if busy.stopping => Outcome::Free,
        (Some(0), _) => Outcome::Success,
        (Some(8), _) if ran < QUICK_FAIL => Outcome::Crash,
        (Some(3 | 4 | 5 | 7 | 8 | 9 | 10), _) => Outcome::Free,
        (Some(1), _) if race => Outcome::Free,
        (Some(11), _) => Outcome::DaemonLost,
        (Some(130 | 143), _) => Outcome::Free,
        _ => Outcome::Crash,
    };
    let what = match (code, signal) {
        (Some(c), _) => format!("exit {c}"),
        (None, Some(s)) => format!("signal {s}"),
        _ => "unknown exit".to_string(),
    };
    eprintln!("cork workers: {profile} finished {task_id}: {what}");
    match outcome {
        Outcome::Success => {
            p.workers[i].backoff = FIRST_BACKOFF;
            st.daemon_backoff = DAEMON_FIRST_BACKOFF;
        }
        Outcome::Free => {}
        Outcome::DaemonLost => {
            let b = st.daemon_backoff;
            st.daemon_wait_until = Some(now + b);
            st.daemon_backoff = (b * 2).min(MAX_BACKOFF);
            st.cooldown.insert(task_id.clone(), now + b);
        }
        Outcome::Crash => {
            st.cooldown.insert(task_id.clone(), now + COOLDOWN);
            let last = tail.lines().last().unwrap_or("").trim().to_string();
            let msg = if last.is_empty() {
                format!("{profile}: {what} on {task_id}")
            } else {
                format!("{profile}: {what} on {task_id}: {last}")
            };
            crashed(&mut st, &pool, i, msg);
        }
    }
    let fleet = st.fleet.name.clone();
    if let Some(p) = st.pools.get_mut(&pool) {
        p.reconcile(&fleet, &pool);
        if p.removed && p.workers.is_empty() {
            st.pools.remove(&pool);
        }
    }
    drop(st);
    announce(&shared).await;
    shared.kick.notify_one();
    shared.done.notify_one();
}

enum Outcome {
    Success,
    Free,
    Crash,
    DaemonLost,
}

/// Count a crash: back the worker off, and pause the pool on a crash loop.
fn crashed(st: &mut State, pool: &str, i: usize, msg: String) {
    let now = Instant::now();
    let p = st.pools.get_mut(pool).expect("pool");
    p.restarts += 1;
    p.last_error = Some(msg.clone());
    let w = &mut p.workers[i];
    w.not_before = now + w.backoff;
    w.backoff = (w.backoff * 2).min(MAX_BACKOFF);
    p.crashes.push_back(now);
    while p
        .crashes
        .front()
        .is_some_and(|t| now.duration_since(*t) > CRASH_WINDOW)
    {
        p.crashes.pop_front();
    }
    if p.crashes.len() >= CRASH_LIMIT && p.paused.is_none() {
        *st.crash_loops.entry(pool.to_string()).or_default() += 1;
        let p = st.pools.get_mut(pool).expect("pool");
        eprintln!("cork workers: pool {pool} paused after {CRASH_LIMIT} crashes in 60 s: {msg}");
        p.paused = Some(msg);
    }
}

/// Post one message to `cork/fleet` for each newly paused pool.
async fn announce(shared: &Shared) {
    let posts: Vec<(String, String)> = {
        let mut st = shared.state.lock().await;
        let fleet = st.fleet.name.clone();
        let new: Vec<(String, String)> = st
            .pools
            .iter()
            .filter_map(|(n, p)| p.paused.clone().map(|e| (n.clone(), e)))
            .filter(|(n, _)| !st.announced.contains(n))
            .collect();
        for (n, _) in &new {
            st.announced.insert(n.clone());
        }
        new.into_iter()
            .map(|(n, e)| {
                (
                    n.clone(),
                    format!(
                        "Fleet {fleet}: pool {n} is paused after {CRASH_LIMIT} crashes in 60 s. Last error: {e}\nResume it with `cork workers scale {n} <n>` or `cork workers reload`."
                    ),
                )
            })
            .collect()
    };
    if posts.is_empty() {
        return;
    }
    let client = shared.client.lock().await.clone();
    for (pool, text) in posts {
        let mut labels = cork_core::Labels::new();
        labels.insert("fleet.pool".into(), pool);
        let m = cork_api::PostMessage {
            text,
            labels,
            data: None,
        };
        if let Err(e) = client.post_message(FLEET_CHANNEL, &m, None).await {
            eprintln!("cork workers: could not post to {FLEET_CHANNEL}: {e}");
        }
    }
}

// ---------- metrics ----------

/// Where a worker's wrapper leaves its usage record for the supervisor.
fn usage_report_path(home: &std::path::Path, fleet: &str, profile: &str) -> PathBuf {
    home.join("workers")
        .join(format!("{fleet}-usage"))
        .join(format!("{profile}.json"))
}

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// The supervisor's `/metrics` (18.4): workers per pool, restarts, crash
/// loops, unmatched tasks, and the usage its wrappers reported.
fn metrics_text(st: &State) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let fleet = esc(&st.fleet.name);
    let head = |out: &mut String, name: &str, kind: &str, help: &str| {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
    };
    head(
        &mut out,
        "cork_fleet_workers",
        "gauge",
        "Workers by pool and state.",
    );
    for (n, p) in &st.pools {
        let busy = p.workers.iter().filter(|w| w.busy.is_some()).count();
        let live: Vec<&Worker> = p.live().collect();
        let idle = live.iter().filter(|w| w.busy.is_none()).count();
        let draining = p.workers.iter().filter(|w| w.draining).count();
        for (state, v) in [("busy", busy), ("idle", idle), ("draining", draining)] {
            let _ = writeln!(
                out,
                "cork_fleet_workers{{fleet=\"{fleet}\",pool=\"{}\",model=\"{}\",state=\"{state}\"}} {v}",
                esc(n),
                esc(&p.spec.model.name)
            );
        }
    }
    head(
        &mut out,
        "cork_fleet_pool_target",
        "gauge",
        "Workers each pool aims for.",
    );
    for (n, p) in &st.pools {
        let _ = writeln!(
            out,
            "cork_fleet_pool_target{{fleet=\"{fleet}\",pool=\"{}\"}} {}",
            esc(n),
            p.target
        );
    }
    head(
        &mut out,
        "cork_fleet_pool_paused",
        "gauge",
        "1 while a pool is paused after a crash loop.",
    );
    for (n, p) in &st.pools {
        let _ = writeln!(
            out,
            "cork_fleet_pool_paused{{fleet=\"{fleet}\",pool=\"{}\"}} {}",
            esc(n),
            p.paused.is_some() as u8
        );
    }
    head(
        &mut out,
        "cork_fleet_restarts_total",
        "counter",
        "Worker crashes, each followed by a restart.",
    );
    for (n, p) in &st.pools {
        let _ = writeln!(
            out,
            "cork_fleet_restarts_total{{fleet=\"{fleet}\",pool=\"{}\"}} {}",
            esc(n),
            p.restarts
        );
    }
    head(
        &mut out,
        "cork_fleet_crash_loops_total",
        "counter",
        "Times a pool was paused for a crash loop.",
    );
    for n in st.pools.keys() {
        let _ = writeln!(
            out,
            "cork_fleet_crash_loops_total{{fleet=\"{fleet}\",pool=\"{}\"}} {}",
            esc(n),
            st.crash_loops.get(n).copied().unwrap_or(0)
        );
    }
    head(
        &mut out,
        "cork_fleet_unmatched_tasks",
        "gauge",
        "Ready tasks with needs no pool covers.",
    );
    let _ = writeln!(
        out,
        "cork_fleet_unmatched_tasks{{fleet=\"{fleet}\"}} {}",
        st.unmatched.len()
    );
    for (name, help) in [
        (
            "cork_fleet_usage_attempts_total",
            "Attempts whose wrapper reported usage.",
        ),
        (
            "cork_fleet_input_tokens_total",
            "Input tokens the wrappers reported.",
        ),
        (
            "cork_fleet_output_tokens_total",
            "Output tokens the wrappers reported.",
        ),
        (
            "cork_fleet_cost_usd_total",
            "Cost the agents reported, in dollars. An estimate.",
        ),
        (
            "cork_fleet_cost_unknown_total",
            "Attempts whose usage had no cost.",
        ),
    ] {
        head(&mut out, name, "counter", help);
        for ((pool, model), u) in &st.usage {
            let v = match name {
                "cork_fleet_usage_attempts_total" => u.attempts as f64,
                "cork_fleet_input_tokens_total" => u.input_tokens as f64,
                "cork_fleet_output_tokens_total" => u.output_tokens as f64,
                "cork_fleet_cost_usd_total" => u.cost_usd,
                _ => u.cost_unknown as f64,
            };
            let _ = writeln!(
                out,
                "{name}{{fleet=\"{fleet}\",pool=\"{}\",model=\"{}\"}} {v}",
                esc(pool),
                esc(model)
            );
        }
    }
    out
}

/// A tiny HTTP server for `GET /metrics` on loopback.
async fn serve_metrics(shared: Arc<Shared>, l: tokio::net::TcpListener) {
    loop {
        let Ok((mut s, _)) = l.accept().await else {
            continue;
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            while !buf.windows(4).any(|w| w == b"\r\n\r\n") && buf.len() < 16 * 1024 {
                match tokio::time::timeout(Duration::from_secs(5), s.read(&mut chunk)).await {
                    Ok(Ok(n)) if n > 0 => buf.extend_from_slice(&chunk[..n]),
                    _ => return,
                }
            }
            let line = String::from_utf8_lossy(&buf);
            let path = line.split_whitespace().nth(1).unwrap_or("");
            let (status, body) = if line.starts_with("GET ") && path == "/metrics" {
                let st = shared.state.lock().await;
                ("200 OK", metrics_text(&st))
            } else {
                ("404 Not Found", "not found\n".to_string())
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = s.write_all(resp.as_bytes()).await;
            let _ = s.shutdown().await;
        });
    }
}

// ---------- control ----------

async fn control(shared: Arc<Shared>, listener: tokio::net::UnixListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            let (r, mut w) = stream.into_split();
            let mut lines = BufReader::new(r).lines();
            let Ok(Some(line)) = lines.next_line().await else {
                return;
            };
            let reply = match serde_json::from_str::<Value>(&line) {
                Ok(req) => handle(&shared, &req).await,
                Err(e) => json!({"ok": false, "error": format!("bad request: {e}")}),
            };
            let _ = w.write_all(format!("{reply}\n").as_bytes()).await;
            let _ = w.shutdown().await;
        });
    }
}

/// The channels on the board now, for overlap reports. Empty if the board
/// can't be reached: the report is information, never a reason to fail.
async fn board_channels(shared: &Shared) -> Vec<String> {
    let client = shared.client.lock().await.clone();
    client
        .get("/channels")
        .await
        .ok()
        .and_then(|v| serde_json::from_value::<ChannelsOut>(v).ok())
        .map(|c| c.channels.into_iter().map(|c| c.path).collect())
        .unwrap_or_default()
}

/// Overlaps between the pools still in the config (HANDOFF 19.7).
fn pool_overlaps(st: &State, channels: &[String]) -> Vec<String> {
    let pools = st
        .pools
        .values()
        .filter(|p| !p.removed)
        .map(|p| p.spec.as_ref());
    overlaps(pools, channels, &st.person)
        .iter()
        .map(|o| o.to_string())
        .collect()
}

fn err(msg: impl Into<String>) -> Value {
    json!({"ok": false, "error": msg.into()})
}

async fn handle(shared: &Arc<Shared>, req: &Value) -> Value {
    let op = req["op"].as_str().unwrap_or("");
    let reply = match op {
        "status" => {
            let channels = board_channels(shared).await;
            let st = shared.state.lock().await;
            let mut v = status(&st);
            v["overlaps"] = json!(pool_overlaps(&st, &channels));
            v
        }
        "metrics" => {
            let st = shared.state.lock().await;
            json!({"ok": true, "text": metrics_text(&st)})
        }
        "scale" => {
            let mut st = shared.state.lock().await;
            scale(
                &mut st,
                req["pool"].as_str().unwrap_or(""),
                req["count"].as_str().unwrap_or(""),
            )
        }
        "drain" => {
            let mut st = shared.state.lock().await;
            let pool = req["pool"].as_str().unwrap_or("");
            let fleet = st.fleet.name.clone();
            match st.pools.get_mut(pool) {
                Some(p) => {
                    p.target = 0;
                    p.reconcile(&fleet, pool);
                    json!({"ok": true, "message": format!("draining pool {pool}")})
                }
                None => err(format!("no pool `{pool}`")),
            }
        }
        "reload" => {
            let channels = board_channels(shared).await;
            let mut st = shared.state.lock().await;
            let mut v = reload(&mut st);
            if v["ok"] == true {
                let o = pool_overlaps(&st, &channels);
                if !o.is_empty() {
                    let m = v["message"].as_str().unwrap_or("").to_string();
                    v["message"] = json!(format!("{m}\n{}", o.join("\n")));
                }
                v["overlaps"] = json!(o);
            }
            v
        }
        "down" => {
            let now = req["now"].as_bool().unwrap_or(false);
            let mut st = shared.state.lock().await;
            down(&mut st, if now { Shutdown::Now } else { Shutdown::Drain });
            json!({"ok": true, "message": if now { "stopping now" } else { "draining" }})
        }
        _ => err(format!("unknown op `{op}`")),
    };
    shared.kick.notify_one();
    shared.done.notify_one();
    reply
}

fn status(st: &State) -> Value {
    let now = Instant::now();
    let pools: Vec<Value> = st
        .pools
        .iter()
        .map(|(n, p)| {
            let workers: Vec<Value> = p
                .workers
                .iter()
                .map(|w| {
                    json!({
                        "profile": w.profile,
                        "task": w.busy.as_ref().map(|b| b.task_id.clone()),
                        "pid": w.busy.as_ref().and_then(|b| b.pid),
                        "draining": w.draining,
                        "backing_off": w.busy.is_none() && w.not_before > now,
                        "stale_model": w.generation != p.generation,
                    })
                })
                .collect();
            let live: Vec<&Worker> = p.live().collect();
            json!({
                "pool": n,
                "model": p.spec.model.name,
                "tier": p.spec.model.tier,
                "select": p.spec.select.to_param(),
                "target": p.target,
                "min": p.spec.min,
                "max": p.spec.max,
                "live": live.len(),
                "busy": p.workers.iter().filter(|w| w.busy.is_some()).count(),
                "idle": live.iter().filter(|w| w.busy.is_none()).count(),
                "draining": p.workers.iter().filter(|w| w.draining).count(),
                "paused": p.paused.is_some(),
                "last_error": p.last_error,
                "restarts": p.restarts,
                "removed": p.removed,
                "autoscale": p.spec.autoscale.is_some(),
                "workers": workers,
            })
        })
        .collect();
    json!({
        "ok": true,
        "fleet": st.fleet.name,
        "person": st.person,
        "config": st.fleet.config_path,
        "max_workers": st.fleet.max_workers,
        "up_secs": st.started.elapsed().as_secs(),
        "shutting_down": st.shutdown.map(|s| if s == Shutdown::Now { "now" } else { "drain" }),
        "pools": pools,
        "unmatched": st.unmatched,
    })
}

/// `n`, `+n` or `-n`, within the pool's min and max and the fleet's cap.
fn scale(st: &mut State, pool: &str, count: &str) -> Value {
    let total: usize = st
        .pools
        .iter()
        .filter(|(n, _)| n.as_str() != pool)
        .map(|(_, p)| p.target)
        .sum();
    let cap = st.fleet.max_workers;
    let fleet = st.fleet.name.clone();
    let Some(p) = st.pools.get_mut(pool) else {
        return err(format!("no pool `{pool}`"));
    };
    if p.removed {
        return err(format!("pool `{pool}` was removed from the config"));
    }
    let cur = p.target as i64;
    let want = if let Some(n) = count.strip_prefix('+') {
        n.parse::<i64>().map(|n| cur + n)
    } else if let Some(n) = count.strip_prefix('-') {
        n.parse::<i64>().map(|n| cur - n)
    } else {
        count.parse::<i64>()
    };
    let Ok(want) = want else {
        return err(format!("scale wants n, +n or -n, got `{count}`"));
    };
    let (min, max) = (p.spec.min as i64, p.spec.max as i64);
    if want < min || want > max {
        return err(format!(
            "pool `{pool}` must stay within min {min} and max {max}; {want} is outside"
        ));
    }
    if total + want as usize > cap {
        return err(format!(
            "that makes {} workers, over the fleet's max_workers {cap}",
            total + want as usize
        ));
    }
    p.target = want as usize;
    // Scaling resumes a paused pool (16.4).
    p.paused = None;
    p.crashes.clear();
    p.reconcile(&fleet, pool);
    st.announced.remove(pool);
    json!({"ok": true, "message": format!("pool {pool}: {cur} -> {want}")})
}

/// Re-read both files and apply count, model and command changes (16.3).
fn reload(st: &mut State) -> Value {
    let path = st.fleet.config_path.clone();
    let new = match Fleet::load(&path) {
        Ok(f) => f,
        Err(e) => return err(format!("kept the running config: {e}")),
    };
    if new.name != st.fleet.name {
        return err(format!(
            "kept the running config: the fleet name changed from {} to {}; run `down` and `up` instead",
            st.fleet.name, new.name
        ));
    }
    let fleet = new.name.clone();
    let mut changes = vec![];
    for (name, spec) in &new.pools {
        match st.pools.get_mut(name) {
            Some(p) => {
                if !p.spec.same_workers(spec) {
                    p.generation += 1;
                    changes.push(format!("{name}: new workers ({})", spec.model.name));
                }
                if p.target != spec.count {
                    changes.push(format!("{name}: {} -> {}", p.target, spec.count));
                }
                p.spec = Arc::new(spec.clone());
                p.target = spec.count;
                p.removed = false;
                p.paused = None;
                p.crashes.clear();
                p.reconcile(&fleet, name);
            }
            None => {
                let mut p = PoolState::new(spec.clone());
                p.reconcile(&fleet, name);
                st.pools.insert(name.clone(), p);
                changes.push(format!("{name}: added"));
            }
        }
    }
    let gone: Vec<String> = st
        .pools
        .keys()
        .filter(|n| !new.pools.contains_key(*n))
        .cloned()
        .collect();
    for name in gone {
        let p = st.pools.get_mut(&name).expect("pool");
        p.removed = true;
        p.target = 0;
        p.reconcile(&fleet, &name);
        if p.workers.is_empty() {
            st.pools.remove(&name);
        }
        changes.push(format!("{name}: removed"));
    }
    st.announced.clear();
    st.fleet = new;
    json!({"ok": true, "message": if changes.is_empty() { "no changes".to_string() } else { changes.join("; ") }})
}

fn down(st: &mut State, how: Shutdown) {
    let how = match (st.shutdown, how) {
        (Some(Shutdown::Now), _) | (_, Shutdown::Now) => Shutdown::Now,
        _ => Shutdown::Drain,
    };
    st.shutdown = Some(how);
    let fleet = st.fleet.name.clone();
    for (name, p) in st.pools.iter_mut() {
        p.target = 0;
        p.reconcile(&fleet, name);
        if how == Shutdown::Now {
            for w in &mut p.workers {
                if let Some(b) = &mut w.busy {
                    b.stopping = true;
                    if let Some(pid) = b.pid {
                        // The wrapper releases its task and exits 143.
                        unsafe {
                            libc::kill(pid as i32, libc::SIGTERM);
                        }
                    }
                }
            }
        }
    }
}

/// First SIGTERM or Ctrl-C drains; a second stops every child now (16.4).
async fn signals(shared: Arc<Shared>) {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
    loop {
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
        let mut st = shared.state.lock().await;
        let how = if st.shutdown.is_some() {
            eprintln!("cork workers: stopping every worker now");
            Shutdown::Now
        } else {
            eprintln!("cork workers: draining; signal again to stop now");
            Shutdown::Drain
        };
        down(&mut st, how);
        drop(st);
        shared.kick.notify_one();
        shared.done.notify_one();
    }
}

// ---------- the client side of the socket ----------

/// Send one request to a running fleet and return its reply.
pub async fn call(sock: &std::path::Path, req: &Value) -> Result<Value, String> {
    let stream = tokio::net::UnixStream::connect(sock).await.map_err(|e| {
        format!(
            "no fleet answers on {} ({e}); start one with `cork workers up`",
            sock.display()
        )
    })?;
    let (r, mut w) = stream.into_split();
    w.write_all(format!("{req}\n").as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let mut lines = BufReader::new(r).lines();
    let line = lines
        .next_line()
        .await
        .map_err(|e| e.to_string())?
        .ok_or("the fleet closed the connection")?;
    serde_json::from_str(&line).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::EXAMPLE;
    use crate::config::EXAMPLE_WORKERS;

    fn pool(name: &str) -> PoolState {
        let f = Fleet::parse(EXAMPLE_WORKERS, EXAMPLE).unwrap();
        let mut p = PoolState::new(f.pools[name].clone());
        p.reconcile("dev", name);
        p
    }

    fn busy(p: &mut PoolState, slot: usize) {
        let w = p.workers.iter_mut().find(|w| w.slot == slot).unwrap();
        w.busy = Some(Busy {
            task_id: format!("t{slot}"),
            pid: None,
            started: Instant::now(),
            stopping: false,
        });
    }

    #[test]
    fn workers_get_profiles_by_slot() {
        let p = pool("fast");
        let names: Vec<_> = p.workers.iter().map(|w| w.profile.clone()).collect();
        assert_eq!(
            names,
            vec![
                "dev-fast-1",
                "dev-fast-2",
                "dev-fast-3",
                "dev-fast-4",
                "dev-fast-5"
            ]
        );
    }

    #[test]
    fn scale_down_stops_idle_first_and_drains_busy() {
        let mut p = pool("fast");
        busy(&mut p, 1);
        busy(&mut p, 2);
        p.target = 3;
        p.reconcile("dev", "fast");
        // Two idle ones (5 and 4) stop at once.
        assert_eq!(p.workers.len(), 3);
        p.target = 1;
        p.reconcile("dev", "fast");
        // 3 is idle and goes; 2 is busy: drained, not killed.
        let left: Vec<(usize, bool)> = p.workers.iter().map(|w| (w.slot, w.draining)).collect();
        assert_eq!(left, vec![(1, false), (2, true)]);
        // Scaling up again reuses the lowest free slot.
        p.target = 2;
        p.reconcile("dev", "fast");
        let live: Vec<usize> = p.live().map(|w| w.slot).collect();
        assert_eq!(live, vec![1, 3]);
    }

    #[test]
    fn a_new_generation_drains_the_old_workers() {
        let mut p = pool("strong");
        busy(&mut p, 1);
        p.generation += 1;
        p.reconcile("dev", "strong");
        let w: Vec<(usize, bool, u64)> = p
            .workers
            .iter()
            .map(|w| (w.slot, w.draining, w.generation))
            .collect();
        assert_eq!(w, vec![(1, true, 0), (2, false, 1), (3, false, 1)]);
    }

    #[test]
    fn crash_loop_pauses_after_five() {
        let f = Fleet::parse(EXAMPLE_WORKERS, EXAMPLE).unwrap();
        let mut pools = BTreeMap::new();
        pools.insert("strong".to_string(), pool("strong"));
        let mut st = State {
            fleet: f,
            person: "chris".into(),
            pools,
            inflight: HashMap::new(),
            cooldown: HashMap::new(),
            unmatched: vec![],
            shutdown: None,
            daemon_wait_until: None,
            daemon_backoff: DAEMON_FIRST_BACKOFF,
            announced: BTreeSet::new(),
            started: Instant::now(),
            usage: BTreeMap::new(),
            crash_loops: BTreeMap::new(),
        };
        for n in 0..5 {
            assert!(st.pools["strong"].paused.is_none(), "paused after {n}");
            crashed(&mut st, "strong", 0, format!("boom {n}"));
        }
        assert_eq!(st.pools["strong"].paused.as_deref(), Some("boom 4"));
        assert!(
            st.pools["strong"]
                .free_worker(Instant::now() + Duration::from_secs(99))
                .is_none()
        );
        let v = scale(&mut st, "strong", "+1");
        assert_eq!(v["ok"], true, "{v}");
        assert!(st.pools["strong"].paused.is_none(), "scale resumes");
        assert_eq!(scale(&mut st, "strong", "9")["ok"], false, "over max 6");
        assert_eq!(scale(&mut st, "strong", "-9")["ok"], false, "under min");
        assert_eq!(scale(&mut st, "strong", "x")["ok"], false);
    }
}
