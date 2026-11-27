//! The five stress scenarios (HANDOFF 11, item 13).

use crate::client::{CallError, Client};
use crate::daemon::{Daemon, Sync, process_usage};
use crate::invariants;
use crate::stats::{Samples, ms};
use cork_core::TaskState;
use cork_store::{Store, TaskQuery};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Scenario {
    Throughput,
    Contention,
    Chaos,
    Waiters,
    Soak,
    /// A fake lead and fake workers through 100 features (HANDOFF 17.5).
    Lead,
    /// Random dependencies and replacements under failing workers (6.8.9).
    Deps,
}

/// Sizes for a run. `scale` shrinks counts and durations for quick runs.
#[derive(Debug, Clone)]
pub struct Params {
    pub sync: Sync,
    pub scale: f64,
    pub soak_secs: u64,
    /// Fail the run when a latency target is missed (for release builds).
    pub strict: bool,
}

impl Params {
    pub(crate) fn n(&self, full: usize) -> usize {
        ((full as f64 * self.scale).round() as usize).max(1)
    }
}

#[derive(Debug, Default)]
pub struct Report {
    pub name: String,
    pub lines: Vec<String>,
    /// Invariant violations and missed targets. Empty means the run passed.
    pub failures: Vec<String>,
}

impl Report {
    pub(crate) fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }
    pub(crate) fn line(&mut self, s: impl Into<String>) {
        self.lines.push(s.into());
    }
    fn target(&mut self, strict: bool, ok: bool, what: String) {
        self.line(format!(
            "{} {what}",
            if ok { "target met:" } else { "TARGET MISSED:" }
        ));
        if !ok && strict {
            self.failures.push(what);
        }
    }
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }
    pub fn print(&self) {
        println!("== {} ==", self.name);
        for l in &self.lines {
            println!("  {l}");
        }
        for f in &self.failures {
            println!("  FAIL: {f}");
        }
        println!("  {}", if self.passed() { "PASS" } else { "FAIL" });
    }
    pub(crate) async fn check(&mut self, d: &Daemon) {
        let c = invariants::check(d).await;
        self.line(format!(
            "invariants: {} events, {} tasks, {} violations",
            c.events,
            c.tasks,
            c.violations.len()
        ));
        self.failures.extend(c.violations);
    }
}

pub async fn run(s: Scenario, p: &Params) -> Report {
    match s {
        Scenario::Throughput => throughput(p).await,
        Scenario::Contention => contention(p).await,
        Scenario::Chaos => chaos(p).await,
        Scenario::Waiters => waiters(p).await,
        Scenario::Soak => soak(p).await,
        Scenario::Lead => crate::lead::lead(p).await,
        Scenario::Deps => crate::deps::deps(p).await,
    }
}

async fn count(d: &Daemon, state: TaskState) -> usize {
    d.board
        .store()
        .list_tasks(&TaskQuery {
            state: Some(state),
            limit: 10_000,
            now: d.board.now(),
            ..Default::default()
        })
        .await
        .map(|v| v.len())
        .unwrap_or(0)
}

/// Create `n` tasks with `par` creators.
async fn create_many(
    c: &Client,
    channel: &str,
    n: usize,
    lease_ms: i64,
    attempts: u32,
    par: usize,
) -> Samples {
    let next = Arc::new(AtomicUsize::new(0));
    let mut hs = vec![];
    for _ in 0..par {
        let c = c.clone();
        let next = next.clone();
        let channel = channel.to_string();
        hs.push(tokio::spawn(async move {
            let mut lat = Samples::default();
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= n {
                    return lat;
                }
                let t = Instant::now();
                c.create(&channel, &format!("task {i}"), lease_ms, attempts)
                    .await
                    .expect("create");
                lat.push(t.elapsed());
            }
        }));
    }
    let mut all = Samples::default();
    for h in hs {
        all.extend(h.await.unwrap());
    }
    all
}

/// 50 workers, 10,000 tasks with instant work. Claims per second and drain time.
pub async fn throughput(p: &Params) -> Report {
    let mut r = Report::new("throughput");
    let d = Daemon::start(p.sync).await;
    let (workers, tasks) = (p.n(50), p.n(10_000));
    let lead = d.client("lead");
    let created = create_many(&lead, "tp", tasks, 60_000, 3, 16).await;
    r.line(format!("create: {}", created.summary()));
    let start = Instant::now();
    let mut hs = vec![];
    for w in 0..workers {
        let c = d.client(&format!("w{w}"));
        hs.push(tokio::spawn(async move {
            let (mut claim_lat, mut done_lat) = (Samples::default(), Samples::default());
            loop {
                let t = Instant::now();
                let Some(cl) = c.claim("tp", 0).await.expect("claim") else {
                    return (claim_lat, done_lat);
                };
                claim_lat.push(t.elapsed());
                let t = Instant::now();
                c.settle(&cl.task_id, "complete", &cl.claim_id)
                    .await
                    .expect("complete");
                done_lat.push(t.elapsed());
            }
        }));
    }
    let (mut claims, mut completes) = (Samples::default(), Samples::default());
    for h in hs {
        let (a, b) = h.await.unwrap();
        claims.extend(a);
        completes.extend(b);
    }
    let drain = start.elapsed();
    r.line(format!(
        "{workers} workers, {tasks} tasks, sync {:?}",
        p.sync
    ));
    r.line(format!(
        "drained in {:.2}s: {:.0} claims/s",
        drain.as_secs_f64(),
        claims.len() as f64 / drain.as_secs_f64()
    ));
    r.line(format!("claim: {}", claims.summary()));
    r.line(format!("complete: {}", completes.summary()));
    let done = count(&d, TaskState::Completed).await;
    if done != tasks {
        r.failures
            .push(format!("{done} of {tasks} tasks completed"));
    }
    r.check(&d).await;
    r
}

/// 200 workers on 1 channel with `--wait`, tasks created at a steady rate.
/// The spread of wait times shows fairness.
pub async fn contention(p: &Params) -> Report {
    let mut r = Report::new("contention");
    let d = Daemon::start(p.sync).await;
    let workers = p.n(200);
    let secs = (20.0 * p.scale).max(2.0);
    let rate = 100.0; // tasks per second
    let tasks = (secs * rate) as usize;
    let finished = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicUsize::new(0));
    let mut hs = vec![];
    for w in 0..workers {
        let c = d.client(&format!("w{w}"));
        let finished = finished.clone();
        let done = done.clone();
        hs.push(tokio::spawn(async move {
            let mut waits = Samples::default();
            while !finished.load(Ordering::Relaxed) {
                let t = Instant::now();
                match c.claim("hot", 2_000).await.expect("claim") {
                    Some(cl) => {
                        waits.push(t.elapsed());
                        c.settle(&cl.task_id, "complete", &cl.claim_id)
                            .await
                            .expect("complete");
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                    None => continue,
                }
            }
            waits
        }));
    }
    // Let every worker join the waiting queue before tasks arrive.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let lead = d.client("lead");
    let start = Instant::now();
    for i in 0..tasks {
        let due = start + Duration::from_secs_f64(i as f64 / rate);
        tokio::time::sleep_until(due.into()).await;
        lead.create("hot", &format!("t{i}"), 60_000, 3)
            .await
            .expect("create");
    }
    while done.load(Ordering::Relaxed) < tasks {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    finished.store(true, Ordering::Relaxed);
    let mut waits = Samples::default();
    let mut served = vec![];
    for h in hs {
        let s = h.await.unwrap();
        served.push(s.len());
        waits.extend(s);
    }
    served.sort();
    r.line(format!(
        "{workers} waiting workers, {tasks} tasks at {rate}/s, sync {:?}",
        p.sync
    ));
    r.line(format!("wait for a task: {}", waits.summary()));
    r.line(format!(
        "tasks per worker: min {} median {} max {}",
        served[0],
        served[served.len() / 2],
        served[served.len() - 1]
    ));
    r.check(&d).await;
    r
}

/// 50 workers, 2,000 tasks, 1 s leases. 20% of workers vanish after
/// claiming. Every task still ends completed or failed, and no stale claim
/// ever settles anything.
pub async fn chaos(p: &Params) -> Report {
    let mut r = Report::new("chaos");
    let d = Daemon::start(p.sync).await;
    // At least 5 workers, so some survive the ones that vanish.
    let (workers, tasks) = (p.n(50).max(5), p.n(2_000));
    let lead = d.client("lead");
    create_many(&lead, "chaos", tasks, 1_000, 3, 16).await;
    let stop = Arc::new(AtomicBool::new(false));
    let vanished = Arc::new(std::sync::Mutex::new(Vec::<(String, String)>::new()));
    let mut hs = vec![];
    for w in 0..workers {
        let c = d.client(&format!("w{w}"));
        let stop = stop.clone();
        let vanished = vanished.clone();
        // One worker in five vanishes after its (w % 7 + 1)th claim.
        let vanish_after = (w % 5 == 4).then_some(w % 7 + 1);
        hs.push(tokio::spawn(async move {
            let mut claims = 0;
            while !stop.load(Ordering::Relaxed) {
                let Some(cl) = c.claim("chaos", 500).await.expect("claim") else {
                    continue;
                };
                claims += 1;
                if vanish_after == Some(claims) {
                    vanished.lock().unwrap().push((cl.task_id, cl.claim_id));
                    return;
                }
                c.settle(&cl.task_id, "complete", &cl.claim_id)
                    .await
                    .expect("complete");
            }
        }));
    }
    let start = Instant::now();
    loop {
        let finished = count(&d, TaskState::Completed).await + count(&d, TaskState::Failed).await;
        if finished == tasks {
            break;
        }
        if start.elapsed() > Duration::from_secs(600) {
            r.failures.push(format!(
                "only {finished} of {tasks} tasks finished in 10 minutes"
            ));
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let drain = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        h.await.unwrap();
    }
    // The vanished workers come back and try to finish with their old claims.
    let zombies = vanished.lock().unwrap().clone();
    let mut refused = 0;
    for (task, claim) in &zombies {
        match d.client("zombie").settle(task, "complete", claim).await {
            Err(CallError { code, .. }) if code == "claim_lost" => refused += 1,
            other => r
                .failures
                .push(format!("stale claim on {task} was not refused: {other:?}")),
        }
    }
    r.line(format!(
        "{workers} workers ({} vanished), {tasks} tasks, 1 s leases, sync {:?}",
        zombies.len(),
        p.sync
    ));
    r.line(format!(
        "all finished in {:.2}s: {} completed, {} failed",
        drain.as_secs_f64(),
        count(&d, TaskState::Completed).await,
        count(&d, TaskState::Failed).await
    ));
    r.line(format!(
        "{refused} of {} stale claims refused with claim_lost",
        zombies.len()
    ));
    r.check(&d).await;
    r
}

/// 1,000 open `wait` calls while 50 workers run. Wake-up p99 stays under
/// 100 ms, and store reads stay flat as waiters grow.
pub async fn waiters(p: &Params) -> Report {
    let mut r = Report::new("waiters");
    let d = Daemon::start(p.sync).await;
    let base = Instant::now();
    let (wake_small, reads_small) = wake_round(&d, p.n(10), p.n(50), 10, base).await;
    let (wake_big, reads_big) = wake_round(&d, p.n(1_000), p.n(50), 10, base).await;
    r.line(format!(
        "{} waiters: wake {} , store reads {reads_small}",
        p.n(10),
        wake_small.summary()
    ));
    r.line(format!(
        "{} waiters: wake {} , store reads {reads_big}",
        p.n(1_000),
        wake_big.summary()
    ));
    let p99 = wake_big.pct(0.99);
    r.target(
        p.strict,
        p99 < Duration::from_millis(100),
        format!("wake p99 {:.1}ms < 100ms", ms(p99)),
    );
    // Flat: reads must not grow with the number of waiters.
    let flat = reads_big <= reads_small.max(1) * 3;
    r.target(
        p.strict,
        flat,
        format!("store reads {reads_small} -> {reads_big} as waiters grew"),
    );
    r.check(&d).await;
    r
}

/// One round: `n` waiters each wait for `posts` messages while workers churn.
/// Returns wake latencies and store reads made by waits during the round.
async fn wake_round(
    d: &Daemon,
    n: usize,
    workers: usize,
    posts: usize,
    base: Instant,
) -> (Samples, u64) {
    let stop = Arc::new(AtomicBool::new(false));
    // Background churn: a creator and workers on another channel.
    let mut churn = vec![];
    {
        let c = d.client("churn-lead");
        let stop = stop.clone();
        churn.push(tokio::spawn(async move {
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                let _ = c.create("busy", &format!("c{i}"), 60_000, 3).await;
                i += 1;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }));
    }
    for w in 0..workers {
        let c = d.client(&format!("cw{w}"));
        let stop = stop.clone();
        churn.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(Some(cl)) = c.claim("busy", 200).await {
                    let _ = c.settle(&cl.task_id, "complete", &cl.claim_id).await;
                }
            }
        }));
    }
    let start_head = d.board.last_position();
    let ready = Arc::new(AtomicUsize::new(0));
    let mut hs = vec![];
    for i in 0..n {
        let c = d.client(&format!("waiter{i}"));
        let ready = ready.clone();
        hs.push(tokio::spawn(async move {
            let mut lat = Samples::default();
            let mut after = start_head;
            let mut first = true;
            while lat.len() < posts {
                let q = format!("/events?after={after}&channel=notify&label=k=v&wait_ms=20000");
                if first {
                    ready.fetch_add(1, Ordering::Relaxed);
                    first = false;
                }
                let Ok(page) = c.get(&q).await else { continue };
                let got = Instant::now();
                after = page["next_after"].as_u64().unwrap_or(after);
                for e in page["events"].as_array().cloned().unwrap_or_default() {
                    let sent_us = e["data"]["data"]["sent_us"].as_u64().unwrap_or(0);
                    let sent = base + Duration::from_micros(sent_us);
                    lat.push(got.saturating_duration_since(sent));
                }
            }
            lat
        }));
    }
    while ready.load(Ordering::Relaxed) < n {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let reads_before = d.board.store_reads();
    let poster = d.client("poster");
    for i in 0..posts {
        let sent_us = base.elapsed().as_micros() as u64;
        poster
            .post(
                "/channels/notify/messages",
                serde_json::json!({ "text": format!("n{i}"), "labels": { "k": "v" }, "data": { "sent_us": sent_us } }),
            )
            .await
            .expect("post");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let mut all = Samples::default();
    for h in hs {
        all.extend(h.await.unwrap());
    }
    let reads = d.board.store_reads() - reads_before;
    stop.store(true, Ordering::Relaxed);
    for h in churn {
        let _ = h.await;
    }
    (all, reads)
}

/// Mixed load for a while. Memory, open files and the WAL stay bounded, and
/// the database restarts cleanly.
pub async fn soak(p: &Params) -> Report {
    let mut r = Report::new("soak");
    let d = Daemon::start(p.sync).await;
    let secs = p.soak_secs;
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let mut hs = vec![];
    for k in 0..4 {
        let c = d.client(&format!("lead{k}"));
        let (stop, ops) = (stop.clone(), ops.clone());
        hs.push(tokio::spawn(async move {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let _ = c
                    .create(&format!("soak/{k}"), &format!("t{i}"), 2_000, 3)
                    .await;
                let _ = c
                    .post_message(&format!("soak/{k}"), &format!("note {i}"), ("kind", "note"))
                    .await;
                ops.fetch_add(2, Ordering::Relaxed);
                i += 1;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
    }
    for w in 0..20 {
        let c = d.client(&format!("sw{w}"));
        let (stop, ops) = (stop.clone(), ops.clone());
        hs.push(tokio::spawn(async move {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if let Ok(Some(cl)) = c.claim("soak/", 500).await {
                    n += 1;
                    // Mix: most complete, some release, a few are abandoned.
                    let op = match n % 20 {
                        0 => None,
                        1 | 2 => Some("release"),
                        3 => Some("fail"),
                        _ => Some("complete"),
                    };
                    if let Some(op) = op {
                        let _ = c.settle(&cl.task_id, op, &cl.claim_id).await;
                    }
                    ops.fetch_add(2, Ordering::Relaxed);
                }
            }
        }));
    }
    for w in 0..20 {
        let c = d.client(&format!("watch{w}"));
        let stop = stop.clone();
        hs.push(tokio::spawn(async move {
            let mut after = 0;
            while !stop.load(Ordering::Relaxed) {
                if let Ok(page) = c
                    .get(&format!(
                        "/events?after={after}&label=kind=note&wait_ms=2000"
                    ))
                    .await
                {
                    after = page["next_after"].as_u64().unwrap_or(after);
                }
            }
        }));
    }
    let start = Instant::now();
    let mut samples = vec![];
    while start.elapsed() < Duration::from_secs(secs) {
        tokio::time::sleep(Duration::from_secs((secs / 20).clamp(1, 30))).await;
        let (rss, fds) = process_usage();
        samples.push((start.elapsed().as_secs(), rss, fds, d.wal_bytes()));
    }
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        let _ = h.await;
    }
    r.line(format!(
        "{}s mixed load, {} operations, sync {:?}",
        secs,
        ops.load(Ordering::Relaxed),
        p.sync
    ));
    for (t, rss, fds, wal) in &samples {
        r.line(format!(
            "t={t}s rss={}MB fds={fds} wal={}KB",
            rss / 1024,
            wal / 1024
        ));
    }
    if samples.len() >= 4 {
        let half = samples.len() / 2;
        let max_rss_first = samples[..half].iter().map(|s| s.1).max().unwrap_or(0);
        let max_rss_last = samples[half..].iter().map(|s| s.1).max().unwrap_or(0);
        let max_fds_first = samples[..half].iter().map(|s| s.2).max().unwrap_or(0);
        let max_fds_last = samples[half..].iter().map(|s| s.2).max().unwrap_or(0);
        let max_wal = samples.iter().map(|s| s.3).max().unwrap_or(0);
        r.target(
            true,
            max_fds_last <= max_fds_first + 50,
            format!("open files bounded ({max_fds_first} -> {max_fds_last})"),
        );
        r.target(
            p.strict,
            max_rss_last <= max_rss_first * 2 + 50 * 1024,
            format!(
                "memory bounded ({}MB -> {}MB)",
                max_rss_first / 1024,
                max_rss_last / 1024
            ),
        );
        r.target(
            true,
            max_wal <= 512 * 1024 * 1024,
            format!("WAL bounded (max {}MB)", max_wal / 1024 / 1024),
        );
    }
    r.check(&d).await;
    // Restart on the same file and check again.
    let d = d.restart(p.sync).await;
    r.line(format!("restarted at head {}", d.board.last_position()));
    r.check(&d).await;
    r
}
