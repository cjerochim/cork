//! What the daemon knows about itself, for `GET /metrics` in Prometheus
//! text format (HANDOFF 18.4).
//!
//! Only what the daemon knows: tasks by state, event counters, write
//! latency and commit size, waiters and streams, the head, idempotency hits
//! and the WAL size. No model or cost data: the daemon knows nothing of
//! models.
//!
//! Everything is updated after a commit, from what the commit wrote, with
//! atomics or a short lock, so a scrape never waits on a write and a write
//! never waits on a scrape. Counters count since the daemon started.

use cork_core::TaskState;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Seconds.
const LATENCY_BOUNDS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
];
/// Events per commit.
const SIZE_BOUNDS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0];

pub struct Histogram {
    bounds: &'static [f64],
    /// One per bound, not cumulative; the last is +Inf.
    counts: Vec<AtomicU64>,
    /// The sum, in millionths, so it fits an atomic.
    sum_micro: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    fn new(bounds: &'static [f64]) -> Self {
        Self {
            bounds,
            counts: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            sum_micro: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, v: f64) {
        let i = self
            .bounds
            .iter()
            .position(|b| v <= *b)
            .unwrap_or(self.bounds.len());
        self.counts[i].fetch_add(1, Ordering::Relaxed);
        self.sum_micro
            .fetch_add((v * 1_000_000.0).round() as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        let mut acc = 0;
        for (i, b) in self.bounds.iter().enumerate() {
            acc += self.counts[i].load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{b}\"}} {acc}");
        }
        acc += self.counts[self.bounds.len()].load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {acc}");
        let _ = writeln!(
            out,
            "{name}_sum {}",
            self.sum_micro.load(Ordering::Relaxed) as f64 / 1_000_000.0
        );
        let _ = writeln!(out, "{name}_count {acc}");
    }
}

pub struct Metrics {
    /// (channel, stored state) to tasks.
    tasks: Mutex<BTreeMap<(String, &'static str), i64>>,
    /// Event type to events committed since start.
    events: Mutex<BTreeMap<String, u64>>,
    commits: AtomicU64,
    pub idempotency_hits: AtomicU64,
    pub write_seconds: Histogram,
    pub commit_events: Histogram,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            tasks: Mutex::default(),
            events: Mutex::default(),
            commits: AtomicU64::new(0),
            idempotency_hits: AtomicU64::new(0),
            write_seconds: Histogram::new(LATENCY_BOUNDS),
            commit_events: Histogram::new(SIZE_BOUNDS),
        }
    }
}

/// A task that changed state in a commit: (channel, before, after).
pub type Move = (String, Option<TaskState>, TaskState);

/// What the board adds at scrape time.
pub struct Live {
    pub head: u64,
    pub waiters: usize,
    pub streams: usize,
    pub wal_bytes: Option<u64>,
    pub degraded: bool,
}

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

impl Metrics {
    /// Count tasks loaded at start.
    pub fn seed(&self, channel: &str, state: TaskState) {
        *self
            .tasks
            .lock()
            .unwrap()
            .entry((channel.to_string(), state.as_str()))
            .or_default() += 1;
    }

    /// Record a commit that succeeded.
    pub fn committed<'a>(
        &self,
        types: impl Iterator<Item = &'a str>,
        moves: &[Move],
        seconds: f64,
    ) {
        let mut n = 0u64;
        {
            let mut ev = self.events.lock().unwrap();
            for t in types {
                n += 1;
                match ev.get_mut(t) {
                    Some(c) => *c += 1,
                    None => {
                        ev.insert(t.to_string(), 1);
                    }
                }
            }
        }
        self.write_seconds.observe(seconds);
        if n > 0 {
            self.commits.fetch_add(1, Ordering::Relaxed);
            self.commit_events.observe(n as f64);
        }
        if !moves.is_empty() {
            let mut t = self.tasks.lock().unwrap();
            for (channel, before, after) in moves {
                if let Some(b) = before {
                    *t.entry((channel.clone(), b.as_str())).or_default() -= 1;
                }
                *t.entry((channel.clone(), after.as_str())).or_default() += 1;
            }
            t.retain(|_, n| *n != 0);
        }
    }

    /// Events of this type committed since start.
    pub fn events_of(&self, t: &str) -> u64 {
        self.events.lock().unwrap().get(t).copied().unwrap_or(0)
    }

    /// The Prometheus text. With `channels` off, task gauges are summed
    /// over channels so channel names stay private (18.6).
    pub fn render(&self, live: &Live, channels: bool) -> String {
        let mut out = String::new();
        let tasks: BTreeMap<(String, &'static str), i64> = {
            let t = self.tasks.lock().unwrap();
            if channels {
                t.clone()
            } else {
                let mut m = BTreeMap::new();
                for ((_, s), n) in t.iter() {
                    *m.entry((String::new(), *s)).or_default() += n;
                }
                m
            }
        };
        let _ = writeln!(out, "# HELP cork_tasks Tasks by stored state.");
        let _ = writeln!(out, "# TYPE cork_tasks gauge");
        for ((c, s), n) in &tasks {
            if channels {
                let _ = writeln!(
                    out,
                    "cork_tasks{{channel=\"{}\",state=\"{}\"}} {n}",
                    esc(c),
                    s
                );
            } else {
                let _ = writeln!(out, "cork_tasks{{state=\"{s}\"}} {n}");
            }
        }

        let events = self.events.lock().unwrap().clone();
        let get = |t: &str| events.get(t).copied().unwrap_or(0);
        let _ = writeln!(
            out,
            "# HELP cork_events_total Events committed since the daemon started, by type."
        );
        let _ = writeln!(out, "# TYPE cork_events_total counter");
        for (t, n) in &events {
            let _ = writeln!(out, "cork_events_total{{type=\"{}\"}} {n}", esc(t));
        }
        let _ = writeln!(out, "# HELP cork_claims_total Claims since start.");
        let _ = writeln!(out, "# TYPE cork_claims_total counter");
        let _ = writeln!(out, "cork_claims_total {}", get("TaskClaimed"));
        let _ = writeln!(
            out,
            "# HELP cork_settles_total Settles since start, by what they did."
        );
        let _ = writeln!(out, "# TYPE cork_settles_total counter");
        for (op, t) in [
            ("complete", "TaskCompleted"),
            ("fail", "TaskFailed"),
            ("release", "TaskReleased"),
            ("block", "TaskBlocked"),
        ] {
            let _ = writeln!(out, "cork_settles_total{{op=\"{op}\"}} {}", get(t));
        }
        let _ = writeln!(
            out,
            "# HELP cork_lease_expiries_total Lease expiries since start."
        );
        let _ = writeln!(out, "# TYPE cork_lease_expiries_total counter");
        let _ = writeln!(out, "cork_lease_expiries_total {}", get("LeaseExpired"));
        let _ = writeln!(out, "# HELP cork_commits_total Commits that wrote events.");
        let _ = writeln!(out, "# TYPE cork_commits_total counter");
        let _ = writeln!(
            out,
            "cork_commits_total {}",
            self.commits.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP cork_idempotency_hits_total Writes answered from a stored idempotency key."
        );
        let _ = writeln!(out, "# TYPE cork_idempotency_hits_total counter");
        let _ = writeln!(
            out,
            "cork_idempotency_hits_total {}",
            self.idempotency_hits.load(Ordering::Relaxed)
        );
        self.write_seconds.render(
            &mut out,
            "cork_write_seconds",
            "Time in the store's write transaction, per write.",
        );
        self.commit_events
            .render(&mut out, "cork_commit_events", "Events per commit.");

        for (name, help, v) in [
            ("cork_log_head", "The last committed position.", live.head),
            (
                "cork_waiters",
                "Claims waiting for a task.",
                live.waiters as u64,
            ),
            ("cork_streams", "Open event streams.", live.streams as u64),
            (
                "cork_degraded",
                "1 while the store refuses writes.",
                live.degraded as u64,
            ),
        ] {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} gauge");
            let _ = writeln!(out, "{name} {v}");
        }
        if let Some(w) = live.wal_bytes {
            let _ = writeln!(out, "# HELP cork_wal_bytes Size of the write-ahead log.");
            let _ = writeln!(out, "# TYPE cork_wal_bytes gauge");
            let _ = writeln!(out, "cork_wal_bytes {w}");
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_moves_and_renders() {
        let m = Metrics::default();
        m.seed("work", TaskState::Ready);
        m.committed(
            ["TaskClaimed", "MessagePosted"].into_iter(),
            &[("work".into(), Some(TaskState::Ready), TaskState::Claimed)],
            0.002,
        );
        m.committed(
            ["TaskCreated"].into_iter(),
            &[("a\"b".into(), None, TaskState::Ready)],
            0.0001,
        );
        m.committed(std::iter::empty(), &[], 0.0001);
        let live = Live {
            head: 9,
            waiters: 1,
            streams: 2,
            wal_bytes: Some(4096),
            degraded: false,
        };
        let t = m.render(&live, true);
        assert!(
            t.contains("cork_tasks{channel=\"work\",state=\"claimed\"} 1"),
            "{t}"
        );
        assert!(!t.contains("state=\"ready\"} 0"), "{t}");
        assert!(
            t.contains("cork_tasks{channel=\"a\\\"b\",state=\"ready\"} 1"),
            "{t}"
        );
        assert!(t.contains("cork_claims_total 1"));
        assert!(t.contains("cork_events_total{type=\"MessagePosted\"} 1"));
        assert!(t.contains("cork_commits_total 2"));
        assert!(
            t.contains("cork_write_seconds_bucket{le=\"0.0025\"} 3"),
            "{t}"
        );
        assert!(t.contains("cork_write_seconds_count 3"));
        assert!(t.contains("cork_commit_events_bucket{le=\"2\"} 2"), "{t}");
        assert!(t.contains("cork_log_head 9"));
        assert!(t.contains("cork_wal_bytes 4096"));
        let private = m.render(&live, false);
        assert!(!private.contains("channel="));
        assert!(private.contains("cork_tasks{state=\"ready\"} 1"));
    }
}
