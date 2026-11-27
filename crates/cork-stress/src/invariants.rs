//! Invariants checked from the log after every run (HANDOFF 11, item 13):
//!
//! - Positions have no gaps, and versions in each stream are 1, 2, 3, ….
//! - At most one terminal event per task per open period.
//! - A claim ends (lease end, release, block or settle) before the next claim.
//! - No `TaskCompleted` or `TaskFailed` from a claim that was not live at its
//!   `at` time, and no `LeaseExpired` before the lease really ended.
//! - Projections equal a fresh fold of the log.
//! - `/metrics` agrees with the log: event counters since the daemon
//!   started, and tasks by channel and state (HANDOFF 18.7, item 5).

use crate::daemon::Daemon;
use cork_core::{Task, TaskEvent};
use cork_store::Store;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct Check {
    pub events: usize,
    pub tasks: usize,
    pub violations: Vec<String>,
}

impl Check {
    pub fn ok(&self) -> bool {
        self.violations.is_empty()
    }
    fn bad(&mut self, s: String) {
        if self.violations.len() < 50 {
            self.violations.push(s);
        }
    }
}

pub async fn check(d: &Daemon) -> Check {
    let events = d
        .client("checker")
        .all_events()
        .await
        .expect("read the log");
    let mut c = Check {
        events: events.len(),
        ..Default::default()
    };

    let mut streams: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for (i, e) in events.iter().enumerate() {
        let pos = e["position"].as_u64().unwrap_or(0);
        if pos != i as u64 + 1 {
            c.bad(format!(
                "position {pos} at index {i}: positions must have no gaps"
            ));
        }
        streams
            .entry(e["stream"].as_str().unwrap_or("").to_string())
            .or_default()
            .push(e);
    }
    let mut posted = 0u64;
    for (stream, evs) in &streams {
        for (i, e) in evs.iter().enumerate() {
            if e["version"].as_u64() != Some(i as u64 + 1) {
                c.bad(format!("{stream}: version {} at index {i}", e["version"]));
            }
            if e["type"] == "MessagePosted" {
                posted += 1;
            }
        }
        if let Some(id) = stream.strip_prefix("task:") {
            c.tasks += 1;
            check_task(&mut c, id, evs);
            check_projection(&mut c, d, id, evs).await;
        }
    }
    check_metrics(&mut c, d, &events).await;
    let channels = d.board.store().list_channels().await.expect("channels");
    let projected: u64 = channels.iter().map(|ch| ch.messages).sum();
    if projected != posted {
        c.bad(format!(
            "{projected} messages projected, {posted} MessagePosted in the log"
        ));
    }
    c
}

fn check_task(c: &mut Check, id: &str, evs: &[&Value]) {
    let mut open = true;
    let mut claim: Option<(String, i64)> = None;
    for e in evs {
        let ty = e["type"].as_str().unwrap_or("");
        let at = e["at"].as_i64().unwrap_or(0);
        let d = &e["data"];
        let cid = d["claim_id"].as_str().map(str::to_string);
        match ty {
            "TaskClaimed" => {
                if let Some((old, _)) = &claim {
                    c.bad(format!(
                        "task {id}: claimed again while claim {old} had not ended"
                    ));
                }
                claim = Some((
                    cid.unwrap_or_default(),
                    d["expires_at"].as_i64().unwrap_or(0),
                ));
            }
            "LeaseRenewed" => match &mut claim {
                Some((held, exp)) if Some(held.as_str()) == cid.as_deref() => {
                    *exp = d["expires_at"].as_i64().unwrap_or(*exp);
                }
                _ => c.bad(format!("task {id}: renew of a claim that is not held")),
            },
            "LeaseExpired" => {
                match &claim {
                    Some((held, exp)) if Some(held.as_str()) == cid.as_deref() => {
                        if at <= *exp {
                            c.bad(format!(
                                "task {id}: lease recorded as ended at {at}, live until {exp}"
                            ));
                        }
                    }
                    _ => c.bad(format!("task {id}: lease end of a claim that is not held")),
                }
                claim = None;
            }
            "TaskReleased" | "TaskBlocked" => {
                live(c, id, ty, &claim, cid.as_deref(), at);
                claim = None;
            }
            "TaskCompleted" | "TaskFailed" | "TaskCanceled" => {
                if !open {
                    c.bad(format!(
                        "task {id}: a second terminal event ({ty}) in one open period"
                    ));
                }
                open = false;
                if ty != "TaskCanceled" && cid.is_some() {
                    live(c, id, ty, &claim, cid.as_deref(), at);
                } else if ty == "TaskFailed" && claim.is_some() {
                    c.bad(format!(
                        "task {id}: TaskFailed without a claim while a claim was held"
                    ));
                }
                claim = None;
            }
            "TaskReopened" => {
                open = true;
            }
            _ => {}
        }
    }
}

fn live(
    c: &mut Check,
    id: &str,
    ty: &str,
    claim: &Option<(String, i64)>,
    cid: Option<&str>,
    at: i64,
) {
    match claim {
        Some((held, exp)) if Some(held.as_str()) == cid && at <= *exp => {}
        Some((held, exp)) => c.bad(format!(
            "task {id}: {ty} by claim {cid:?} at {at}, but the live claim was {held} until {exp}"
        )),
        None => c.bad(format!(
            "task {id}: {ty} by claim {cid:?} with no live claim"
        )),
    }
}

async fn check_projection(c: &mut Check, d: &Daemon, id: &str, evs: &[&Value]) {
    let parsed: Result<Vec<(TaskEvent, i64)>, _> = evs
        .iter()
        .map(|e| {
            serde_json::from_value::<TaskEvent>(e["data"].clone())
                .map(|t| (t, e["at"].as_i64().unwrap_or(0)))
        })
        .collect();
    let folded = match parsed.map(|p| Task::from_events(p.iter().map(|(e, at)| (e, *at)))) {
        Ok(Ok(t)) => t,
        other => {
            c.bad(format!("task {id}: log does not fold: {other:?}"));
            return;
        }
    };
    match d.board.store().get_task(id).await {
        Ok(Some(st)) if st.task == folded => {}
        // The board kept writing (a lease ended) after the log was read.
        // Fold the stream again, read after the projection, and compare.
        Ok(Some(st)) if st.task.version > folded.version => match refold(d, id).await {
            Some(again) if again == st.task => {}
            _ => c.bad(format!(
                "task {id}: projection differs from a fresh fold (state {:?}, version {}, after a re-read)",
                st.task.state, st.task.version
            )),
        },
        Ok(Some(st)) => c.bad(format!(
            "task {id}: projection differs from a fresh fold (state {:?} vs {:?}, version {} vs {})",
            st.task.state, folded.state, st.task.version, folded.version
        )),
        other => c.bad(format!("task {id}: no projection: {other:?}")),
    }
}

/// The task folded from its stream as it is now.
async fn refold(d: &Daemon, id: &str) -> Option<Task> {
    let f = cork_store::EventFilter {
        stream_id: Some(format!("task:{id}")),
        limit: 10_000,
        ..Default::default()
    };
    let page = d.board.store().read_events(&f).await.ok()?;
    let mut evs = vec![];
    for e in &page.events {
        evs.push((
            serde_json::from_value::<TaskEvent>(e.data.clone()).ok()?,
            e.at,
        ));
    }
    Task::from_events(evs.iter().map(|(e, at)| (e, *at))).ok()
}

/// `/metrics` against the log. The lease timer may write while we look, so
/// a mismatch is read again a few times before it counts.
async fn check_metrics(c: &mut Check, d: &Daemon, first: &[Value]) {
    let mut events: Vec<Value> = first.to_vec();
    let mut last = String::new();
    for round in 0..5 {
        if round > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
        let Some(m) = d.metrics().await else {
            last = "could not scrape /metrics".into();
            continue;
        };
        let head = m.get("cork_log_head").copied().unwrap_or(0.0) as u64;
        if head > events.len() as u64 {
            match d.client("checker").all_events().await {
                Ok(e) => events = e,
                Err(_) => continue,
            }
        }
        let start = d.board.started_head();
        let mut want: BTreeMap<String, f64> = BTreeMap::new();
        for e in &events {
            let pos = e["position"].as_u64().unwrap_or(0);
            if pos > start && pos <= head {
                *want
                    .entry(format!(
                        "cork_events_total{{type=\"{}\"}}",
                        e["type"].as_str().unwrap_or("")
                    ))
                    .or_default() += 1.0;
            }
        }
        let mut tasks: BTreeMap<String, f64> = BTreeMap::new();
        let mut after = 0;
        loop {
            let page = d
                .board
                .store()
                .list_tasks(&cork_store::TaskQuery {
                    after,
                    limit: 1_000,
                    ..Default::default()
                })
                .await
                .unwrap_or_default();
            let n = page.len();
            for t in page {
                after = after.max(t.created_position);
                *tasks
                    .entry(format!(
                        "cork_tasks{{channel=\"{}\",state=\"{}\"}}",
                        t.task.channel,
                        t.task.state.as_str()
                    ))
                    .or_default() += 1.0;
            }
            if n < 1_000 {
                break;
            }
        }
        want.extend(tasks);
        let got: BTreeMap<String, f64> = m
            .into_iter()
            .filter(|(k, _)| k.starts_with("cork_events_total{") || k.starts_with("cork_tasks{"))
            .collect();
        if got == want {
            return;
        }
        let diff: Vec<String> = want
            .keys()
            .chain(got.keys())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .filter(|k| want.get(*k) != got.get(*k))
            .take(5)
            .map(|k| format!("{k}: log {:?}, /metrics {:?}", want.get(k), got.get(k)))
            .collect();
        last = diff.join("; ");
    }
    c.bad(format!("/metrics disagrees with the log: {last}"));
}
