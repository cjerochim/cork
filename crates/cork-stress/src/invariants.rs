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
//! - Dependencies and replacement: invariants 1 to 7 of HANDOFF 6.8.9, in
//!   [`check_dependencies`] (8 is the fold check above).

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
    check_dependencies(&mut c, &events);
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
    let mut replaced = false;
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
                if replaced {
                    c.bad(format!("task {id}: reopened after it was replaced"));
                }
                open = true;
            }
            "TaskReplaced" => {
                if replaced {
                    c.bad(format!("task {id}: replaced twice"));
                }
                replaced = true;
                open = false;
                claim = None;
            }
            _ => {}
        }
        if replaced && matches!(ty, "TaskClaimed" | "TaskCompleted") {
            c.bad(format!("task {id}: {ty} after it was replaced"));
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

// ---------- dependencies (HANDOFF 6.8.9) ----------

/// What the dependency checks track per task while replaying the log.
#[derive(Default, Clone)]
struct DepTask {
    created: u64,
    state: String,
    depends_on: Vec<String>,
    replaces: Option<String>,
    replaced_by: Option<String>,
}

/// Events the board writes after a decision in the same commit. A commit
/// starts at any other event, so the log is "between commits" just before it.
fn follow_up(ty: &str) -> bool {
    matches!(
        ty,
        "DependenciesMet" | "DependencyFailed" | "TaskClaimed" | "TaskReplaced"
    )
}

fn head<'a>(tasks: &'a BTreeMap<String, DepTask>, id: &str) -> Option<(&'a str, &'a DepTask)> {
    let mut cur = id;
    for _ in 0..=cork_core::deps::MAX_REPLACEMENTS {
        let (k, t) = tasks.get_key_value(cur)?;
        match &t.replaced_by {
            Some(n) => cur = n,
            None => return Some((k.as_str(), t)),
        }
    }
    None
}

fn all_met(tasks: &BTreeMap<String, DepTask>, t: &DepTask) -> bool {
    t.depends_on
        .iter()
        .all(|d| head(tasks, d).is_some_and(|(_, h)| h.state == "completed"))
}

fn strs(v: &Value) -> Vec<String> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|x| x.as_str().map(str::to_string))
        .collect()
}

/// Invariants 1 to 7 of 6.8.9, by replaying the whole log in order.
pub fn check_dependencies(c: &mut Check, events: &[Value]) {
    let mut tasks: BTreeMap<String, DepTask> = BTreeMap::new();
    // (dependent, head) -> DependencyFailed owed minus written.
    let mut owed: BTreeMap<(String, String), i64> = BTreeMap::new();
    let mut last_completed: BTreeMap<String, u64> = BTreeMap::new();
    let mut by_pos: BTreeMap<u64, &Value> = BTreeMap::new();
    for e in events {
        by_pos.insert(e["position"].as_u64().unwrap_or(0), e);
    }
    for e in events {
        let ty = e["type"].as_str().unwrap_or("");
        let pos = e["position"].as_u64().unwrap_or(0);
        let Some(id) = e["stream"].as_str().and_then(|s| s.strip_prefix("task:")) else {
            continue;
        };
        let id = id.to_string();
        let d = &e["data"];
        let after = e["labels"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|l| l[0] == "board.task")
            .and_then(|l| l[1].as_str())
            .unwrap_or("")
            .to_string();

        // 3. Between commits, nothing waits on heads that all completed.
        if !follow_up(ty) {
            for (tid, t) in &tasks {
                if t.state == "waiting" && all_met(&tasks, t) {
                    c.bad(format!(
                        "task {tid}: waiting at position {pos} though every dependency completed"
                    ));
                }
            }
        }

        let before = tasks.get(&id).map(|t| t.state.clone());
        match ty {
            "TaskCreated" | "TaskReopened" => {
                let (depends_on, replaces) = if ty == "TaskCreated" {
                    (
                        strs(&d["depends_on"]),
                        d["replaces"].as_str().map(str::to_string),
                    )
                } else {
                    let t = tasks.get(&id).cloned().unwrap_or_default();
                    (t.depends_on, t.replaces)
                };
                let open = strs(&d["open_dependencies"]);
                // 1. open_dependencies is exactly the ids whose head had not completed.
                let want: Vec<String> = depends_on
                    .iter()
                    .filter(|x| !head(&tasks, x).is_some_and(|(_, h)| h.state == "completed"))
                    .cloned()
                    .collect();
                if open != want {
                    c.bad(format!(
                        "task {id}: {ty} at {pos} has open_dependencies {open:?}, the log says {want:?}"
                    ));
                }
                if (after == "waiting") != !open.is_empty() {
                    c.bad(format!("task {id}: {ty} is {after} with open {open:?}"));
                }
                if ty == "TaskCreated" {
                    // 7. Every declared id is older.
                    for x in &depends_on {
                        if !tasks.contains_key(x) {
                            c.bad(format!("task {id}: depends on {x}, which is not older"));
                        }
                    }
                    // 5. A retry inherits the old task's edges exactly.
                    if let Some(old) = &replaces {
                        match tasks.get(old) {
                            Some(o) if o.depends_on == depends_on => {}
                            _ => c.bad(format!(
                                "task {id}: replaces {old} but does not inherit its depends_on"
                            )),
                        }
                        let next = by_pos.get(&(pos + 1)).copied();
                        let ok = (pos + 1..pos + 4).any(|p| {
                            by_pos.get(&p).is_some_and(|n| {
                                n["type"] == "TaskReplaced"
                                    && n["stream"] == format!("task:{old}")
                                    && n["data"]["by"] == id.as_str()
                            })
                        });
                        if !ok {
                            c.bad(format!(
                                "task {id}: replaces {old}, but no TaskReplaced follows in its commit ({next:?})"
                            ));
                        }
                    }
                }
                let t = tasks.entry(id.clone()).or_default();
                if ty == "TaskCreated" {
                    t.created = pos;
                    t.depends_on = depends_on;
                    t.replaces = replaces;
                }
            }
            "DependenciesMet" => {
                // 2. Met by a completion just before it, with every head completed.
                let y = d["met_by"].as_str().unwrap_or("");
                match last_completed.get(y) {
                    Some(p) if *p < pos => {
                        let between = (*p + 1..pos).all(|q| {
                            by_pos
                                .get(&q)
                                .is_some_and(|n| follow_up(n["type"].as_str().unwrap_or("")))
                        });
                        if !between {
                            c.bad(format!(
                                "task {id}: DependenciesMet at {pos} is not in the commit of {y}'s completion at {p}"
                            ));
                        }
                    }
                    _ => c.bad(format!(
                        "task {id}: DependenciesMet by {y}, which did not complete before it"
                    )),
                }
                if let Some(t) = tasks.get(&id)
                    && !all_met(&tasks, t)
                {
                    c.bad(format!(
                        "task {id}: DependenciesMet at {pos} with a dependency not met"
                    ));
                }
            }
            "DependencyFailed" => {
                let h = d["head"].as_str().unwrap_or("").to_string();
                *owed.entry((id.clone(), h)).or_default() -= 1;
            }
            "TaskReplaced" => {
                let by = d["by"].as_str().unwrap_or("");
                if !tasks
                    .get(by)
                    .is_some_and(|n| n.replaces.as_deref() == Some(id.as_str()))
                {
                    c.bad(format!(
                        "task {id}: replaced by {by}, which does not declare it"
                    ));
                }
            }
            _ => {}
        }

        // 2. A task leaves waiting only by these.
        if before.as_deref() == Some("waiting")
            && after != "waiting"
            && !matches!(ty, "DependenciesMet" | "TaskCanceled" | "TaskReplaced")
        {
            c.bad(format!("task {id}: left waiting by {ty} at {pos}"));
        }
        // 4. Never claimed while waiting.
        if ty == "TaskClaimed" && before.as_deref() == Some("waiting") {
            c.bad(format!("task {id}: claimed at {pos} while waiting"));
        }

        if let Some(t) = tasks.get_mut(&id) {
            t.state = after.clone();
            if ty == "TaskReplaced" {
                t.replaced_by = d["by"].as_str().map(str::to_string);
            }
        }
        if ty == "TaskCompleted" {
            last_completed.insert(id.clone(), pos);
        }
        // 6. Each failure or cancel of a head is owed to its waiting dependents,
        // unless the task is replaced in the same decision (it then is no head).
        if matches!(ty, "TaskFailed" | "TaskCanceled") {
            let replaced_now = by_pos.get(&(pos + 1)).is_some_and(|n| {
                n["type"] == "TaskReplaced" && n["stream"] == format!("task:{id}")
            });
            if !replaced_now {
                let dependents: Vec<String> = tasks
                    .iter()
                    .filter(|(_, t)| {
                        t.state == "waiting"
                            && t.depends_on
                                .iter()
                                .any(|x| head(&tasks, x).is_some_and(|(h, _)| h == id))
                    })
                    .map(|(k, _)| k.clone())
                    .collect();
                for dep in dependents {
                    *owed.entry((dep, id.clone())).or_default() += 1;
                }
            }
        }
    }
    for (tid, t) in &tasks {
        if t.state == "waiting" && all_met(&tasks, t) {
            c.bad(format!(
                "task {tid}: still waiting though every dependency completed"
            ));
        }
        let mut n = 0;
        let mut cur = t.replaces.clone();
        while let Some(p) = cur {
            n += 1;
            if n > cork_core::deps::MAX_REPLACEMENTS {
                c.bad(format!(
                    "task {tid}: replacement chain longer than {}",
                    cork_core::deps::MAX_REPLACEMENTS
                ));
                break;
            }
            cur = tasks.get(&p).and_then(|x| x.replaces.clone());
        }
        let _ = t.created;
    }
    for ((dep, h), n) in owed {
        if n != 0 {
            c.bad(format!(
                "task {dep}: {} DependencyFailed naming {h} than failures of it while waiting",
                if n > 0 { "fewer" } else { "more" }
            ));
        }
    }
}
