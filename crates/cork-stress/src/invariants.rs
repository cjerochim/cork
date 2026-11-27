//! Invariants checked from the log after every run (HANDOFF 11, item 13):
//!
//! - Positions have no gaps, and versions in each stream are 1, 2, 3, ….
//! - At most one terminal event per task per open period.
//! - A claim ends (lease end, release, block or settle) before the next claim.
//! - No `TaskCompleted` or `TaskFailed` from a claim that was not live at its
//!   `at` time, and no `LeaseExpired` before the lease really ended.
//! - Projections equal a fresh fold of the log.

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
        Ok(Some(st)) => c.bad(format!(
            "task {id}: projection differs from a fresh fold (state {:?} vs {:?}, version {} vs {})",
            st.task.state, folded.state, st.task.version, folded.version
        )),
        other => c.bad(format!("task {id}: no projection: {other:?}")),
    }
}
