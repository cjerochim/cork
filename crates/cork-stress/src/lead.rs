//! The `lead` scenario (HANDOFF 17.5): a fake lead and fake workers run 100
//! features linked to their children by plain `parent=<id>` labels.
//!
//! Checks, from the log:
//! - The lead's wait on `parent=<id>` saw every child's settle event.
//! - Each feature's rollup (`tasks?label=parent=…`) matches the log.
//! - No feature completes before all of its children are settled.
//! - Escalations never duplicate, even when the lead repeats each write
//!   as a restarted lead would.

use crate::client::{CallResult, Client};
use crate::daemon::Daemon;
use crate::scenarios::{Params, Report};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const WORK: &str = "work";
const FEATURES: &str = "features";

fn enc(s: &str) -> String {
    s.replace('=', "%3D").replace('/', "%2F")
}

/// Create a task under an idempotency key, with labels.
async fn create(
    c: &Client,
    channel: &str,
    text: &str,
    labels: Value,
    key: &str,
) -> CallResult<String> {
    let v = c
        .post_idem(
            "/tasks",
            json!({"channel": channel, "text": text, "labels": labels, "max_attempts": 1, "lease_ms": 60_000}),
            key,
        )
        .await?;
    Ok(v["task"]["id"].as_str().unwrap_or_default().to_string())
}

/// What one feature's lead saw and did.
#[derive(Default)]
struct Run {
    feature: String,
    children: BTreeSet<String>,
    /// Failed child to its retry.
    retries: BTreeMap<String, String>,
    /// (task, event type) seen through the wait.
    seen: BTreeSet<(String, String)>,
    failures: Vec<String>,
}

/// Hard tasks fail on the first model; retries always succeed.
fn hard(text: &str) -> bool {
    text.bytes().map(u32::from).sum::<u32>() % 4 == 0
}

async fn lead_one(c: Client, i: usize) -> Run {
    let mut r = Run::default();
    let head = c.get("/whoami").await.expect("whoami")["head"]
        .as_u64()
        .unwrap();
    let f = create(
        &c,
        FEATURES,
        &format!("feature {i}"),
        json!({"kind": "feature"}),
        &format!("feat-{i}"),
    )
    .await
    .expect("feature");
    r.feature = f.clone();
    let claim = c
        .post("/tasks/claim", json!({"task_id": f, "lease_ms": 600_000}))
        .await
        .expect("claim the feature");
    let claim_id = claim["claim_id"].as_str().unwrap().to_string();
    let parent = format!("parent={f}");

    // Plan: 3 to 5 children. A restarted lead repeats the plan: same ids.
    let n = 3 + i % 3;
    for round in 0..2 {
        for k in 0..n {
            let id = create(
                &c,
                WORK,
                &format!("feature {i} child {k}"),
                json!({"parent": f}),
                &format!("{f}-{k}"),
            )
            .await
            .expect("child");
            if round == 0 {
                r.children.insert(id);
            } else if !r.children.contains(&id) {
                r.failures
                    .push(format!("{f}: re-planning made a new child {id}"));
            }
        }
    }

    // Watch every child through one label wait, never by polling tasks.
    let mut after = head;
    let mut settled: BTreeSet<String> = BTreeSet::new();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let want: BTreeSet<&String> = r.children.iter().chain(r.retries.values()).collect();
        if want.iter().all(|t| settled.contains(*t)) {
            break;
        }
        if Instant::now() > deadline {
            r.failures.push(format!("{f}: children did not settle"));
            break;
        }
        let page = c
            .get(&format!(
                "/events?label={}&type=TaskCompleted&type=TaskFailed&after={after}&wait_ms=5000",
                enc(&parent)
            ))
            .await
            .expect("wait");
        after = page["next_after"].as_u64().unwrap_or(after);
        for e in page["events"].as_array().cloned().unwrap_or_default() {
            let task = e["stream"]
                .as_str()
                .unwrap_or("")
                .trim_start_matches("task:")
                .to_string();
            let ty = e["type"].as_str().unwrap_or("").to_string();
            r.seen.insert((task.clone(), ty.clone()));
            settled.insert(task.clone());
            if ty == "TaskFailed" && !r.retries.contains_key(&task) {
                // Escalate, twice, as a lead that crashed after the first.
                let key = format!("escalate-{task}");
                let labels = json!({"parent": f, "retry_of": task, "escalation": "1"});
                let a = create(&c, WORK, &format!("retry of {task}"), labels.clone(), &key)
                    .await
                    .expect("retry");
                let b = create(&c, WORK, &format!("retry of {task}"), labels, &key)
                    .await
                    .expect("retry again");
                if a != b {
                    r.failures.push(format!("{task}: two retries {a} and {b}"));
                }
                r.retries.insert(task, a);
            }
        }
    }

    // Rollup from one list read must match what the wait saw.
    let list = c
        .get(&format!("/tasks?label={}&limit=1000", enc(&parent)))
        .await
        .expect("rollup");
    let mut rollup: BTreeMap<String, usize> = BTreeMap::new();
    for t in list["tasks"].as_array().cloned().unwrap_or_default() {
        *rollup
            .entry(t["state"].as_str().unwrap_or("").to_string())
            .or_default() += 1;
    }
    let seen_done = r.seen.iter().filter(|(_, t)| t == "TaskCompleted").count();
    let seen_failed = r.seen.iter().filter(|(_, t)| t == "TaskFailed").count();
    if rollup.get("completed").copied().unwrap_or(0) != seen_done
        || rollup.get("failed").copied().unwrap_or(0) != seen_failed
        || rollup.values().sum::<usize>() != r.children.len() + r.retries.len()
    {
        r.failures.push(format!(
            "{f}: rollup {rollup:?} but saw {seen_done} completed, {seen_failed} failed"
        ));
    }
    let _ = c
        .post(
            &format!("/tasks/{f}/complete"),
            json!({"claim_id": claim_id, "result": format!("{seen_done} done")}),
        )
        .await
        .expect("complete the feature");
    r
}

pub async fn lead(p: &Params) -> Report {
    let mut r = Report::new("lead");
    let d = Daemon::start(p.sync).await;
    let features = p.n(100);
    let stop = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicUsize::new(0));
    let mut workers = vec![];
    for w in 0..8 {
        let c = d.client(&format!("worker-{w}"));
        let stop = stop.clone();
        let done = done.clone();
        workers.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let v = c
                    .post("/tasks/claim", json!({"channel": WORK, "wait_ms": 500}))
                    .await
                    .expect("claim");
                if v["outcome"] != "claimed" {
                    continue;
                }
                let id = v["task"]["id"].as_str().unwrap().to_string();
                let claim = v["claim_id"].as_str().unwrap().to_string();
                let retry = v["task"]["labels"]["retry_of"].is_string();
                let text = v["root"]["text"].as_str().unwrap_or("");
                let op = if !retry && hard(text) {
                    "fail"
                } else {
                    "complete"
                };
                c.settle(&id, op, &claim).await.expect("settle");
                done.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    let t = Instant::now();
    let mut leads = vec![];
    for i in 0..features {
        let c = d.client("lead");
        leads.push(tokio::spawn(lead_one(c, i)));
    }
    let mut runs = vec![];
    for h in leads {
        runs.push(h.await.expect("lead"));
    }
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.await;
    }
    let children: usize = runs.iter().map(|x| x.children.len()).sum();
    let retries: usize = runs.iter().map(|x| x.retries.len()).sum();
    r.line(format!(
        "{features} features, {children} children, {retries} escalations, {} settles in {:.1}s",
        done.load(Ordering::Relaxed),
        t.elapsed().as_secs_f64()
    ));
    for x in &runs {
        r.failures.extend(x.failures.iter().cloned());
    }

    // Against the log: every child settle was seen, no feature finished early,
    // and each failed child has exactly one retry.
    let events = d.client("checker").all_events().await.expect("log");
    let mut by_feature: BTreeMap<String, &Run> = BTreeMap::new();
    for x in &runs {
        by_feature.insert(x.feature.clone(), x);
    }
    let mut parent_of: BTreeMap<String, String> = BTreeMap::new();
    let mut settled_at: BTreeMap<String, u64> = BTreeMap::new();
    let mut feature_done: BTreeMap<String, u64> = BTreeMap::new();
    let mut retry_count: BTreeMap<String, usize> = BTreeMap::new();
    for e in &events {
        let task = e["stream"]
            .as_str()
            .unwrap_or("")
            .trim_start_matches("task:")
            .to_string();
        let labels: BTreeMap<String, String> = e["labels"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|l| Some((l[0].as_str()?.to_string(), l[1].as_str()?.to_string())))
            .collect();
        let pos = e["position"].as_u64().unwrap_or(0);
        match e["type"].as_str().unwrap_or("") {
            "TaskCreated" => {
                if let Some(p) = labels.get("parent") {
                    parent_of.insert(task.clone(), p.clone());
                }
                if let Some(o) = labels.get("retry_of") {
                    *retry_count.entry(o.clone()).or_default() += 1;
                }
            }
            ty @ ("TaskCompleted" | "TaskFailed") => {
                if let Some(p) = parent_of.get(&task) {
                    settled_at.insert(task.clone(), pos);
                    let seen = by_feature
                        .get(p)
                        .is_some_and(|x| x.seen.contains(&(task.clone(), ty.to_string())));
                    if !seen {
                        r.failures
                            .push(format!("{p}: the wait missed {ty} on {task}"));
                    }
                } else if by_feature.contains_key(&task) && ty == "TaskCompleted" {
                    feature_done.insert(task.clone(), pos);
                }
            }
            _ => {}
        }
    }
    for (child, parent) in &parent_of {
        match (settled_at.get(child), feature_done.get(parent)) {
            (Some(s), Some(f)) if s > f => r
                .failures
                .push(format!("{parent} completed before {child} settled")),
            (None, _) => r.failures.push(format!("{child} never settled")),
            (_, None) => r.failures.push(format!("{parent} never completed")),
            _ => {}
        }
    }
    let failed: usize = runs.iter().map(|x| x.retries.len()).sum();
    for (old, n) in &retry_count {
        if *n != 1 {
            r.failures.push(format!("{old} has {n} retries"));
        }
    }
    if retry_count.len() != failed {
        r.failures.push(format!(
            "{} tasks have retries but {failed} failed",
            retry_count.len()
        ));
    }
    r.line(format!(
        "checked {} events against {} leads",
        events.len(),
        runs.len()
    ));
    r.check(&d).await;
    r
}
