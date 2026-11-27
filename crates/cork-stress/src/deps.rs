//! The `deps` scenario (HANDOFF 6.8.9, "Chaos"): random dependencies and
//! random replacements under workers that fail and vanish.
//!
//! A fake lead creates tasks that each wait on up to three older tasks,
//! some as plan files, and replaces some tasks at random. Workers fail every
//! "hard" task the first time and some vanish mid-claim, so leases run out.
//! The lead replaces each failed task it sees. The run ends when the head of
//! every chain has completed. Then the log is checked (invariants 1 to 8),
//! and nothing may be left waiting or stuck.

use crate::daemon::Daemon;
use crate::scenarios::{Params, Report};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const CH: &str = "deps";

/// A small deterministic generator, so a failing run can be repeated.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
}

fn id_of(v: &Value) -> String {
    v["task"]["id"].as_str().unwrap_or_default().to_string()
}

pub async fn deps(p: &Params) -> Report {
    let mut r = Report::new("deps");
    let d = Daemon::start(p.sync).await;
    let n = p.n(1_000).max(20);
    let workers = p.n(20).max(4);
    let lead = d.client("lead");
    let mut rng = Rng(0x5eed);
    let stop = Arc::new(AtomicBool::new(false));
    let vanished = Arc::new(AtomicUsize::new(0));

    // Workers start first, so tasks are taken while the plan grows.
    let mut hs = vec![];
    for w in 0..workers {
        let c = d.client(&format!("w{w}"));
        let stop = stop.clone();
        let vanished = vanished.clone();
        let vanish_after = (w % 5 == 4).then_some(w % 7 + 2);
        hs.push(tokio::spawn(async move {
            let mut claims = 0;
            while !stop.load(Ordering::Relaxed) {
                let v = c
                    .post("/tasks/claim", json!({ "channel": CH, "wait_ms": 300 }))
                    .await
                    .expect("claim");
                if v["outcome"] != "claimed" {
                    continue;
                }
                claims += 1;
                if vanish_after == Some(claims) {
                    vanished.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                let id = id_of(&v);
                let text = v["root"]["text"].as_str().unwrap_or("");
                // Each dependency's result is passed on (Decision 2).
                let want = v["task"]["depends_on"].as_array().map_or(0, |a| a.len());
                let got = v["dependencies"].as_array().map_or(0, |a| a.len());
                assert_eq!(want, got, "task {id}: {got} dependency results for {want}");
                let (op, body) = if text.contains("[hard]") && !text.starts_with("retry") {
                    (
                        "fail",
                        json!({ "claim_id": v["claim_id"], "reason": "too hard" }),
                    )
                } else {
                    (
                        "complete",
                        json!({ "claim_id": v["claim_id"], "result": format!("{id} ok") }),
                    )
                };
                // A lease may have run out under a slow commit; that is fine.
                let _ = c.post(&format!("/tasks/{id}/{op}"), body).await;
            }
        }));
    }

    let mut ids: Vec<String> = vec![];
    let mut planned = 0;
    let mut random_replaced = 0;
    let lease = 1_000;
    let mut i = 0;
    while i < n {
        let pick = |rng: &mut Rng, ids: &Vec<String>| -> Vec<String> {
            let k = rng.below(4) as usize;
            (0..k.min(ids.len()))
                .map(|_| ids[rng.below(ids.len() as u64) as usize].clone())
                .collect()
        };
        if i % 50 == 25 && i + 5 <= n {
            // A plan of five: a little graph of its own, joined to older tasks.
            let mut tasks = vec![];
            for k in 0..5 {
                let mut after: Vec<String> = (0..k)
                    .filter(|_| rng.below(2) == 0)
                    .map(|j| format!("k{j}"))
                    .collect();
                after.extend(pick(&mut rng, &ids));
                let hard = if rng.below(8) == 0 { " [hard]" } else { "" };
                tasks.push(json!({
                    "key": format!("k{k}"), "text": format!("plan {i} step {k}{hard}"),
                    "after": after, "lease_ms": lease, "max_attempts": 2,
                }));
            }
            let key = format!("plan-{i}");
            let body = json!({ "channel": CH, "tasks": tasks });
            let a = lead
                .post_idem("/tasks/plan", body.clone(), &key)
                .await
                .expect("plan");
            let b = lead
                .post_idem("/tasks/plan", body, &key)
                .await
                .expect("plan again");
            if a["tasks"] != b["tasks"] {
                r.failures
                    .push(format!("{key}: a resent plan made new tasks"));
            }
            for t in a["tasks"].as_array().cloned().unwrap_or_default() {
                ids.push(id_of(&t));
            }
            planned += 5;
            i += 5;
            continue;
        }
        let after = pick(&mut rng, &ids);
        let hard = if rng.below(8) == 0 { " [hard]" } else { "" };
        let v = lead
            .post(
                "/tasks",
                json!({ "channel": CH, "text": format!("task {i}{hard}"), "depends_on": after,
                        "lease_ms": lease, "max_attempts": 2 }),
            )
            .await
            .expect("create");
        ids.push(id_of(&v));
        // Now and then, replace a random task. Refused while claimed or done.
        if rng.below(20) == 0 {
            let old = ids[rng.below(ids.len() as u64) as usize].clone();
            if let Ok(v) = lead
                .post(
                    "/tasks",
                    json!({ "channel": CH, "text": format!("retry: a new take on {old}"),
                            "replaces": old, "lease_ms": lease, "max_attempts": 2 }),
                )
                .await
            {
                random_replaced += 1;
                ids.push(id_of(&v));
            }
        }
        i += 1;
    }

    // The lead replaces every failed head it sees, until every head completed.
    let start = Instant::now();
    let mut replaced_failed = 0;
    let mut seen: BTreeSet<String> = BTreeSet::new();
    loop {
        let failed = lead
            .get(&format!("/tasks?channel={CH}&state=failed&limit=1000"))
            .await
            .expect("list failed");
        for t in failed["tasks"].as_array().cloned().unwrap_or_default() {
            let id = t["id"].as_str().unwrap_or_default().to_string();
            if !t["replaced_by"].is_null() || !seen.insert(id.clone()) {
                continue;
            }
            // Twice, as a lead that crashed after the first would.
            let key = format!("replace-{id}");
            let body = json!({ "channel": CH, "text": format!("retry of {id}"), "replaces": id,
                               "lease_ms": lease, "max_attempts": 3 });
            let a = lead
                .post_idem("/tasks", body.clone(), &key)
                .await
                .expect("replace");
            let b = lead
                .post_idem("/tasks", body, &key)
                .await
                .expect("replace again");
            if id_of(&a) != id_of(&b) {
                r.failures.push(format!("{id}: two replacements"));
            }
            replaced_failed += 1;
        }
        let open = lead
            .get(&format!("/tasks?channel={CH}&limit=1000&state=waiting"))
            .await
            .expect("list waiting")["tasks"]
            .as_array()
            .map_or(0, |a| a.len());
        let mut unfinished = 0;
        for s in ["ready", "claimed", "blocked"] {
            unfinished += lead
                .get(&format!("/tasks?channel={CH}&limit=1000&state={s}"))
                .await
                .expect("list")["tasks"]
                .as_array()
                .map_or(0, |a| a.len());
        }
        let failed_heads = failed["tasks"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|t| t["replaced_by"].is_null())
            .count();
        if open == 0 && unfinished == 0 && failed_heads == 0 {
            break;
        }
        if start.elapsed() > Duration::from_secs(600) {
            r.failures.push(format!(
                "not done in 10 minutes: {open} waiting, {unfinished} unfinished, {failed_heads} failed heads"
            ));
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let drain = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        h.await.unwrap();
    }
    let stuck = lead
        .get(&format!("/tasks?channel={CH}&stuck=true"))
        .await
        .expect("stuck")["tasks"]
        .as_array()
        .map_or(0, |a| a.len());
    if stuck != 0 {
        r.failures
            .push(format!("{stuck} tasks still stuck at the end"));
    }
    r.line(format!(
        "{} tasks ({planned} from plans), {workers} workers ({} vanished), 1 s leases",
        ids.len(),
        vanished.load(Ordering::Relaxed)
    ));
    r.line(format!(
        "{random_replaced} random replacements, {replaced_failed} failed tasks replaced; done {:.2}s after the last create",
        drain.as_secs_f64()
    ));
    r.check(&d).await;
    r
}
