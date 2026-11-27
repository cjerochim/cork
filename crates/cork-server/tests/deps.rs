//! Task dependencies against a real daemon (HANDOFF 6.8, "Done when").
//! Every worker here is a plain HTTP client: no agents run.

mod common;

use common::*;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::time::Duration;

fn s(v: &Value) -> String {
    v.as_str().unwrap().to_string()
}

async fn new_task(c: &Client, text: &str, extra: Value) -> String {
    s(&c.create("work", text, extra).await["task"]["id"])
}

/// Claim `id` by id and settle it with `op`.
async fn run(w: &Client, id: &str, op: &str) -> Value {
    let c = w.claim(json!({ "task_id": id })).await.ok();
    assert_eq!(c["outcome"], "claimed", "{c}");
    let body = match op {
        "complete" => json!({ "claim_id": c["claim_id"], "result": format!("{id} done") }),
        _ => json!({ "claim_id": c["claim_id"], "reason": "broke" }),
    };
    w.settle(id, op, body).await.ok()
}

/// Events of one type in the log.
fn of_type<'a>(events: &'a [Value], t: &str) -> Vec<&'a Value> {
    events.iter().filter(|e| e["type"] == t).collect()
}

fn position_of(events: &[Value], t: &str, task: &str) -> u64 {
    events
        .iter()
        .find(|e| e["type"] == t && e["thread_id"] == task)
        .unwrap_or_else(|| panic!("no {t} on {task}"))["position"]
        .as_u64()
        .unwrap()
}

#[tokio::test]
async fn waits_then_readies_in_the_completing_commit() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let w = d.client("w");
    let a = new_task(&lead, "a", json!({})).await;
    let b = new_task(&lead, "b", json!({})).await;
    let c = lead
        .create("work", "c", json!({ "depends_on": [a, b, a] }))
        .await;
    let cid = s(&c["task"]["id"]);
    assert_eq!(c["task"]["state"], "waiting");
    assert_eq!(c["task"]["depends_on"], json!([a, b]));
    assert_eq!(c["root"]["labels"]["board.task"], "waiting");

    // Never offered: by id it names what it waits on; the queue skips it.
    let r = w.claim(json!({ "task_id": cid })).await;
    assert_eq!(r.code(), "invalid_state");
    assert!(r.body["error"]["message"].as_str().unwrap().contains(&a));
    run(&w, &a, "complete").await;
    let shown = lead.get(&format!("/tasks/{cid}")).await.ok();
    assert_eq!(shown["task"]["state"], "waiting");
    run(&w, &b, "complete").await;

    let events = all_events(&lead).await;
    let met = of_type(&events, "DependenciesMet");
    assert_eq!(met.len(), 1);
    assert_eq!(met[0]["data"]["met_by"], b);
    assert_eq!(met[0]["actor"], "cork");
    assert_eq!(met[0]["labels"][0], json!(["board.task", "ready"]));
    assert_eq!(
        position_of(&events, "DependenciesMet", &cid),
        position_of(&events, "TaskCompleted", &b) + 1,
        "in the commit of the completion that met it"
    );
    let r = w.claim(json!({ "channel": "work" })).await.ok();
    assert_eq!(r["task"]["id"], cid);
}

#[tokio::test]
async fn create_checks() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let a = new_task(&lead, "a", json!({})).await;
    let r = lead
        .post(
            "/tasks",
            json!({ "channel": "work", "text": "x", "depends_on": ["01NOPE"] }),
        )
        .await;
    assert_eq!(r.code(), "not_found");
    let r = lead
        .post(
            "/tasks",
            json!({ "channel": "work", "text": "x", "depends_on": [a], "replaces": a }),
        )
        .await;
    assert_eq!(r.code(), "invalid_input");
    let many: Vec<String> = (0..65).map(|i| format!("x{i}")).collect();
    let r = lead
        .post(
            "/tasks",
            json!({ "channel": "work", "text": "x", "depends_on": many }),
        )
        .await;
    assert_eq!(r.code(), "invalid_input");
    // A dependency may be in another channel, and already done.
    let other = s(&lead.create("elsewhere", "o", json!({})).await["task"]["id"]);
    run(&d.client("w"), &other, "complete").await;
    let t = lead
        .create("work", "x", json!({ "depends_on": [other] }))
        .await;
    assert_eq!(t["task"]["state"], "ready");
}

/// Fan-in: a task waiting on 64 tasks, completed by 20 workers at once,
/// readies exactly once, in the commit of the last completion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fan_in() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let mut deps = vec![];
    for i in 0..64 {
        deps.push(
            new_task(
                &lead,
                &format!("dep {i}"),
                json!({ "labels": { "kind": "dep" } }),
            )
            .await,
        );
    }
    let top = new_task(&lead, "top", json!({ "depends_on": deps })).await;
    let mut hs = vec![];
    for i in 0..20 {
        let w = d.client(&format!("w{i}"));
        hs.push(tokio::spawn(async move {
            loop {
                let r = w
                    .claim(json!({ "channel": "work", "labels": ["kind=dep"] }))
                    .await
                    .ok();
                if r["outcome"] != "claimed" {
                    return;
                }
                let id = s(&r["task"]["id"]);
                w.settle(&id, "complete", json!({ "claim_id": r["claim_id"] }))
                    .await
                    .ok();
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
    let events = all_events(&lead).await;
    let met = of_type(&events, "DependenciesMet");
    assert_eq!(met.len(), 1);
    let last = s(&met[0]["data"]["met_by"]);
    let last_done = of_type(&events, "TaskCompleted").last().unwrap()["thread_id"].clone();
    assert_eq!(last_done, last, "met by the last completion");
    assert_eq!(
        position_of(&events, "DependenciesMet", &top),
        position_of(&events, "TaskCompleted", &last) + 1
    );
}

/// Fan-out: 1,000 tasks waiting on one all ready in its completion commit,
/// and claims that were waiting before it are served first come, first served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fan_out() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let root = new_task(&lead, "root", json!({ "labels": { "kind": "root" } })).await;
    let mut kids = vec![];
    for i in 0..1000 {
        kids.push(
            new_task(
                &lead,
                &format!("kid {i:04}"),
                json!({ "depends_on": [root], "labels": { "kind": "kid" } }),
            )
            .await,
        );
    }
    let mut waiters = vec![];
    for i in 0..5 {
        let w = d.client(&format!("q{i}"));
        waiters.push(tokio::spawn(async move {
            w.claim(json!({ "channel": "work", "labels": ["kind=kid"], "wait_ms": 20000 }))
                .await
                .ok()
        }));
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    run(&d.client("w"), &root, "complete").await;
    let mut got = vec![];
    for h in waiters {
        let r = h.await.unwrap();
        assert_eq!(r["outcome"], "claimed");
        got.push(s(&r["task"]["id"]));
    }
    assert_eq!(
        got,
        kids[..5],
        "oldest kids, to the waiters in arrival order"
    );
    let events = all_events(&lead).await;
    let done = position_of(&events, "TaskCompleted", &root);
    let met: Vec<u64> = of_type(&events, "DependenciesMet")
        .iter()
        .map(|e| e["position"].as_u64().unwrap())
        .collect();
    assert_eq!(met.len(), 1000);
    assert_eq!(*met.first().unwrap(), done + 1);
    // Contiguous: one commit holds the completion, all 1,000 and the claims.
    assert_eq!(*met.last().unwrap(), done + 1000);
    let ready = lead.get("/tasks?state=ready&limit=1000").await.ok();
    assert_eq!(ready["tasks"].as_array().unwrap().len(), 995);
}

/// A chain of 100 tasks run by 10 workers completes in order, and no task is
/// claimed before its dependency completed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chain() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let mut ids = vec![new_task(&lead, "step 0", json!({})).await];
    for i in 1..100 {
        let prev = ids.last().unwrap().clone();
        ids.push(new_task(&lead, &format!("step {i}"), json!({ "depends_on": [prev] })).await);
    }
    let mut hs = vec![];
    for i in 0..10 {
        let w = d.client(&format!("w{i}"));
        let last = ids.last().unwrap().clone();
        let lead = lead.clone();
        hs.push(tokio::spawn(async move {
            loop {
                let r = w
                    .claim(json!({ "channel": "work", "wait_ms": 300 }))
                    .await
                    .ok();
                if r["outcome"] == "claimed" {
                    let id = s(&r["task"]["id"]);
                    w.settle(&id, "complete", json!({ "claim_id": r["claim_id"] }))
                        .await
                        .ok();
                }
                let t = lead.get(&format!("/tasks/{last}")).await.ok();
                if t["task"]["state"] == "completed" {
                    return;
                }
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
    let events = all_events(&lead).await;
    let order: Vec<String> = of_type(&events, "TaskCompleted")
        .iter()
        .map(|e| s(&e["thread_id"]))
        .collect();
    assert_eq!(order, ids);
    for pair in ids.windows(2) {
        assert!(
            position_of(&events, "TaskClaimed", &pair[1])
                > position_of(&events, "TaskCompleted", &pair[0])
        );
    }
}

/// Escalation: a task with 10 waiting dependents fails, is replaced twice,
/// and the second retry completes. All 10 ready in that commit, and nothing
/// but `DependencyFailed` was written on them in between.
#[tokio::test]
async fn escalation_by_replacement() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let w = d.client("w");
    let x = new_task(&lead, "x", json!({})).await;
    let mut kids = vec![];
    for i in 0..10 {
        kids.push(new_task(&lead, &format!("kid {i}"), json!({ "depends_on": [x] })).await);
    }
    run(&w, &x, "fail").await;

    let events = all_events(&lead).await;
    let failed = of_type(&events, "DependencyFailed");
    assert_eq!(failed.len(), 10);
    let fail_pos = position_of(&events, "TaskFailed", &x);
    for (i, e) in failed.iter().enumerate() {
        assert_eq!(e["position"].as_u64().unwrap(), fail_pos + 1 + i as u64);
        assert_eq!(e["data"]["dependency"], x);
        assert_eq!(e["data"]["head"], x);
        assert_eq!(e["data"]["state"], "failed");
        assert_eq!(e["data"]["reason"], "broke");
        let labels = e["labels"].as_array().unwrap();
        assert!(
            labels.contains(&json!(["board.mention", "lead"])),
            "{labels:?}"
        );
        assert!(
            labels.contains(&json!(["board.mention", "chris"])),
            "{labels:?}"
        );
        assert!(labels.contains(&json!(["board.task", "waiting"])));
    }
    // The lead and the person see it through the mention filter.
    let inbox = lead
        .get("/events?after=0&mentions=true&type=DependencyFailed")
        .await
        .ok();
    assert_eq!(inbox["events"].as_array().unwrap().len(), 10);

    // Replace it; the retry fails too; replace that one.
    let r1 = lead
        .create("work", "x again", json!({ "replaces": x }))
        .await;
    let r1id = s(&r1["task"]["id"]);
    assert_eq!(r1["task"]["replaces"], x);
    assert_eq!(r1["task"]["state"], "ready");
    let old = lead.get(&format!("/tasks/{x}")).await.ok();
    assert_eq!(old["task"]["replaced_by"], r1id);
    assert_eq!(old["task"]["state"], "failed", "a failed task stays failed");
    run(&w, &r1id, "fail").await;
    let r2 = lead
        .create("work", "x, harder", json!({ "replaces": r1id }))
        .await;
    let r2id = s(&r2["task"]["id"]);
    assert_eq!(
        lead.history_types(&r1id).await,
        ["TaskCreated", "TaskClaimed", "TaskFailed", "TaskReplaced"]
    );
    run(&w, &r2id, "complete").await;

    let events = all_events(&lead).await;
    let done = position_of(&events, "TaskCompleted", &r2id);
    for kid in &kids {
        assert_eq!(
            lead.history_types(kid).await,
            [
                "TaskCreated",
                "DependencyFailed",
                "DependencyFailed",
                "DependenciesMet"
            ]
        );
        let met = position_of(&events, "DependenciesMet", kid);
        assert!(met > done && met <= done + 10, "in the completion's commit");
        let t = lead.get(&format!("/tasks/{kid}")).await.ok();
        assert_eq!(t["task"]["state"], "ready");
        assert_eq!(t["task"]["depends_on"], json!([x]), "never edited");
    }
    let second = of_type(&events, "DependencyFailed")
        .into_iter()
        .filter(|e| e["data"]["head"] == r1id)
        .count();
    assert_eq!(
        second, 10,
        "the retry's failure names the declared id and its head"
    );
}

#[tokio::test]
async fn replacement_rules() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let w = d.client("w");

    // Never while claimed or once completed.
    let a = new_task(&lead, "a", json!({})).await;
    let c = w.claim(json!({ "task_id": a })).await.ok();
    let r = lead
        .post(
            "/tasks",
            json!({ "channel": "work", "text": "r", "replaces": a }),
        )
        .await;
    assert_eq!(r.code(), "invalid_state");
    w.settle(&a, "complete", json!({ "claim_id": c["claim_id"] }))
        .await
        .ok();
    let r = lead
        .post(
            "/tasks",
            json!({ "channel": "work", "text": "r", "replaces": a }),
        )
        .await;
    assert_eq!(r.code(), "invalid_state");

    // A ready task is canceled by its replacement, in one commit.
    let b = new_task(&lead, "b", json!({})).await;
    let b2 = s(&lead.create("work", "b2", json!({ "replaces": b })).await["task"]["id"]);
    let events = all_events(&lead).await;
    assert_eq!(
        position_of(&events, "TaskReplaced", &b),
        position_of(&events, "TaskCreated", &b2) + 1
    );
    let bt = lead.get(&format!("/tasks/{b}")).await.ok();
    assert_eq!(bt["task"]["state"], "canceled");
    // Twice is refused, naming the first; reopen is refused.
    let r = lead
        .post(
            "/tasks",
            json!({ "channel": "work", "text": "r", "replaces": b }),
        )
        .await;
    assert_eq!(r.code(), "invalid_state");
    assert!(r.body["error"]["message"].as_str().unwrap().contains(&b2));
    let r = lead.settle(&b, "reopen", json!({})).await;
    assert_eq!(r.code(), "invalid_state");
    // The person's other profiles are the owner too (another person is
    // refused; core tests cover it, this daemon has one person).
    let e = new_task(&lead, "e", json!({})).await;
    let r = d
        .client("someone")
        .post(
            "/tasks",
            json!({ "channel": "work", "text": "r", "replaces": e }),
        )
        .await;
    assert_eq!(r.status, 200, "same person, different profile");
}

/// A dependency whose attempts run out under the lease timer is surfaced
/// by the timer's commit.
#[tokio::test]
async fn lease_timer_failure_is_surfaced() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let a = new_task(&lead, "a", json!({ "lease_ms": 150, "max_attempts": 1 })).await;
    let b = new_task(&lead, "b", json!({ "depends_on": [a] })).await;
    let c = d.client("dead").claim(json!({ "task_id": a })).await.ok();
    assert_eq!(c["outcome"], "claimed");
    let r = lead
        .get(&format!(
            "/events?after=0&stream=task:{b}&type=DependencyFailed&wait_ms=5000"
        ))
        .await
        .ok();
    let e = &r["events"][0];
    assert_eq!(e["data"]["head"], a);
    assert_eq!(e["actor"], "cork");
    let events = all_events(&lead).await;
    assert_eq!(
        position_of(&events, "DependencyFailed", &b),
        position_of(&events, "TaskFailed", &a) + 1
    );
}

#[tokio::test]
async fn reopen_waits_again_and_cancel_works_while_waiting() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let w = d.client("w");
    let a = new_task(&lead, "a", json!({})).await;
    let b = new_task(&lead, "b", json!({ "depends_on": [a] })).await;
    run(&w, &a, "complete").await;
    run(&w, &b, "complete").await;
    // Reopen the dependency: b, finished, is not touched.
    lead.settle(&a, "reopen", json!({})).await.ok();
    assert_eq!(
        lead.get(&format!("/tasks/{b}")).await.ok()["task"]["state"],
        "completed"
    );
    // Reopen b while a is open: it waits again, and readies when a completes.
    let r = lead.settle(&b, "reopen", json!({})).await.ok();
    assert_eq!(r["task"]["state"], "waiting");
    let h = lead.get(&format!("/tasks/{b}/history")).await.ok();
    let last = h["events"].as_array().unwrap().last().unwrap().clone();
    assert_eq!(last["data"]["open_dependencies"], json!([a]));
    assert_eq!(last["schema"], 2);
    run(&w, &a, "complete").await;
    assert_eq!(
        lead.get(&format!("/tasks/{b}")).await.ok()["task"]["state"],
        "ready"
    );

    let c = new_task(&lead, "c", json!({ "depends_on": [b] })).await;
    let r = lead
        .settle(&c, "cancel", json!({ "reason": "not needed" }))
        .await
        .ok();
    assert_eq!(r["task"]["state"], "canceled");
}

/// The dependency index survives a restart, and a rebuild from the log
/// writes the same rows.
#[tokio::test]
async fn projections_follow_the_log() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let a = new_task(&lead, "a", json!({})).await;
    let b = new_task(&lead, "b", json!({ "depends_on": [a] })).await;
    let a2 = s(&lead.create("work", "a2", json!({ "replaces": a })).await["task"]["id"]);
    let before = d.board.store().projection_dump().await.unwrap();
    assert!(
        before
            .iter()
            .any(|l| l.starts_with("task_dependencies") && l.contains(&b))
    );

    let (path, port, dir) = d.shutdown().await;
    let d = Daemon::open(path, Some(port)).await;
    let _dir = dir;
    d.board.store().rebuild_projections().await.unwrap();
    assert_eq!(d.board.store().projection_dump().await.unwrap(), before);
    let lead = d.client("lead");
    run(&d.client("w"), &a2, "complete").await;
    let t = lead.get(&format!("/tasks/{b}")).await.ok();
    assert_eq!(
        t["task"]["state"], "ready",
        "found through the index after restart"
    );
    let ids: HashSet<String> =
        cork_store::Store::dependents(&**d.board.store(), std::slice::from_ref(&a))
            .await
            .unwrap()
            .into_iter()
            .collect();
    assert_eq!(ids, HashSet::from([b]));
}
