//! API tests against a real daemon: HANDOFF 11 "Done when" items 3-10 and 12
//! at the HTTP level, plus idempotency, the stream and export.

mod common;

use common::*;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::{Duration, Instant};

fn s(v: &Value) -> String {
    v.as_str().unwrap().to_string()
}

#[tokio::test]
async fn post_reply_read_and_thread() {
    let d = Daemon::start().await;
    let c = d.client("lead");
    let m = c
        .post(
            "/channels/work%2Ffix/messages",
            json!({ "text": "hi @Fixer", "labels": { "step": "fix" } }),
        )
        .await
        .ok();
    assert_eq!(m["channel"], "work/fix");
    assert_eq!(m["actor"]["profile"], "lead");
    assert_eq!(m["mentions"][0], "fixer");
    let id = s(&m["id"]);
    let r = c
        .post(
            &format!("/threads/{id}/replies"),
            json!({ "text": "on it" }),
        )
        .await
        .ok();
    assert!(r["position"].as_u64() > m["position"].as_u64());
    // Reply to the reply lands in the same thread.
    let r2 = c
        .post(
            &format!("/threads/{}/replies", s(&r["id"])),
            json!({ "text": "ok" }),
        )
        .await
        .ok();
    assert_eq!(r2["thread_id"], id);
    let t = c.get(&format!("/threads/{}", s(&r["id"]))).await.ok();
    assert_eq!(t["root"]["id"], id);
    assert_eq!(t["replies"].as_array().unwrap().len(), 2);
    let msgs = c.get("/messages?channel=work/&label=step=fix").await.ok();
    assert_eq!(msgs["messages"].as_array().unwrap().len(), 1);
    let ch = c.get("/channels").await.ok();
    assert_eq!(ch["channels"][0]["messages"], 3);
    let bad = c
        .post(
            "/channels/dev/messages",
            json!({ "text": "x", "labels": { "board.task": "done" } }),
        )
        .await;
    assert_eq!(bad.status, 400);
    assert_eq!(bad.code(), "reserved_label");
    assert_eq!(
        c.get("/threads/01J0000000000000000000NONE").await.status,
        404
    );
}

#[tokio::test]
async fn security_checks() {
    let d = Daemon::start().await;
    let http = reqwest::Client::new();
    // No token.
    let r = http.get(format!("{}/whoami", d.url)).send().await.unwrap();
    assert_eq!(r.status(), 401);
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["error"]["code"], "unauthorized");
    // Wrong token.
    let r = http
        .get(format!("{}/whoami", d.url))
        .bearer_auth("nope")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    // Health needs no token.
    let r = http.get(format!("{}/health", d.url)).send().await.unwrap();
    assert_eq!(r.status(), 200);
    // A foreign Host is refused, even for health (DNS rebinding).
    let r = http
        .get(format!("{}/health", d.url))
        .header("host", "evil.example:80")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    // A foreign Origin is refused; the daemon's own origin is fine.
    let r = http
        .get(format!("{}/whoami", d.url))
        .bearer_auth(TOKEN)
        .header("origin", "https://evil.example")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let r = http
        .get(format!("{}/whoami", d.url))
        .bearer_auth(TOKEN)
        .header("origin", format!("http://127.0.0.1:{}", d.port))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.headers().contains_key("x-cork-head"));
    let w: Value = r.json().await.unwrap();
    assert_eq!(w["person"], "chris");
    // A bad profile name is invalid input, not a new identity.
    let r = http
        .get(format!("{}/whoami", d.url))
        .bearer_auth(TOKEN)
        .header("x-cork-profile", "no spaces")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn claim_settle_and_codes() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let w1 = d.client("w1");
    let w2 = d.client("w2");
    let t = lead
        .create("work", "do it", json!({ "labels": { "step": "fix" } }))
        .await;
    let id = s(&t["task"]["id"]);
    assert_eq!(t["task"]["state"], "ready");
    assert_eq!(t["root"]["labels"]["board.task"], "ready");

    let c1 = w1.claim(json!({ "task_id": id })).await.ok();
    assert_eq!(c1["outcome"], "claimed");
    assert_eq!(c1["attempt"], 1);
    let claim = s(&c1["claim_id"]);
    let r = w2.claim(json!({ "task_id": id })).await;
    assert_eq!((r.status, r.code()), (409, "task_claimed"));
    // Nothing else is ready.
    let r = w2.claim(json!({ "channel": "work" })).await.ok();
    assert_eq!(r["outcome"], "nothing_ready");
    assert!(r["task"].is_null());

    let r = w1.settle(&id, "complete", json!({})).await;
    assert_eq!(r.code(), "claim_not_held");
    let r = w1
        .settle(&id, "complete", json!({ "claim_id": "wrong" }))
        .await;
    assert_eq!(r.code(), "claim_lost");
    let r = w1
        .settle(&id, "renew", json!({ "claim_id": claim }))
        .await
        .ok();
    assert_eq!(r["task"]["state"], "claimed");
    let r = w1
        .settle(
            &id,
            "complete",
            json!({ "claim_id": claim, "result": "done" }),
        )
        .await
        .ok();
    assert_eq!(r["task"]["state"], "completed");
    assert_eq!(r["replayed"], false);
    // Lost-response test (11.8): the retry gets the first result, replayed.
    let r = w1
        .settle(
            &id,
            "complete",
            json!({ "claim_id": claim, "result": "done" }),
        )
        .await
        .ok();
    assert_eq!(r["replayed"], true);
    assert_eq!(r["task"]["result"], "done");
    // A different op from the same claim is still lost.
    assert_eq!(
        w1.settle(&id, "fail", json!({ "claim_id": claim, "reason": "x" }))
            .await
            .code(),
        "claim_lost"
    );
    let r = lead.settle(&id, "cancel", json!({})).await;
    assert_eq!(r.code(), "invalid_state");
    let r = lead.settle(&id, "reopen", json!({})).await.ok();
    assert_eq!(r["task"]["state"], "ready");
    assert_eq!(lead.settle(&id, "nope", json!({})).await.status, 404);

    let types = lead.history_types(&id).await;
    assert_eq!(
        types,
        [
            "TaskCreated",
            "TaskClaimed",
            "LeaseRenewed",
            "TaskCompleted",
            "TaskReopened"
        ]
    );
    let list = lead.get("/tasks?state=ready&label=step=fix").await.ok();
    assert_eq!(list["tasks"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn block_unblock_and_ownership() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let w = d.client("w");
    let t = lead.create("work", "x", json!({})).await;
    let id = s(&t["task"]["id"]);
    let c = w.claim(json!({ "channel": "work" })).await.ok();
    let claim = s(&c["claim_id"]);
    let r = w
        .settle(
            &id,
            "block",
            json!({ "claim_id": claim, "question": "which db?" }),
        )
        .await
        .ok();
    assert_eq!(r["task"]["state"], "blocked");
    assert_eq!(r["task"]["question"], "which db?");
    // Any profile of the owner acts as the owner.
    let r = w
        .settle(&id, "unblock", json!({ "note": "sqlite" }))
        .await
        .ok();
    assert_eq!(r["task"]["state"], "ready");
    // `--for`: only that handle may claim.
    let t2 = lead
        .create("work", "y", json!({ "assignee": "reviewer" }))
        .await;
    let id2 = s(&t2["task"]["id"]);
    let r = w.claim(json!({ "task_id": id2 })).await;
    assert_eq!(r.status, 403, "{:?}", r.body);
    let r = d
        .client("reviewer")
        .claim(json!({ "task_id": id2 }))
        .await
        .ok();
    assert_eq!(r["outcome"], "claimed");
}

/// 11.3: 20 workers claim from 100 tasks at once. Each task completes exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn race_twenty_workers_hundred_tasks() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    for i in 0..100 {
        lead.create("race", &format!("t{i}"), json!({})).await;
    }
    let mut handles = vec![];
    for w in 0..20 {
        let c = d.client(&format!("w{w}"));
        handles.push(tokio::spawn(async move {
            let mut done = 0;
            loop {
                let r = c.claim(json!({ "channel": "race" })).await.ok();
                if r["outcome"] == "nothing_ready" {
                    return done;
                }
                let id = s(&r["task"]["id"]);
                let claim = s(&r["claim_id"]);
                c.settle(&id, "complete", json!({ "claim_id": claim }))
                    .await
                    .ok();
                done += 1;
            }
        }));
    }
    let mut total = 0;
    for h in handles {
        total += h.await.unwrap();
    }
    assert_eq!(total, 100);
    let mut completed: HashMap<String, usize> = HashMap::new();
    for e in all_events(&lead).await {
        if e["type"] == "TaskCompleted" {
            *completed.entry(s(&e["thread_id"])).or_default() += 1;
        }
    }
    assert_eq!(completed.len(), 100);
    assert!(completed.values().all(|n| *n == 1));
}

/// 11.4 at the API level: the worker vanishes after claiming. After the
/// lease, another worker gets the task; the late complete is `claim_lost`;
/// the log holds `LeaseExpired` between the claims.
#[tokio::test]
async fn crashed_worker_loses_its_claim() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let t = lead.create("work", "x", json!({ "lease_ms": 300 })).await;
    let id = s(&t["task"]["id"]);
    let dead = d.client("dead");
    let c1 = dead.claim(json!({ "channel": "work" })).await.ok();
    let start = Instant::now();
    let c2 = d
        .client("alive")
        .claim(json!({ "channel": "work", "wait_ms": 5000 }))
        .await
        .ok();
    assert_eq!(c2["outcome"], "claimed");
    assert_eq!(c2["attempt"], 2);
    assert!(start.elapsed() >= Duration::from_millis(250));
    let r = d
        .client("alive")
        .settle(&id, "complete", json!({ "claim_id": c2["claim_id"] }))
        .await
        .ok();
    assert_eq!(r["task"]["state"], "completed");
    let late = dead
        .settle(&id, "complete", json!({ "claim_id": c1["claim_id"] }))
        .await;
    assert_eq!(late.code(), "claim_lost");
    assert_eq!(
        lead.history_types(&id).await,
        [
            "TaskCreated",
            "TaskClaimed",
            "LeaseExpired",
            "TaskClaimed",
            "TaskCompleted"
        ]
    );
    // The timer wrote the lease end as the system actor.
    let h = lead.get(&format!("/tasks/{id}/history")).await.ok();
    assert_eq!(h["events"][2]["actor"], "cork");
}

/// 11.5: claims that never settle fail the task after `max_attempts` leases.
#[tokio::test]
async fn attempts_run_out() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let t = lead
        .create("work", "x", json!({ "lease_ms": 150, "max_attempts": 2 }))
        .await;
    let id = s(&t["task"]["id"]);
    let w = d.client("w");
    for _ in 0..2 {
        let r = w
            .claim(json!({ "channel": "work", "wait_ms": 3000 }))
            .await
            .ok();
        assert_eq!(r["outcome"], "claimed");
    }
    // Wait for the task to fail by watching for TaskFailed, not by sleeping.
    let r = lead
        .get(&format!(
            "/events?after=0&stream=task:{id}&type=TaskFailed&wait_ms=3000"
        ))
        .await
        .ok();
    assert_eq!(r["events"].as_array().unwrap().len(), 1);
    assert_eq!(
        lead.history_types(&id).await,
        [
            "TaskCreated",
            "TaskClaimed",
            "LeaseExpired",
            "TaskClaimed",
            "LeaseExpired",
            "TaskFailed"
        ]
    );
    let shown = lead.get(&format!("/tasks/{id}")).await.ok();
    assert_eq!(shown["task"]["state"], "failed");
    assert_eq!(shown["root"]["labels"]["board.task"], "failed");
}

/// 11.6: a wait returns within 100 ms of a matching post, and a
/// `claim --ready --wait` within 100 ms of a lease ending.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waits_wake_fast() {
    let d = Daemon::start().await;
    let c = d.client("lead");
    let waiter = {
        let c = c.clone();
        tokio::spawn(async move {
            let r = c
                .get("/events?channel=dev&label=k=v&wait_ms=5000")
                .await
                .ok();
            (Instant::now(), r)
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    c.post("/channels/other/messages", json!({ "text": "no" }))
        .await
        .ok();
    c.post("/channels/dev/messages", json!({ "text": "no" }))
        .await
        .ok();
    let posted = Instant::now();
    c.post(
        "/channels/dev/messages",
        json!({ "text": "yes", "labels": { "k": "v" } }),
    )
    .await
    .ok();
    let (woke, r) = waiter.await.unwrap();
    let events = r["events"].as_array().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["data"]["text"], "yes");
    assert!(
        woke.duration_since(posted) < Duration::from_millis(100),
        "{:?}",
        woke.duration_since(posted)
    );

    // A wait with nothing to see times out with a cursor to resume from.
    let r = c.get("/events?channel=nothing&wait_ms=100").await.ok();
    assert!(r["events"].as_array().unwrap().is_empty());
    assert_eq!(r["next_after"], 3);

    // Claim wait wakes on the lease end.
    let t = c.create("q", "x", json!({ "lease_ms": 400 })).await;
    let c1 = d.client("w1").claim(json!({ "channel": "q" })).await.ok();
    let expires = c1["expires_at"].as_i64().unwrap();
    let r = d
        .client("w2")
        .claim(json!({ "channel": "q", "wait_ms": 5000 }))
        .await
        .ok();
    let now = r["now"].as_i64().unwrap();
    assert_eq!(r["task"]["id"], t["task"]["id"]);
    assert!(
        now - expires <= 100,
        "woke {} ms after the lease ended",
        now - expires
    );
}

/// 11.7: 20 claims waiting on one channel are served in the order they
/// started waiting.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn waiting_claims_are_fifo() {
    let d = Daemon::start().await;
    let lead = d.client("lead");
    let mut handles = vec![];
    for i in 0..20 {
        let c = d.client(&format!("w{i:02}"));
        handles.push(tokio::spawn(async move {
            let r = c
                .claim(json!({ "channel": "fair", "wait_ms": 20000 }))
                .await
                .ok();
            (i, r)
        }));
        // Make the arrival order certain.
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    for i in 0..20 {
        lead.create("fair", &format!("t{i:02}"), json!({})).await;
    }
    let mut got = vec![];
    for h in handles {
        let (i, r) = h.await.unwrap();
        assert_eq!(r["outcome"], "claimed");
        let root_text = s(&r["root"]["text"]);
        got.push((i, root_text));
    }
    for (i, text) in got {
        assert_eq!(text, format!("t{i:02}"), "waiter {i} got {text}");
    }
}

#[tokio::test]
async fn claim_wait_times_out() {
    let d = Daemon::start().await;
    let start = Instant::now();
    let r = d
        .client("w")
        .claim(json!({ "channel": "empty", "wait_ms": 200 }))
        .await
        .ok();
    assert_eq!(r["outcome"], "timeout");
    assert!(start.elapsed() >= Duration::from_millis(190));
    // The timed-out waiter is gone: a new task stays ready for the next claim.
    d.client("lead").create("empty", "x", json!({})).await;
    let r = d
        .client("w2")
        .claim(json!({ "channel": "empty" }))
        .await
        .ok();
    assert_eq!(r["outcome"], "claimed");
}

#[tokio::test]
async fn claim_takes_only_own_tasks_unless_from() {
    let d = Daemon::start().await;
    d.client("lead").create("work", "x", json!({})).await;
    let mut dana = d.client("w");
    // Same token maps to chris; simulate another owner by reading `from`.
    dana.profile = "w".into();
    let r = dana
        .claim(json!({ "channel": "work", "from": "dana" }))
        .await
        .ok();
    assert_eq!(r["outcome"], "nothing_ready");
    let r = dana
        .claim(json!({ "channel": "work", "from": "*" }))
        .await
        .ok();
    assert_eq!(r["outcome"], "claimed");
}

#[tokio::test]
async fn idempotency_keys() {
    let d = Daemon::start().await;
    let c = d.client("lead");
    let a = c
        .post_key("/tasks", "k1", json!({ "channel": "w", "text": "once" }))
        .await
        .ok();
    let b = c
        .post_key("/tasks", "k1", json!({ "channel": "w", "text": "once" }))
        .await
        .ok();
    assert_eq!(a, b);
    let list = c.get("/tasks?channel=w").await.ok();
    assert_eq!(list["tasks"].as_array().unwrap().len(), 1);
    let r = c
        .post_key(
            "/tasks",
            "k1",
            json!({ "channel": "w", "text": "different" }),
        )
        .await;
    assert_eq!((r.status, r.code()), (409, "idempotency_mismatch"));
    // Keys are per person and path+body; the same key on a post is a different request.
    let r = c
        .post_key("/channels/w/messages", "k1", json!({ "text": "hi" }))
        .await;
    assert_eq!(r.code(), "idempotency_mismatch");
}

/// 11.10 at the API level: `GET /tasks/{id}?at=` replays state at each position.
#[tokio::test]
async fn task_at_position() {
    let d = Daemon::start().await;
    let c = d.client("lead");
    let t = c.create("work", "x", json!({ "lease_ms": 150 })).await;
    let id = s(&t["task"]["id"]);
    let w = d.client("w");
    w.claim(json!({ "channel": "work" })).await.ok();
    let c2 = w
        .claim(json!({ "channel": "work", "wait_ms": 3000 }))
        .await
        .ok();
    w.settle(&id, "complete", json!({ "claim_id": c2["claim_id"] }))
        .await
        .ok();
    let h = c.get(&format!("/tasks/{id}/history")).await.ok();
    let mut want = vec![];
    for e in h["events"].as_array().unwrap() {
        let pos = e["position"].as_u64().unwrap();
        let state = e["labels"]
            .as_array()
            .unwrap()
            .iter()
            .find(|l| l[0] == "board.task")
            .unwrap()[1]
            .clone();
        want.push((pos, state));
    }
    assert_eq!(want.len(), 5);
    for (pos, state) in want {
        let r = c.get(&format!("/tasks/{id}?at={pos}")).await.ok();
        assert_eq!(r["task"]["state"], state, "at {pos}");
    }
    let first = h["events"][0]["position"].as_u64().unwrap();
    assert_eq!(
        c.get(&format!("/tasks/{id}?at={}", first - 1)).await.status,
        404
    );
}

/// 11.9 at the API level: restart with live claims; restart grace keeps them.
#[tokio::test]
async fn restart_keeps_live_claims() {
    let d = Daemon::start().await;
    let c = d.client("lead");
    let t = c.create("work", "x", json!({ "lease_ms": 1500 })).await;
    let id = s(&t["task"]["id"]);
    let cl = d.client("w").claim(json!({ "channel": "work" })).await.ok();
    let (path, port, dir) = d.shutdown().await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let d = Daemon::open(path, Some(port)).await;
    let w = d.client("w");
    // Past the original lease end, but within the grace lease.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let r = w
        .settle(&id, "renew", json!({ "claim_id": cl["claim_id"] }))
        .await
        .ok();
    assert_eq!(r["task"]["state"], "claimed");
    let types = d.client("lead").history_types(&id).await;
    assert_eq!(
        types,
        ["TaskCreated", "TaskClaimed", "LeaseRenewed", "LeaseRenewed"]
    );
    let h = d
        .client("lead")
        .get(&format!("/tasks/{id}/history"))
        .await
        .ok();
    assert_eq!(h["events"][2]["data"]["reason"], "daemon_restart");
    drop(dir);
}

#[tokio::test]
async fn event_stream_resumes() {
    let d = Daemon::start().await;
    let c = d.client("lead");
    for i in 0..3 {
        c.post("/channels/s/messages", json!({ "text": format!("m{i}") }))
            .await
            .ok();
    }
    let read_sse = |after_header: Option<&'static str>, query: &'static str, want: usize| {
        let c = c.clone();
        async move {
            let mut r = c
                .http
                .get(format!("{}/events/stream{query}", c.base))
                .bearer_auth(&c.token);
            if let Some(h) = after_header {
                r = r.header("last-event-id", h);
            }
            let mut resp = r.send().await.unwrap();
            assert_eq!(resp.status(), 200);
            let mut buf = String::new();
            let mut ids = vec![];
            while ids.len() < want {
                let chunk = tokio::time::timeout(Duration::from_secs(5), resp.chunk())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                buf.push_str(std::str::from_utf8(&chunk).unwrap());
                while let Some(i) = buf.find("\n\n") {
                    let msg: String = buf.drain(..i + 2).collect();
                    assert!(!msg.contains("event:"), "no event: field");
                    if let Some(line) = msg.lines().find(|l| l.starts_with("id:")) {
                        ids.push(line[3..].trim().parse::<u64>().unwrap());
                    }
                }
            }
            ids
        }
    };
    assert_eq!(read_sse(None, "?after=0", 3).await, [1, 2, 3]);
    // Last-Event-ID wins over `after`.
    let live = tokio::spawn(read_sse(Some("2"), "?after=0", 2));
    tokio::time::sleep(Duration::from_millis(200)).await;
    c.post("/channels/s/messages", json!({ "text": "m3" }))
        .await
        .ok();
    assert_eq!(live.await.unwrap(), [3, 4]);
}

#[tokio::test]
async fn export_is_the_whole_log() {
    let d = Daemon::start().await;
    let c = d.client("lead");
    c.create("w", "x", json!({})).await;
    c.post("/channels/w/messages", json!({ "text": "y" }))
        .await
        .ok();
    let text = c
        .http
        .get(format!("{}/export", c.base))
        .bearer_auth(&c.token)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let lines: Vec<Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 3);
    for (i, l) in lines.iter().enumerate() {
        assert_eq!(l["position"], i as u64 + 1);
        let t = l["type"].as_str().unwrap();
        assert_eq!(l["schema"], cork_core::schema::current(t));
    }
}
