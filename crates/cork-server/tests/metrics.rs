//! `GET /metrics` (HANDOFF 18.4): behind the token, and its numbers match
//! the log.

mod common;

use common::*;
use serde_json::json;
use std::collections::BTreeMap;

async fn scrape(d: &Daemon, token: &str) -> (u16, String) {
    let r = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/metrics", d.port))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    (r.status().as_u16(), r.text().await.unwrap())
}

/// `name{labels} value` lines, as a map.
fn parse(text: &str) -> BTreeMap<String, f64> {
    text.lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .filter_map(|l| {
            let (k, v) = l.rsplit_once(' ')?;
            Some((k.to_string(), v.parse().ok()?))
        })
        .collect()
}

#[tokio::test]
async fn metrics_need_the_token_and_match_the_log() {
    let d = Daemon::start().await;
    assert_eq!(scrape(&d, "wrong").await.0, 401);

    let c = d.client("w");
    let a = c.create("work", "a", json!({})).await;
    let b = c.create("work/sub", "b", json!({})).await;
    c.create("work", "c", json!({})).await;
    let a_id = a["task"]["id"].as_str().unwrap().to_string();
    let b_id = b["task"]["id"].as_str().unwrap().to_string();
    let ca = c.claim(json!({"task_id": a_id})).await.ok();
    c.settle(
        &a_id,
        "complete",
        json!({"claim_id": ca["claim_id"], "result": "ok"}),
    )
    .await
    .ok();
    let cb = c.claim(json!({"task_id": b_id})).await.ok();
    // A retried settle is answered from the idempotency key.
    for _ in 0..2 {
        c.post_key(
            &format!("/tasks/{b_id}/fail"),
            "fail-b",
            json!({"claim_id": cb["claim_id"], "reason": "no"}),
        )
        .await
        .ok();
    }

    let (status, text) = scrape(&d, TOKEN).await;
    assert_eq!(status, 200);
    let m = parse(&text);
    assert_eq!(
        m["cork_tasks{channel=\"work\",state=\"completed\"}"], 1.0,
        "{text}"
    );
    assert_eq!(m["cork_tasks{channel=\"work\",state=\"ready\"}"], 1.0);
    assert_eq!(m["cork_tasks{channel=\"work/sub\",state=\"failed\"}"], 1.0);
    assert!(!m.contains_key("cork_tasks{channel=\"work\",state=\"claimed\"}"));
    assert_eq!(m["cork_claims_total"], 2.0);
    assert_eq!(m["cork_settles_total{op=\"complete\"}"], 1.0);
    assert_eq!(m["cork_settles_total{op=\"fail\"}"], 1.0);
    assert_eq!(m["cork_idempotency_hits_total"], 1.0);

    // Every event counter matches the log.
    let events = all_events(&c).await;
    let mut by_type: BTreeMap<String, f64> = BTreeMap::new();
    for e in &events {
        *by_type
            .entry(e["type"].as_str().unwrap().to_string())
            .or_default() += 1.0;
    }
    for (t, n) in &by_type {
        assert_eq!(m[&format!("cork_events_total{{type=\"{t}\"}}")], *n, "{t}");
    }
    let head = events.last().unwrap()["position"].as_f64().unwrap();
    assert_eq!(m["cork_log_head"], head);
    assert!(m["cork_write_seconds_count"] >= m["cork_commits_total"]);
    assert_eq!(m["cork_commit_events_count"], m["cork_commits_total"]);

    // A restart counts stored tasks again; counters start over.
    let (path, port, dir) = d.shutdown().await;
    let d = Daemon::open(path, Some(port)).await;
    let m = parse(&scrape(&d, TOKEN).await.1);
    assert_eq!(m["cork_tasks{channel=\"work\",state=\"completed\"}"], 1.0);
    assert_eq!(m["cork_tasks{channel=\"work/sub\",state=\"failed\"}"], 1.0);
    assert_eq!(m["cork_claims_total"], 0.0);
    drop(dir);
}
