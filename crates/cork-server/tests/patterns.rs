//! Channel patterns over HTTP (HANDOFF 19.10, item 3): a wait, a stream and a
//! waiting claim on `work/*/tests` wake for a channel created after they
//! started, and not for `work/tests`.

mod common;

use common::*;
use serde_json::json;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wait_wakes_for_a_new_matching_channel() {
    let d = Daemon::start().await;
    let c = d.client("lead");
    let waiter = {
        let c = c.clone();
        tokio::spawn(async move {
            c.get("/events?channel=work/*/tests&wait_ms=5000")
                .await
                .ok()
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    c.post("/channels/work%2Ftests/messages", json!({ "text": "no" }))
        .await
        .ok();
    c.post(
        "/channels/work%2Fapi%2Ftests/messages",
        json!({ "text": "yes" }),
    )
    .await
    .ok();
    let r = waiter.await.unwrap();
    let events = r["events"].as_array().unwrap();
    assert_eq!(events.len(), 1, "{r}");
    assert_eq!(events[0]["data"]["text"], "yes");

    // Catch-up reads agree, with a repeated `channel` and an exclusion.
    let r = c
        .get("/events?after=0&channel=**&channel=!work/api/")
        .await
        .ok();
    let texts: Vec<&str> = r["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["data"]["text"].as_str().unwrap())
        .collect();
    assert_eq!(texts, ["no"]);
}

#[tokio::test]
async fn a_stream_takes_patterns() {
    let d = Daemon::start().await;
    let c = d.client("lead");
    let mut resp = c
        .http
        .get(format!(
            "{}/events/stream?after=0&channel=work/*/tests&channel=docs/",
            c.base
        ))
        .bearer_auth(&c.token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    for ch in ["work/tests", "work/api/tests", "other", "docs/howto"] {
        c.post(
            &format!("/channels/{}/messages", ch.replace('/', "%2F")),
            json!({ "text": ch }),
        )
        .await
        .ok();
    }
    let mut buf = String::new();
    let mut ids = vec![];
    while ids.len() < 2 {
        let chunk = tokio::time::timeout(Duration::from_secs(5), resp.chunk())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        buf.push_str(std::str::from_utf8(&chunk).unwrap());
        while let Some(i) = buf.find("\n\n") {
            let msg: String = buf.drain(..i + 2).collect();
            if let Some(line) = msg.lines().find(|l| l.starts_with("id:")) {
                ids.push(line[3..].trim().parse::<u64>().unwrap());
            }
        }
    }
    assert_eq!(ids, [2, 4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_waiting_claim_wakes_for_a_new_matching_channel() {
    let d = Daemon::start().await;
    let c = d.client("lead");
    let waiter = {
        let w = d.client("w1");
        tokio::spawn(async move {
            w.claim(json!({
                "channel": "work/*/tests",
                "channels": ["!work/secret/tests"],
                "wait_ms": 5000
            }))
            .await
            .ok()
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    let skip1 = c.create("work/tests", "no", json!({})).await;
    let skip2 = c.create("work/secret/tests", "no", json!({})).await;
    let want = c.create("work/api/tests", "yes", json!({})).await;
    let r = waiter.await.unwrap();
    assert_eq!(r["task"]["id"], want["task"]["id"]);

    // `claim --ready` with a list takes the oldest task any pattern matches.
    let r = d
        .client("w2")
        .claim(json!({ "channel": "nothing,work/*" }))
        .await
        .ok();
    assert_eq!(r["task"]["id"], skip1["task"]["id"]);
    let r = d
        .client("w3")
        .claim(json!({ "channel": "**/tests" }))
        .await
        .ok();
    assert_eq!(r["task"]["id"], skip2["task"]["id"]);
}

#[tokio::test]
async fn bad_patterns_are_invalid_input() {
    let d = Daemon::start().await;
    let c = d.client("lead");
    for q in [
        "/tasks?channel=work-*",
        "/tasks?channel=!work/",
        "/events?channel=a,,b",
        "/messages?channel=w*rk",
    ] {
        let r = c.get(q).await;
        assert_eq!(r.status, 400, "{q}: {}", r.body);
        assert_eq!(r.code(), "invalid_input", "{q}");
    }
    let r = c.claim(json!({ "channel": "work/**-x" })).await;
    assert_eq!(r.code(), "invalid_input");
    let many = vec!["a"; 17].join(",");
    let r = c.get(&format!("/tasks?channel={many}")).await;
    assert_eq!(r.code(), "invalid_input");
}
