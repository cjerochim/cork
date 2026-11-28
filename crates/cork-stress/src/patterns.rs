//! The `patterns` scenario (HANDOFF 19.10, item 4): reads and claims with
//! wildcard selectors, against plain ones, over a few hundred channels.
//!
//! Tasks go round-robin into `work/s<i>`, `work/s<i>/tests` and
//! `work/s<i>/docs`. Reads are timed for plain and pattern selectors, with
//! the list of known channels and again with the fallback scan, and each
//! answer is checked against the core matcher. Then workers drain the board,
//! each with its own selector, and every claim must match the selector that
//! made it. The log invariants are checked at the end.

use crate::daemon::Daemon;
use crate::scenarios::{Params, Report};
use crate::stats::Samples;
use cork_core::ChannelSelector;
use cork_store::{Store, TaskQuery};
use serde_json::json;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Selectors timed for reads, plain ones first.
const READS: [(&str, &str); 6] = [
    ("plain", "work/"),
    ("plain", "work/s1/"),
    ("pattern", "work/*/tests"),
    ("pattern", "**/docs"),
    ("list", "work/*/tests,work/*/docs"),
    ("exclusion", "work/,!work/*/docs"),
];

/// Each worker claims with one of these, in turn.
const CLAIMS: [&str; 4] = [
    "work/*/tests",
    "work/*/docs",
    "**,!work/*/tests,!work/*/docs",
    "work/",
];

pub async fn patterns(p: &Params) -> Report {
    let mut r = Report::new("patterns");
    let d = Daemon::start(p.sync).await;
    let services = p.n(200).max(4);
    let tasks = p.n(6_000).max(services * 3);
    let workers = p.n(40).max(CLAIMS.len());
    let lead = d.client("lead");

    // Create the tasks, recording each one's channel.
    let mut channel_of = std::collections::HashMap::new();
    for i in 0..tasks {
        let s = i % services;
        let ch = match (i / services) % 3 {
            0 => format!("work/s{s}"),
            1 => format!("work/s{s}/tests"),
            _ => format!("work/s{s}/docs"),
        };
        let id = lead
            .create(&ch, &format!("task {i}"), 60_000, 3)
            .await
            .expect("create");
        channel_of.insert(id, ch);
    }
    r.line(format!(
        "{tasks} tasks over {} channels, {workers} workers, sync {:?}",
        services * 3,
        p.sync
    ));

    // Reads: every answer agrees with the core matcher.
    let rounds = p.n(200).max(5);
    for (path, list_max) in [("known-channel list", 500), ("fallback scan", 0)] {
        cork_store_sqlite::set_pattern_list_max(list_max);
        for (kind, sel) in READS {
            let parsed = ChannelSelector::parse(sel).unwrap();
            let want: BTreeSet<String> = channel_of
                .iter()
                .filter(|(_, ch)| parsed.matches(ch))
                .map(|(id, _)| id.clone())
                .collect();
            // The whole answer, straight from the store (HTTP pages at 1,000).
            let got: BTreeSet<String> = d
                .board
                .store()
                .list_tasks(&TaskQuery {
                    channel: Some(parsed.clone()),
                    limit: 10_000,
                    now: d.board.now(),
                    ..Default::default()
                })
                .await
                .expect("list")
                .into_iter()
                .map(|t| t.task.id)
                .collect();
            if got != want {
                r.failures.push(format!(
                    "{path}: {sel} listed {} tasks, the core matcher says {}",
                    got.len(),
                    want.len()
                ));
            }
            let q = format!("/tasks?channel={sel}&state=ready&limit=20");
            let mut lat = Samples::default();
            for _ in 0..rounds {
                let t = Instant::now();
                lead.get(&q).await.expect("list");
                lat.push(t.elapsed());
            }
            r.line(format!(
                "read {kind:9} {sel:28} ({path}): {}",
                lat.summary()
            ));
        }
    }
    cork_store_sqlite::set_pattern_list_max(500);

    // Claims: workers drain the board, each with its selector.
    let wrong = Arc::new(AtomicUsize::new(0));
    let mut hs = vec![];
    for w in 0..workers {
        let c = d.client(&format!("w{w}"));
        let sel = CLAIMS[w % CLAIMS.len()];
        let wrong = wrong.clone();
        hs.push(tokio::spawn(async move {
            let parsed = ChannelSelector::parse(sel).unwrap();
            let mut lat = Samples::default();
            loop {
                let t = Instant::now();
                let v = c
                    .post("/tasks/claim", json!({ "channel": sel }))
                    .await
                    .expect("claim");
                if v["outcome"] != "claimed" {
                    return (sel, lat);
                }
                lat.push(t.elapsed());
                let ch = v["task"]["channel"].as_str().unwrap_or_default();
                if !parsed.matches(ch) {
                    wrong.fetch_add(1, Ordering::Relaxed);
                }
                let id = v["task"]["id"].as_str().unwrap_or_default();
                let claim = v["claim_id"].as_str().unwrap_or_default();
                c.settle(id, "complete", claim).await.expect("complete");
            }
        }));
    }
    let mut by_sel: std::collections::BTreeMap<&str, Samples> = Default::default();
    for h in hs {
        let (sel, lat) = h.await.unwrap();
        by_sel.entry(sel).or_default().extend(lat);
    }
    let mut claimed = 0;
    for sel in CLAIMS {
        let lat = by_sel.remove(sel).unwrap_or_default();
        claimed += lat.len();
        r.line(format!(
            "claim {sel:30}: {} claims, {}",
            lat.len(),
            lat.summary()
        ));
    }
    if claimed != tasks {
        r.failures
            .push(format!("{claimed} of {tasks} tasks were claimed"));
    }
    let n = wrong.load(Ordering::Relaxed);
    if n > 0 {
        r.failures.push(format!(
            "{n} claims took a task their selector does not match"
        ));
    }
    r.check(&d).await;
    r
}
