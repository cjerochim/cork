//! Usage capture by `cork tasks run` (HANDOFF 18.2, 18.7 item 2): one
//! `usage=attempt` reply per attempt, for every outcome, and a broken usage
//! file changes nothing about settling.

mod common;

use common::Env;
use serde_json::Value;

/// The usage replies in a task's thread.
fn usage_replies(e: &Env, task: &str) -> Vec<Value> {
    let t = e.json(&["read", task]);
    t["replies"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["labels"]["usage"] == "attempt")
        .cloned()
        .collect()
}

const WRITE_USAGE: &str = r#"printf '{"model":"fake-1","input_tokens":100,"output_tokens":20,"cost_usd":0.05,"turns":3,"custom":"kept"}' > "$CORK_USAGE""#;

#[test]
fn a_usage_file_gives_one_reply_per_attempt_for_every_outcome() {
    let e = Env::new();

    // Completed.
    let ok = e.ok(&["tasks", "create", "work", "do it"]);
    e.ok(&[
        "--as",
        "w",
        "tasks",
        "run",
        &ok,
        "--",
        "sh",
        "-c",
        &format!("{WRITE_USAGE}; echo done"),
    ]);
    let r = usage_replies(&e, &ok);
    assert_eq!(r.len(), 1, "{r:?}");
    let d = &r[0]["data"];
    assert_eq!(d["outcome"], "completed");
    assert_eq!(d["exit"], 0);
    assert_eq!(d["attempt"], 1);
    assert_eq!(d["task"], ok.as_str());
    assert_eq!(d["profile"], "w");
    assert_eq!(d["model"], "fake-1");
    assert_eq!(d["input_tokens"], 100);
    assert_eq!(d["output_tokens"], 20);
    assert_eq!(d["cost_usd"], 0.05);
    assert_eq!(d["cost_source"], "agent");
    assert_eq!(d["turns"], 3);
    assert_eq!(d["custom"], "kept", "unknown fields are kept");
    assert_eq!(d["source"], "file");
    assert!(d["duration_ms"].as_u64().is_some());
    let claim = e.json(&["tasks", "history", &ok])["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ev| ev["type"] == "TaskClaimed")
        .unwrap()["data"]["claim_id"]
        .clone();
    assert_eq!(d["claim_id"], claim);
    assert_eq!(e.json(&["tasks", "show", &ok])["task"]["result"], "done\n");

    // Failed, then reopened and failed again: one reply per attempt.
    let bad = e.ok(&["tasks", "create", "work", "fail it"]);
    for i in 0..2 {
        if i > 0 {
            e.ok(&["tasks", "reopen", &bad]);
        }
        e.expect(
            8,
            &[
                "--as",
                "w",
                "tasks",
                "run",
                &bad,
                "--",
                "sh",
                "-c",
                &format!("{WRITE_USAGE}; exit 4"),
            ],
        );
    }
    let r = usage_replies(&e, &bad);
    assert_eq!(r.len(), 2, "{r:?}");
    assert_eq!(r[0]["data"]["outcome"], "failed");
    assert_eq!(r[0]["data"]["exit"], 4);
    assert_eq!(r[0]["data"]["attempt"], 1);
    // A reopen starts the attempts again; the claims differ.
    assert_ne!(r[0]["data"]["claim_id"], r[1]["data"]["claim_id"]);

    // Blocked from inside.
    let q = e.ok(&["tasks", "create", "work", "ask"]);
    e.expect(
        9,
        &[
            "--as",
            "w",
            "tasks",
            "run",
            &q,
            "--lease",
            "600ms",
            "--",
            "sh",
            "-c",
            &format!("{WRITE_USAGE}; cork tasks block \"$CORK_TASK\" 'which branch?' && sleep 1"),
        ],
    );
    let r = usage_replies(&e, &q);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0]["data"]["outcome"], "blocked");

    // Stopped by --max-runtime.
    let slow = e.ok(&["tasks", "create", "work", "slow"]);
    e.expect(
        10,
        &[
            "--as",
            "w",
            "tasks",
            "run",
            &slow,
            "--max-runtime",
            "500ms",
            "--",
            "sh",
            "-c",
            &format!("{WRITE_USAGE}; sleep 30"),
        ],
    );
    let r = usage_replies(&e, &slow);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0]["data"]["outcome"], "max_runtime");
    assert_eq!(r[0]["data"]["exit"], Value::Null);
    assert_eq!(r[0]["data"]["cost_usd"], 0.05);
}

#[test]
fn claude_json_output_gives_usage_and_a_readable_result() {
    let e = Env::new();
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/claude-result.json"
    );
    let t = e.ok(&["tasks", "create", "work", "add a test"]);
    e.ok(&[
        "--as",
        "w",
        "tasks",
        "run",
        &t,
        "--usage",
        "claude-json",
        "--",
        "cat",
        fixture,
    ]);
    assert_eq!(
        e.json(&["tasks", "show", &t])["task"]["result"],
        "Added the test and it passes."
    );
    let r = usage_replies(&e, &t);
    assert_eq!(r.len(), 1);
    let d = &r[0]["data"];
    assert_eq!(d["source"], "claude-json");
    assert_eq!(d["cost_usd"], 0.1873);
    assert_eq!(d["turns"], 7);
    assert_eq!(d["model"], "claude-sonnet-4-5-20250929");
    assert_eq!(d["output_tokens"], 2114);
    assert!(
        !r[0]["text"].as_str().unwrap().contains("Added the test"),
        "the usage reply never carries output (18.6)"
    );

    // Under a fleet, the catalog's model name wins and the agent's is kept.
    let t2 = e.ok(&["tasks", "create", "work", "again"]);
    let o = e
        .cmd()
        .env("CORK_MODEL", "sonnet")
        .env("CORK_POOL", "sonnet")
        .args([
            "--as",
            "f-sonnet-1",
            "tasks",
            "run",
            &t2,
            "--usage",
            "claude-json",
            "--",
            "cat",
            fixture,
        ])
        .output()
        .unwrap();
    assert!(o.status.success());
    let d = &usage_replies(&e, &t2)[0]["data"];
    assert_eq!(d["model"], "sonnet");
    assert_eq!(d["agent_model"], "claude-sonnet-4-5-20250929");
    assert_eq!(d["pool"], "sonnet");

    // A parser that finds nothing changes nothing: the task completes with
    // its stdout, and the reply says the usage is empty.
    let t3 = e.ok(&["tasks", "create", "work", "plain"]);
    e.ok(&[
        "--as",
        "w",
        "tasks",
        "run",
        &t3,
        "--usage",
        "claude-json",
        "--",
        "echo",
        "not json",
    ]);
    assert_eq!(
        e.json(&["tasks", "show", &t3])["task"]["result"],
        "not json\n"
    );
    let d = &usage_replies(&e, &t3)[0]["data"];
    assert_eq!(d["source"], Value::Null);
    assert!(d.get("cost_usd").is_none());
}

#[test]
fn a_broken_usage_file_changes_nothing_about_settling() {
    let e = Env::new();
    let t = e.ok(&["tasks", "create", "work", "do it"]);
    e.ok(&[
        "--as",
        "w",
        "tasks",
        "run",
        &t,
        "--",
        "sh",
        "-c",
        "echo '{not json' > \"$CORK_USAGE\"; echo fine",
    ]);
    assert_eq!(e.state(&t), "completed");
    assert_eq!(e.json(&["tasks", "show", &t])["task"]["result"], "fine\n");
    assert!(usage_replies(&e, &t).is_empty());

    // No usage file and no parser: no reply at all.
    let t = e.ok(&["tasks", "create", "work", "quiet"]);
    e.ok(&["--as", "w", "tasks", "run", &t, "--", "true"]);
    assert!(usage_replies(&e, &t).is_empty());
}
