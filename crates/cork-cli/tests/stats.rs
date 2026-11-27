//! `cork stats` against a real daemon (HANDOFF 18.7 items 1, 3, 4): an
//! escalation chain charged to its starting model, an attempt killed with
//! `kill -9` counted as unknown usage, and the same report after `export`
//! and `serve --import`. Dummy agents only.

mod common;

use common::{Env, wait_exit, wait_file};
use serde_json::Value;
use std::time::Duration;

const MODELS: &str = r#"
[model.haiku]
command = ["true"]
tier = 1

[model.sonnet]
command = ["true"]
tier = 2
"#;

fn usage(cost: f64) -> String {
    format!(
        r#"printf '{{"cost_usd":{cost},"input_tokens":1000,"output_tokens":100,"turns":2}}' > "$CORK_USAGE""#
    )
}

/// Run one attempt as a fleet worker of `pool` would.
fn run_as(e: &Env, profile: &str, model: &str, task: &str, script: &str) -> i32 {
    e.cmd()
        .env("CORK_MODEL", model)
        .env("CORK_POOL", model)
        .args([
            "--as", profile, "tasks", "run", task, "--", "sh", "-c", script,
        ])
        .output()
        .unwrap()
        .status
        .code()
        .unwrap()
}

fn group<'a>(r: &'a Value, k: &str) -> &'a Value {
    r["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["key"] == k)
        .unwrap_or_else(|| panic!("no group {k} in {r}"))
}

fn close(a: &Value, b: f64) -> bool {
    (a.as_f64().unwrap() - b).abs() < 1e-9
}

#[test]
fn stats_charge_escalations_to_the_start_and_count_killed_attempts() {
    let e = Env::new();
    let models = e.home().join("models.toml");
    std::fs::write(&models, MODELS).unwrap();
    let models = models.to_str().unwrap();

    // A: haiku fails; B, its escalated retry, sonnet completes.
    let a = e.ok(&["tasks", "create", "work", "fix the bug"]);
    assert_eq!(
        run_as(
            &e,
            "f-haiku-1",
            "haiku",
            &a,
            &format!("{}; exit 1", usage(0.1))
        ),
        8
    );
    let b = e.ok(&[
        "tasks",
        "create",
        "work",
        "fix the bug, again",
        "--label",
        &format!("retry_of={a}"),
    ]);
    assert_eq!(run_as(&e, "f-sonnet-1", "sonnet", &b, &usage(0.5)), 0);

    // C: the first wrapper is killed with kill -9, so its usage is lost;
    // haiku then completes it.
    let c = e.ok(&["tasks", "create", "work", "crash me", "--lease", "1s"]);
    let pid_file = e.home().join("pid");
    let mut w = e
        .cmd()
        .env("CORK_MODEL", "haiku")
        .env("CORK_POOL", "haiku")
        .args([
            "--as",
            "f-haiku-2",
            "tasks",
            "run",
            &c,
            "--",
            "sh",
            "-c",
            &format!("{}; echo $$ > {}; sleep 60", usage(9.0), pid_file.display()),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let sh: i32 = wait_file(&pid_file, Duration::from_secs(10))
        .parse()
        .unwrap();
    unsafe {
        libc::kill(w.id() as i32, libc::SIGKILL);
        libc::kill(-sh, libc::SIGKILL);
    }
    let _ = w.wait();
    e.wait_state(&c, "ready", Duration::from_secs(10));
    assert_eq!(run_as(&e, "f-haiku-1", "haiku", &c, &usage(0.2)), 0);

    let r = e.json(&["stats", "--models", models]);
    let h = group(&r, "haiku");
    assert_eq!(h["tasks"], 2);
    assert_eq!(h["attempts"], 3);
    assert_eq!(h["lease_expiries"], 1);
    assert_eq!(
        h["usage"]["unknown"], 1,
        "the killed attempt is unknown, not zero"
    );
    assert_eq!(h["usage"]["reported"], 2);
    assert_eq!(h["chains"], 2);
    assert_eq!(h["chains_completed"], 2);
    // (0.1 + 0.5) + 0.2, over two successes.
    assert!(close(&h["chain_cost_usd"], 0.8), "{h}");
    assert!(close(&h["cost_per_success"], 0.4), "{h}");
    assert_eq!(h["cost_incomplete"], true);
    let s = group(&r, "sonnet");
    assert_eq!(s["escalated"], 1);
    assert_eq!(s["escalated_completed"], 1);
    assert_eq!(s["chains"], 0, "B's chain started on haiku");
    let split: Vec<(String, String)> = r["finished_by"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| {
            (
                x["started"].as_str().unwrap().into(),
                x["finished_model"].as_str().unwrap().into(),
            )
        })
        .collect();
    assert_eq!(
        split,
        vec![
            ("haiku".into(), "haiku".into()),
            ("haiku".into(), "sonnet".into())
        ]
    );
    assert_eq!(r["total"]["success_rate"], 2.0 / 3.0);

    // The text report says the cost is a lower bound.
    let text = e.ok(&["stats", "--models", models]);
    assert!(text.contains("1 attempts have unknown usage"), "{text}");
    assert!(
        text.lines()
            .any(|l| l.starts_with("haiku") && l.contains("sonnet") && l.contains("$0.60")),
        "{text}"
    );
    let csv = e.ok(&["stats", "--by", "pool", "--csv"]);
    assert!(csv.starts_with("pool,tasks,"), "{csv}");
    let reasons = e.ok(&["stats", "--reasons"]);
    assert!(
        reasons.contains("command exited with status 1"),
        "{reasons}"
    );
    assert!(
        !e.ok(&["stats"]).contains("command exited"),
        "no text unless --reasons"
    );

    // The same numbers from an export, and after serve --import.
    let export = e.home().join("export.jsonl");
    e.ok(&["export", "--out", export.to_str().unwrap()]);
    let from_file = e.json(&[
        "stats",
        "--models",
        models,
        "--file",
        export.to_str().unwrap(),
    ]);
    assert_eq!(from_file, r);
    let e2 = Env::new();
    let mut d = e2.spawn(&[
        "serve",
        "--db",
        e2.db().to_str().unwrap(),
        "--import",
        export.to_str().unwrap(),
    ]);
    wait_file(&e2.home().join("daemon.json"), Duration::from_secs(10));
    let imported = e2.json(&["stats", "--models", models]);
    assert_eq!(imported, r);
    unsafe {
        libc::kill(d.id() as i32, libc::SIGTERM);
    }
    assert_eq!(wait_exit(&mut d, Duration::from_secs(10)), 0);

    // Filters.
    let only_b = e.json(&["stats", "--label", &format!("retry_of={a}")]);
    assert_eq!(only_b["total"]["tasks"], 1);
    let none = e.json(&["stats", "--channel", "other/"]);
    assert_eq!(none["total"]["tasks"], 0);
}
