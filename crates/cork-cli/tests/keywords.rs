//! Keywords, `--needs` and `workers match` (HANDOFF 16.6, 16.8 items 6 and
//! 7), and `wait --type` for the lead (17.1, 17.6 item 1).

mod common;

use common::*;
use std::path::{Path, PathBuf};
use std::time::Duration;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// An env whose CORK_HOME holds the catalog, and a fleet config naming it.
fn env_with_catalog(workers: Option<&str>) -> Env {
    let e = Env::new();
    std::fs::copy(fixture("models.toml"), e.home().join("models.toml")).unwrap();
    if let Some(w) = workers {
        std::fs::write(e.home().join("workers.toml"), w).unwrap();
    }
    e
}

const WORKERS: &str = r#"
[fleet]
name = "dev"
models = "models.toml"

[pool.strong]
model = "opus"
select = "work/"

[pool.fast]
model = "haiku"
select = "work/"
count = 2

[pool.review]
model = "reviewer"
select = "work/"
"#;

#[test]
fn needs_checks_the_vocabulary_and_rewrites_aliases() {
    let e = env_with_catalog(None);
    let o = e.run(&["tasks", "create", "work", "x", "--needs", "typscript"]);
    assert_eq!(o.status.code(), Some(2));
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(
        err.contains("unknown keyword `typscript`; did you mean `typescript`?"),
        "{err}"
    );
    let id = e.ok(&[
        "tasks", "create", "work", "x", "--needs", "ts,docs", "--needs", "tier-2",
    ]);
    let t = e.json(&["tasks", "show", &id]);
    let labels = &t["task"]["labels"];
    assert_eq!(labels["needs.typescript"], "yes");
    assert_eq!(labels["needs.docs"], "yes");
    assert_eq!(labels["needs.tier-2"], "yes");
    assert!(labels.get("needs.ts").is_none());
    // tier-4 does not exist in this catalog.
    e.expect(2, &["tasks", "create", "work", "x", "--needs", "tier-4"]);
}

#[test]
fn needs_without_a_catalog_is_a_usage_error() {
    let e = Env::new();
    let o = e.run(&["tasks", "create", "work", "x", "--needs", "rust"]);
    assert_eq!(o.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&o.stderr).contains("no model catalog"));
    // --models names one explicitly.
    let m = fixture("models.toml");
    e.ok(&[
        "tasks",
        "create",
        "work",
        "x",
        "--needs",
        "rust",
        "--models",
        m.to_str().unwrap(),
    ]);
}

#[test]
fn keywords_lists_who_covers_what() {
    let e = env_with_catalog(None);
    let out = e.ok(&["workers", "keywords"]);
    assert!(out.contains("rust\topus, sonnet"), "{out}");
    assert!(
        out.contains("typescript (also ts)\thaiku, opus, sonnet"),
        "{out}"
    );
    assert!(out.contains("tier-3\topus"), "{out}");
    let j = e.json(&["workers", "keywords"]);
    assert!(j["keywords"].as_array().unwrap().len() >= 10);
}

#[test]
fn match_before_the_task_exists() {
    let e = env_with_catalog(None);
    let out = e.ok(&["workers", "match", "--needs", "rust"]);
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(
        lines,
        vec![
            "haiku: missing rust",
            "sonnet: covers",
            "opus: covers (costs 5x sonnet)",
            "reviewer: missing rust",
        ]
    );
    // Nothing covers vision plus review.
    e.expect(3, &["workers", "match", "--needs", "vision,review"]);
    let j = e.json(&["workers", "match", "--needs", "ts"]);
    assert_eq!(j["needs"][0], "typescript");
}

#[test]
fn match_a_task_against_the_fleet() {
    let e = env_with_catalog(Some(WORKERS));
    let id = e.ok(&["tasks", "create", "work/app", "port it", "--needs", "rust"]);
    let out = e.ok(&["workers", "match", &id]);
    assert!(out.contains("pool fast (haiku): missing rust"), "{out}");
    assert!(out.contains("pool strong (opus): covers"), "{out}");
    assert!(
        out.contains("fleet dev gives it to pool strong (opus), the cheapest that covers it"),
        "{out}"
    );
    let docs = e.ok(&[
        "tasks",
        "create",
        "work/app",
        "typo",
        "--needs",
        "documentation",
    ]);
    let j = e.json(&["workers", "match", &docs]);
    assert_eq!(j["choice"]["pool"], "fast");
    assert_eq!(j["ranked"], serde_json::json!(["fast", "strong"]));
    // A raw label outside the vocabulary: unmatched, never silently routed.
    let raw = e.ok(&[
        "tasks",
        "create",
        "work/app",
        "x",
        "--label",
        "needs.typscript=yes",
    ]);
    let j: serde_json::Value =
        serde_json::from_str(&e.expect(3, &["--json", "workers", "match", &raw])).unwrap();
    assert!(j["choice"].is_null());
    assert_eq!(j["unknown"][0], "typscript");
    // A task in a channel no pool selects.
    let f = e.ok(&[
        "tasks",
        "create",
        "features",
        "a feature",
        "--label",
        "kind=feature",
    ]);
    e.expect(3, &["workers", "match", &f]);
}

#[test]
fn wait_type_wakes_on_any_child_of_a_feature() {
    let e = Env::new();
    let a = e.ok(&["tasks", "create", "work", "child a", "--label", "parent=F1"]);
    let _other = e.ok(&["tasks", "create", "work", "unrelated"]);
    let mut w = e.spawn(&[
        "--json",
        "wait",
        "work/",
        "--label",
        "parent=F1",
        "--type",
        "TaskCompleted",
        "--type",
        "TaskFailed",
        "--timeout",
        "20s",
    ]);
    std::thread::sleep(Duration::from_millis(500));
    // Claims and other tasks' events do not wake it.
    e.ok(&["--as", "w", "tasks", "claim", &a]);
    e.ok(&["--as", "w", "tasks", "complete", &a, "done"]);
    assert_eq!(wait_exit(&mut w, Duration::from_secs(10)), 0);
    let out = w.wait_with_output().unwrap();
    let ev: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(ev["type"], "TaskCompleted");
    assert_eq!(ev["stream"], format!("task:{a}"));
}
