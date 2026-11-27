//! Task dependencies through the real `cork` binary (HANDOFF 6.8.7). The
//! workers are `sh`, never an agent.

mod common;

use common::*;

#[test]
fn after_show_list_and_dependency_results() {
    let e = Env::new();
    let a = e.ok(&["tasks", "create", "work", "write the schema"]);
    let x = e.ok(&["tasks", "create", "work", "flaky step"]);
    let b = e.ok(&[
        "tasks",
        "create",
        "work",
        "use it",
        "--after",
        &format!("{a},{x}"),
    ]);
    assert_eq!(e.state(&b), "waiting");
    // Claiming it by id is refused, naming what it waits on.
    let out = e.run(&["--as", "w", "tasks", "claim", &b]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains(&a));

    e.ok(&[
        "--as",
        "wa",
        "tasks",
        "run",
        &a,
        "--",
        "sh",
        "-c",
        "echo schema-v2 > $CORK_RESULT",
    ]);
    e.expect(8, &["--as", "wx", "tasks", "run", &x, "--", "false"]);
    let shown = e.ok(&["tasks", "show", &b]);
    assert!(shown.contains("Waiting on:"), "{shown}");
    assert!(shown.contains("[stuck]"), "{shown}");
    let stuck = e.json(&["tasks", "list", "--stuck"]);
    assert_eq!(stuck["tasks"][0]["id"], b.as_str());
    let on = e.json(&["tasks", "list", "--waiting-on", &a]);
    assert_eq!(on["tasks"][0]["id"], b.as_str());

    // A retry takes x's place; b is no longer stuck and readies when it completes.
    let x2 = e.ok(&[
        "tasks",
        "create",
        "work",
        "flaky step, again",
        "--replaces",
        &x,
    ]);
    let shown = e.ok(&["tasks", "show", &x]);
    assert!(shown.contains(&format!("Replaced by: {x2}")), "{shown}");
    assert!(!e.ok(&["tasks", "show", &b]).contains("[stuck]"));
    e.ok(&["--as", "wx", "tasks", "run", &x2, "--", "true"]);
    assert_eq!(e.state(&b), "ready");

    // The worker on b gets both results in CORK_DEPENDENCIES.
    let deps = e.home().join("deps.json");
    let script = format!("cp \"$CORK_DEPENDENCIES\" {}", deps.display());
    e.ok(&["--as", "wb", "tasks", "run", &b, "--", "sh", "-c", &script]);
    let v: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&deps).unwrap()).unwrap();
    assert_eq!(v[0]["task_id"], a.as_str());
    assert_eq!(v[0]["result"].as_str().unwrap().trim(), "schema-v2");
    assert_eq!(v[0]["text"], "write the schema");
    assert_eq!(v[1]["task_id"], x.as_str());
    assert_eq!(v[1]["head"], x2.as_str());
}

#[test]
fn plan_files() {
    let e = Env::new();
    let existing = e.ok(&["tasks", "create", "work", "already there"]);
    let plan = e.home().join("plan.json");
    std::fs::write(
        &plan,
        format!(
            r#"{{
  "idempotency_key": "feature-F-plan-1",
  "tasks": [
    {{ "key": "schema", "text": "Add the column", "labels": {{ "parent": "F" }} }},
    {{ "key": "tests",  "text": "Tests for the column", "after": ["schema"] }},
    {{ "key": "docs",   "text": "Document it", "after": ["schema", "{existing}"] }}
  ]
}}"#
        ),
    )
    .unwrap();
    let p = plan.to_str().unwrap();
    let first = e.json(&["tasks", "create", "work", "--file", p]);
    let again = e.json(&["tasks", "create", "work", "--file", p]);
    assert_eq!(
        first["tasks"], again["tasks"],
        "the plan's key makes resending a no-op"
    );
    let tasks = first["tasks"].as_array().unwrap();
    assert_eq!(tasks.len(), 3);
    assert_eq!(tasks[0]["task"]["state"], "ready");
    assert_eq!(tasks[1]["task"]["state"], "waiting");
    let text = e.ok(&["tasks", "list", "--state", "waiting"]);
    assert_eq!(text.lines().count(), 2, "{text}");
    // A cycle is refused and writes nothing.
    std::fs::write(
        &plan,
        r#"{ "tasks": [ { "key": "a", "text": "a", "after": ["b"] }, { "key": "b", "text": "b", "after": ["a"] } ] }"#,
    )
    .unwrap();
    let out = e.run(&["tasks", "create", "work", "--file", p]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("cycle"));
    assert_eq!(
        e.json(&["tasks", "list"])["tasks"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
}
