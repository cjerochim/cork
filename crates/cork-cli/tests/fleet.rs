//! Worker fleets end to end (HANDOFF 16.8): restarts, scaling, keyword
//! routing, two fleets on one board, crash loops, supervisor death and
//! reload. Every test runs the real `cork` binary against an unmodified
//! Milestone 1 daemon (item 12), with `sh` commands standing in for models.

mod common;

use common::*;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Child, Stdio};
use std::time::{Duration, Instant};

/// Dummy models. Each prints its model name, so results show who did the work.
/// `$NAP` and `$FIRST_NAP` (on the first attempt only) slow them down.
const MODELS: &str = r#"
[keywords]
rust = "Rust"
typescript = "TypeScript"
docs = "Prose"
large-context = "Large files"

[aliases]
ts = "typescript"

[model.haiku]
command = ["sh", "-c", "[ \"$CORK_ATTEMPT\" = 1 ] && sleep ${FIRST_NAP:-0}; sleep ${NAP:-0.2}; echo $CORK_MODEL did it"]
tier = 1
cost = 1
keywords = ["typescript", "docs"]

[model.sonnet]
command = ["sh", "-c", "[ \"$CORK_ATTEMPT\" = 1 ] && sleep ${FIRST_NAP:-0}; sleep ${NAP:-0.2}; echo $CORK_MODEL did it"]
tier = 2
cost = 3
keywords = ["rust", "typescript", "docs", "large-context"]

[model.opus]
command = ["sh", "-c", "[ \"$CORK_ATTEMPT\" = 1 ] && sleep ${FIRST_NAP:-0}; sleep ${NAP:-0.2}; echo $CORK_MODEL did it"]
tier = 3
cost = 15
keywords = ["rust", "typescript", "docs", "large-context"]

[model.broken]
command = ["sh", "-c", "echo no such agent >&2; exit 3"]
"#;

struct Fleet {
    name: String,
    config: PathBuf,
    child: Child,
}

impl Fleet {
    fn up(e: &Env, name: &str, pools: &str) -> Self {
        std::fs::write(e.home().join("models.toml"), MODELS).unwrap();
        let config = e.home().join(format!("{name}.toml"));
        std::fs::write(
            &config,
            format!(
                "[fleet]\nname = \"{name}\"\nmodels = \"models.toml\"\nmax_workers = 20\n{pools}"
            ),
        )
        .unwrap();
        let log = std::fs::File::create(e.home().join(format!("{name}.log"))).unwrap();
        let child = e
            .cmd()
            .args(["workers", "up", "--config"])
            .arg(&config)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let f = Fleet {
            name: name.into(),
            config,
            child,
        };
        let deadline = Instant::now() + Duration::from_secs(15);
        while e.run(&["workers", "status", "--fleet", name]).status.code() != Some(0) {
            assert!(
                Instant::now() < deadline,
                "fleet {name} did not start:\n{}",
                f.log(e)
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        f
    }

    fn log(&self, e: &Env) -> String {
        std::fs::read_to_string(e.home().join(format!("{}.log", self.name))).unwrap_or_default()
    }

    fn status(&self, e: &Env) -> Value {
        e.json(&["workers", "status", "--fleet", &self.name])
    }

    fn pool(&self, e: &Env, pool: &str) -> Value {
        self.status(e)["pools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["pool"] == pool)
            .cloned()
            .unwrap_or(Value::Null)
    }

    fn ctl(&self, e: &Env, args: &[&str]) -> String {
        let mut a = vec!["workers"];
        a.extend_from_slice(args);
        a.extend_from_slice(&["--fleet", &self.name]);
        e.ok(&a)
    }

    fn wait_exit(&mut self, within: Duration) -> i32 {
        wait_exit(&mut self.child, within)
    }
}

impl Drop for Fleet {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn until(what: &str, within: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn create(e: &Env, channel: &str, text: &str, extra: &[&str]) -> String {
    let mut a = vec!["tasks", "create", channel, text];
    a.extend_from_slice(extra);
    e.ok(&a)
}

fn history(e: &Env, id: &str) -> Vec<Value> {
    e.json(&["tasks", "history", id])["events"]
        .as_array()
        .unwrap()
        .clone()
}

fn count(h: &[Value], ty: &str) -> usize {
    h.iter().filter(|e| e["type"] == ty).count()
}

/// The profiles that claimed a task, in order.
fn claimers(h: &[Value]) -> Vec<String> {
    h.iter()
        .filter(|e| e["type"] == "TaskClaimed")
        .map(|e| {
            e["data"]["holder"]["profile"]
                .as_str()
                .unwrap_or("")
                .to_string()
        })
        .collect()
}

fn busy(p: &Value) -> usize {
    p["busy"].as_u64().unwrap_or(0) as usize
}

/// `cork` processes acting as one of this fleet's profiles.
fn fleet_children(prefix: &str) -> Vec<i32> {
    let mut out = vec![];
    for e in std::fs::read_dir("/proc").unwrap().flatten() {
        let Ok(pid) = e.file_name().to_string_lossy().parse::<i32>() else {
            continue;
        };
        let Ok(raw) = std::fs::read(e.path().join("cmdline")) else {
            continue;
        };
        let args: Vec<String> = raw
            .split(|b| *b == 0)
            .map(|a| String::from_utf8_lossy(a).into_owned())
            .collect();
        if args.iter().any(|a| a.starts_with(prefix)) && args.iter().any(|a| a == "run") {
            out.push(pid);
        }
    }
    out
}

#[test]
fn a_killed_worker_is_replaced_and_its_task_completes_later() {
    // 16.8 item 1.
    let e = Env::new();
    let f = Fleet::up(
        &e,
        "r",
        "[pool.p]\nmodel = \"haiku\"\nselect = \"work/\"\ncount = 3\nlease = \"2s\"\nenv = { FIRST_NAP = \"30\" }\n",
    );
    let ids: Vec<String> = (0..3)
        .map(|i| create(&e, "work", &format!("t{i}"), &[]))
        .collect();
    until("3 busy workers", Duration::from_secs(10), || {
        busy(&f.pool(&e, "p")) == 3
    });
    let p = f.pool(&e, "p");
    let victim = &p["workers"][0];
    let task = victim["task"].as_str().unwrap().to_string();
    unsafe {
        libc::kill(victim["pid"].as_i64().unwrap() as i32, libc::SIGKILL);
    }
    let killed = Instant::now();
    // The replacement is ready for work within 2 s of the kill.
    until("a free replacement", Duration::from_secs(5), || {
        let p = f.pool(&e, "p");
        p["restarts"] == 1
            && p["workers"]
                .as_array()
                .unwrap()
                .iter()
                .any(|w| w["task"].is_null() && w["backing_off"] == false)
    });
    assert!(
        killed.elapsed() < Duration::from_secs(2),
        "took {:?}",
        killed.elapsed()
    );
    // After the lease ends, a later attempt finishes it (no FIRST_NAP then).
    e.wait_state(&task, "completed", Duration::from_secs(15));
    let h = history(&e, &task);
    assert_eq!(count(&h, "TaskClaimed"), 2, "{h:?}");
    assert_eq!(count(&h, "LeaseExpired"), 1);
    assert!(ids.contains(&task));
}

#[test]
fn scale_down_loses_nothing() {
    // 16.8 item 2.
    let e = Env::new();
    let f = Fleet::up(
        &e,
        "s",
        "[pool.fast]\nmodel = \"haiku\"\nselect = \"work/\"\ncount = 5\nenv = { NAP = \"2\" }\n",
    );
    let ids: Vec<String> = (0..5)
        .map(|i| create(&e, "work", &format!("t{i}"), &[]))
        .collect();
    until("5 busy workers", Duration::from_secs(10), || {
        busy(&f.pool(&e, "fast")) == 5
    });
    assert_eq!(f.ctl(&e, &["scale", "fast", "1"]), "pool fast: 5 -> 1");
    let p = f.pool(&e, "fast");
    assert_eq!(p["live"], 1);
    assert_eq!(p["draining"], 4);
    for id in &ids {
        e.wait_state(id, "completed", Duration::from_secs(15));
        let h = history(&e, id);
        assert_eq!(
            count(&h, "TaskReleased") + count(&h, "TaskFailed"),
            0,
            "{h:?}"
        );
        assert_eq!(count(&h, "TaskClaimed"), 1);
    }
    until("4 workers stopped", Duration::from_secs(5), || {
        f.pool(&e, "fast")["workers"].as_array().unwrap().len() == 1
    });
}

#[test]
fn scale_up_puts_new_workers_to_work() {
    // 16.8 item 3.
    let e = Env::new();
    let f = Fleet::up(
        &e,
        "u",
        "[pool.strong]\nmodel = \"opus\"\nselect = \"work/\"\ncount = 1\nmax = 6\nenv = { NAP = \"4\" }\n",
    );
    for i in 0..6 {
        create(&e, "work", &format!("t{i}"), &[]);
    }
    until("1 busy worker", Duration::from_secs(10), || {
        busy(&f.pool(&e, "strong")) == 1
    });
    f.ctl(&e, &["scale", "strong", "+3"]);
    let t = Instant::now();
    until("4 busy workers", Duration::from_secs(5), || {
        busy(&f.pool(&e, "strong")) == 4
    });
    assert!(
        t.elapsed() < Duration::from_secs(2),
        "took {:?}",
        t.elapsed()
    );
    // Outside min and max is refused.
    let o = e.run(&["workers", "scale", "strong", "+3", "--fleet", "u"]);
    assert_eq!(o.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&o.stderr).contains("max 6"));
    e.run(&["workers", "scale", "strong", "-9", "--fleet", "u"]);
}

#[test]
fn routing_by_keyword_never_breaks_coverage_and_keeps_fifo() {
    // 16.8 items 4 and 6.
    let e = Env::new();
    let f = Fleet::up(
        &e,
        "k",
        "[pool.fast]\nmodel = \"haiku\"\nselect = \"work/\"\ncount = 2\n\
         [pool.mid]\nmodel = \"sonnet\"\nselect = \"work/\"\ncount = 1\n\
         [pool.strong]\nmodel = \"opus\"\nselect = \"work/\"\ncount = 1\n",
    );
    let covers: BTreeMap<&str, Vec<&str>> = [
        ("fast", vec!["typescript", "docs", "tier-1"]),
        (
            "mid",
            vec![
                "rust",
                "typescript",
                "docs",
                "large-context",
                "tier-1",
                "tier-2",
            ],
        ),
        (
            "strong",
            vec![
                "rust",
                "typescript",
                "docs",
                "large-context",
                "tier-1",
                "tier-2",
                "tier-3",
            ],
        ),
    ]
    .into();
    let menu: [&[&str]; 7] = [
        &[],
        &["docs"],
        &["ts"],
        &["rust"],
        &["rust", "large-context"],
        &["docs", "tier-3"],
        &["tier-2"],
    ];
    let mut tasks = vec![];
    for i in 0..28 {
        let needs = menu[i % menu.len()];
        let mut extra = vec![];
        let joined = needs.join(",");
        if !needs.is_empty() {
            extra = vec!["--needs", &joined];
        }
        tasks.push((create(&e, "work/app", &format!("t{i}"), &extra), needs));
    }
    // One with a raw label outside the vocabulary: listed as unmatched.
    let raw = create(&e, "work/app", "raw", &["--label", "needs.typscript=yes"]);
    until("unmatched listed", Duration::from_secs(10), || {
        f.status(&e)["unmatched"]
            .as_array()
            .unwrap()
            .iter()
            .any(|u| u["task"] == raw.as_str() && u["unknown"][0] == "typscript")
    });
    let mut claimed_by: BTreeMap<String, Vec<(u64, usize)>> = BTreeMap::new();
    for (n, (id, needs)) in tasks.iter().enumerate() {
        e.wait_state(id, "completed", Duration::from_secs(30));
        let h = history(&e, id);
        let who = claimers(&h);
        assert_eq!(who.len(), 1, "{id}: {who:?}");
        let pool = who[0].split('-').nth(1).unwrap().to_string();
        for k in needs.iter() {
            let k = if *k == "ts" { "typescript" } else { k };
            assert!(
                covers[pool.as_str()].contains(&k),
                "{pool} took a task needing {k}"
            );
        }
        let at = h.iter().find(|e| e["type"] == "TaskClaimed").unwrap()["position"]
            .as_u64()
            .unwrap();
        claimed_by.entry(pool).or_default().push((at, n));
    }
    // Within each pool, tasks were claimed oldest first.
    for (pool, mut v) in claimed_by {
        v.sort();
        let order: Vec<usize> = v.iter().map(|(_, n)| *n).collect();
        let mut sorted = order.clone();
        sorted.sort();
        assert_eq!(order, sorted, "pool {pool} took tasks out of order");
    }
    assert_eq!(e.state(&raw), "ready");
    let text = e.ok(&["workers", "status", "--fleet", "k"]);
    assert!(
        text.contains("unmatched: 1 ready tasks no pool covers"),
        "{text}"
    );
}

#[test]
fn two_fleets_share_a_board_without_double_work() {
    // 16.8 item 5.
    let e = Env::new();
    let pools =
        "[pool.p]\nmodel = \"haiku\"\nselect = \"work/\"\ncount = 4\nenv = { NAP = \"0\" }\n";
    let _a = Fleet::up(&e, "a", pools);
    let _b = Fleet::up(&e, "b", pools);
    let ids: Vec<String> = (0..40)
        .map(|i| create(&e, "work", &format!("t{i}"), &[]))
        .collect();
    for id in &ids {
        e.wait_state(id, "completed", Duration::from_secs(60));
        let h = history(&e, id);
        assert_eq!(count(&h, "TaskClaimed"), 1, "{id}: {h:?}");
        assert_eq!(count(&h, "TaskCompleted"), 1);
    }
}

#[test]
fn a_crash_loop_pauses_only_its_pool() {
    // 16.8 item 9.
    let e = Env::new();
    let f = Fleet::up(
        &e,
        "c",
        "[pool.bad]\nmodel = \"broken\"\nselect = \"bad\"\ncount = 1\n\
         [pool.good]\nmodel = \"haiku\"\nselect = \"good\"\ncount = 1\n",
    );
    for i in 0..6 {
        create(&e, "bad", &format!("b{i}"), &["--max-attempts", "10"]);
    }
    until("the bad pool paused", Duration::from_secs(40), || {
        f.pool(&e, "bad")["paused"] == true
    });
    let p = f.pool(&e, "bad");
    assert_eq!(p["restarts"], 5);
    assert!(p["last_error"].as_str().unwrap().contains("exit 8"), "{p}");
    // The good pool keeps working.
    let g = create(&e, "good", "fine", &[]);
    e.wait_state(&g, "completed", Duration::from_secs(10));
    // One message names the pool.
    until("the fleet message", Duration::from_secs(5), || {
        let page = e.json(&["read", "cork/fleet"]);
        page["messages"].as_array().unwrap().len() == 1
            && page["messages"][0]["text"]
                .as_str()
                .unwrap()
                .contains("pool bad is paused")
    });
    // Scaling resumes it.
    f.ctl(&e, &["scale", "bad", "1"]);
    assert_eq!(f.pool(&e, "bad")["paused"], false);
}

#[test]
fn supervisor_death_takes_its_children_and_frees_their_tasks() {
    // 16.8 item 10.
    let e = Env::new();
    let mut f = Fleet::up(
        &e,
        "d",
        "[pool.p]\nmodel = \"haiku\"\nselect = \"work/\"\ncount = 2\nlease = \"5s\"\nenv = { NAP = \"60\" }\n",
    );
    let ids: Vec<String> = (0..2)
        .map(|i| create(&e, "work", &format!("t{i}"), &[]))
        .collect();
    until("2 busy workers", Duration::from_secs(10), || {
        busy(&f.pool(&e, "p")) == 2
    });
    assert_eq!(fleet_children("d-p-").len(), 2);
    unsafe {
        libc::kill(f.child.id() as i32, libc::SIGKILL);
    }
    f.wait_exit(Duration::from_secs(5));
    let t = Instant::now();
    until("no children left", Duration::from_secs(3), || {
        fleet_children("d-p-").is_empty()
    });
    assert!(
        t.elapsed() < Duration::from_secs(1),
        "children outlived it by {:?}",
        t.elapsed()
    );
    for id in &ids {
        e.wait_state(id, "ready", Duration::from_secs(6));
    }
}

#[test]
fn reload_moves_a_pool_to_a_new_model() {
    // 16.8 item 11.
    let e = Env::new();
    let f = Fleet::up(
        &e,
        "l",
        "[pool.p]\nmodel = \"haiku\"\nselect = \"work/\"\ncount = 1\nenv = { NAP = \"2\" }\n",
    );
    let first = create(&e, "work", "first", &[]);
    until("busy", Duration::from_secs(10), || {
        busy(&f.pool(&e, "p")) == 1
    });
    let cfg = std::fs::read_to_string(&f.config)
        .unwrap()
        .replace("haiku", "sonnet");
    std::fs::write(&f.config, cfg).unwrap();
    let msg = f.ctl(&e, &["reload"]);
    assert!(msg.contains("p: new workers (sonnet)"), "{msg}");
    let second = create(&e, "work", "second", &[]);
    e.wait_state(&first, "completed", Duration::from_secs(10));
    e.wait_state(&second, "completed", Duration::from_secs(10));
    let r1 = e.json(&["tasks", "show", &first])["task"]["result"].clone();
    let r2 = e.json(&["tasks", "show", &second])["task"]["result"].clone();
    assert_eq!(
        r1.as_str().unwrap().trim(),
        "haiku did it",
        "in progress finishes on the old model"
    );
    assert_eq!(r2.as_str().unwrap().trim(), "sonnet did it");
    // A broken config is refused and the running one kept.
    std::fs::write(&f.config, "[fleet]\nname = \"l\"\nmodels = \"models.toml\"\n[pool.p]\nmodel = \"gpt\"\nselect = \"w\"\n").unwrap();
    let o = e.run(&["workers", "reload", "--fleet", "l"]);
    assert_eq!(o.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&o.stderr).contains("kept the running config"));
}

#[test]
fn down_drains_and_down_now_releases() {
    let e = Env::new();
    let mut f = Fleet::up(
        &e,
        "n",
        "[pool.p]\nmodel = \"haiku\"\nselect = \"work/\"\ncount = 1\nenv = { NAP = \"1\" }\n",
    );
    let a = create(&e, "work", "a", &[]);
    until("busy", Duration::from_secs(10), || {
        busy(&f.pool(&e, "p")) == 1
    });
    f.ctl(&e, &["down"]);
    assert_eq!(f.wait_exit(Duration::from_secs(10)), 0);
    assert_eq!(e.state(&a), "completed", "drain lets the task finish");

    let mut g = Fleet::up(
        &e,
        "m",
        "[pool.p]\nmodel = \"haiku\"\nselect = \"work/\"\ncount = 1\nenv = { NAP = \"60\" }\n",
    );
    let b = create(&e, "work", "b", &[]);
    e.wait_state(&b, "claimed", Duration::from_secs(10));
    g.ctl(&e, &["down", "--now"]);
    assert_eq!(g.wait_exit(Duration::from_secs(15)), 0);
    assert_eq!(e.state(&b), "ready");
    let h = history(&e, &b);
    assert_eq!(count(&h, "TaskReleased"), 1);
    // A second fleet with the same name refuses while the first runs.
    let _x = Fleet::up(&e, "x", "");
    let o = e
        .cmd()
        .args(["workers", "up", "--config"])
        .arg(e.home().join("x.toml"))
        .stdout(Stdio::null())
        .output()
        .unwrap();
    assert_ne!(o.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&o.stderr).contains("already running"));
}

#[test]
fn autoscale_grows_with_the_queue_and_shrinks_when_idle() {
    // 16.5: opt-in per pool, within min and max.
    let e = Env::new();
    let f = Fleet::up(
        &e,
        "a",
        "[pool.p]\nmodel = \"haiku\"\nselect = \"work/\"\ncount = 1\nmin = 1\nmax = 4\nenv = { NAP = \"1\" }\n\
         [pool.p.autoscale]\ntarget_ready_per_worker = 1\nidle_after = \"1s\"\n",
    );
    let ids: Vec<String> = (0..12)
        .map(|i| create(&e, "work", &format!("t{i}"), &[]))
        .collect();
    until(
        "the pool to grow to its max",
        Duration::from_secs(20),
        || f.pool(&e, "p")["target"] == 4,
    );
    for id in &ids {
        e.wait_state(id, "completed", Duration::from_secs(30));
    }
    until(
        "the pool to shrink to its min",
        Duration::from_secs(20),
        || f.pool(&e, "p")["target"] == 1,
    );
}
