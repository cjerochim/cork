//! End-to-end tests of the real `cork` binary (HANDOFF 15): auto-start,
//! claims across profiles, a worker killed with `kill -9`, the retry limit,
//! a daemon restart under running wrappers, export and import, and rebuild.

use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_cork");

struct Env {
    dir: tempfile::TempDir,
}

impl Env {
    fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
    }

    fn home(&self) -> &Path {
        self.dir.path()
    }

    fn db(&self) -> PathBuf {
        self.home().join("cork.db")
    }

    fn cmd(&self) -> Command {
        let mut c = Command::new(BIN);
        c.env("CORK_HOME", self.home())
            .env("CORK_USER", "tester")
            .env("CORK_PORT", "0")
            .env("PATH", path_with_cork())
            .env_remove("CORK_PROFILE")
            .env_remove("CORK_CLAIM")
            .env_remove("CORK_TASK")
            .env_remove("CORK_URL")
            .env_remove("CORK_TOKEN")
            .stdin(Stdio::null());
        c
    }

    fn run(&self, args: &[&str]) -> Output {
        self.cmd().args(args).output().unwrap()
    }

    /// Run, expect `code`, return stdout.
    fn ok(&self, args: &[&str]) -> String {
        self.expect(0, args)
    }

    fn expect(&self, code: i32, args: &[&str]) -> String {
        let o = self.run(args);
        assert_eq!(
            o.status.code(),
            Some(code),
            "cork {args:?}\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        );
        String::from_utf8(o.stdout).unwrap().trim().to_string()
    }

    fn json(&self, args: &[&str]) -> Value {
        let mut a = vec!["--json"];
        a.extend_from_slice(args);
        serde_json::from_str(&self.ok(&a)).unwrap()
    }

    fn spawn(&self, args: &[&str]) -> Child {
        self.cmd()
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    }

    fn state(&self, id: &str) -> String {
        self.json(&["tasks", "show", id])["task"]["state"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn wait_state(&self, id: &str, want: &str, within: Duration) {
        let deadline = Instant::now() + within;
        loop {
            let s = self.state(id);
            if s == want {
                return;
            }
            assert!(Instant::now() < deadline, "task {id} is {s}, wanted {want}");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Daemons serving this env's database.
    fn daemons(&self) -> Vec<i32> {
        daemons_for(&self.db())
    }

    fn stop_daemons(&self, signal: i32) {
        for pid in self.daemons() {
            unsafe {
                libc::kill(pid, signal);
            }
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.daemons().is_empty() {
            assert!(Instant::now() < deadline, "daemon did not stop");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        for pid in self.daemons() {
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
    }
}

fn path_with_cork() -> String {
    let dir = Path::new(BIN).parent().unwrap().display().to_string();
    format!("{dir}:{}", std::env::var("PATH").unwrap_or_default())
}

fn daemons_for(db: &Path) -> Vec<i32> {
    let db = db.display().to_string();
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
        // A zombie has an empty cmdline, so it does not count.
        if args.iter().any(|a| a == "serve") && args.iter().any(|a| a == &db) {
            out.push(pid);
        }
    }
    out
}

fn wait_file(p: &Path, within: Duration) -> String {
    let deadline = Instant::now() + within;
    loop {
        if let Ok(s) = std::fs::read_to_string(p)
            && !s.trim().is_empty()
        {
            return s.trim().to_string();
        }
        assert!(Instant::now() < deadline, "{} never appeared", p.display());
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_exit(c: &mut Child, within: Duration) -> i32 {
    let deadline = Instant::now() + within;
    loop {
        if let Some(s) = c.try_wait().unwrap() {
            return s.code().unwrap_or(-1);
        }
        if Instant::now() > deadline {
            let _ = c.kill();
            panic!("process did not exit within {within:?}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn post_read_and_claims_are_per_profile() {
    let e = Env::new();
    let id = e.ok(&["post", "work", "hello @fixer", "--label", "k=v"]);
    let thread = e.json(&["read", &id]);
    assert_eq!(thread["root"]["text"], "hello @fixer");
    assert_eq!(thread["root"]["labels"]["k"], "v");
    e.ok(&["reply", &id, "a reply"]);
    let page = e.json(&["read", "work"]);
    assert_eq!(page["messages"].as_array().unwrap().len(), 2);

    let task = e.ok(&["tasks", "create", "work", "fix it", "--lease", "1m"]);
    assert_eq!(
        e.ok(&["--as", "a", "tasks", "claim", "--ready", "work/"])
            .lines()
            .next(),
        Some(task.as_str())
    );
    // Nothing else is ready.
    e.expect(3, &["--as", "b", "tasks", "claim", "--ready", "work/"]);
    // Claiming a held task by id is task_claimed.
    e.expect(7, &["--as", "b", "tasks", "claim", &task]);
    // Another profile, or the person, has no claim to send.
    e.expect(6, &["--as", "b", "tasks", "complete", &task, "done"]);
    e.expect(6, &["tasks", "complete", &task, "done"]);
    e.ok(&["--as", "a", "tasks", "renew", &task]);
    e.ok(&["--as", "a", "tasks", "complete", &task, "done"]);
    assert_eq!(e.state(&task), "completed");
    // The claim file is gone once the task is settled.
    e.expect(6, &["--as", "a", "tasks", "complete", &task, "again"]);

    let hist = e.json(&["tasks", "history", &task]);
    let types: Vec<&str> = hist["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["type"].as_str().unwrap())
        .collect();
    assert_eq!(
        types,
        [
            "TaskCreated",
            "TaskClaimed",
            "LeaseRenewed",
            "TaskCompleted"
        ]
    );
    // `--at` folds the stream up to a position.
    let created_at = hist["events"][0]["position"].as_u64().unwrap().to_string();
    assert_eq!(
        e.json(&["tasks", "show", &task, "--at", &created_at])["task"]["state"],
        "ready"
    );
}

#[test]
fn wait_returns_on_a_matching_post_and_times_out_otherwise() {
    let e = Env::new();
    e.ok(&["whoami"]);
    e.expect(4, &["wait", "work", "--timeout", "300ms"]);
    let mut w = e.spawn(&["--json", "wait", "work/", "--label", "k=v"]);
    std::thread::sleep(Duration::from_millis(500));
    e.ok(&["post", "work/a", "no label"]);
    e.ok(&["post", "work/a", "labelled", "--label", "k=v"]);
    assert_eq!(wait_exit(&mut w, Duration::from_secs(10)), 0);
    let out = w.wait_with_output().unwrap();
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["data"]["text"], "labelled");

    // A lead waits for a task to finish.
    let t = e.ok(&["tasks", "create", "work/plan", "do"]);
    let mut w = e.spawn(&[
        "--json",
        "wait",
        "work/",
        "--label",
        "board.task=completed",
        "--timeout",
        "10s",
    ]);
    std::thread::sleep(Duration::from_millis(500));
    e.ok(&["--as", "w", "tasks", "run", &t, "--", "true"]);
    assert_eq!(wait_exit(&mut w, Duration::from_secs(10)), 0);
    let v: Value = serde_json::from_slice(&w.wait_with_output().unwrap().stdout).unwrap();
    assert_eq!(v["type"], "TaskCompleted");
}

#[test]
fn ten_clis_at_once_start_one_daemon() {
    let e = Env::new();
    let mut kids: Vec<Child> = (0..10).map(|_| e.spawn(&["whoami"])).collect();
    for k in &mut kids {
        assert_eq!(wait_exit(k, Duration::from_secs(20)), 0);
    }
    assert_eq!(e.daemons().len(), 1, "exactly one daemon");
    // A second daemon on the same database exits at once.
    let o = e.run(&["serve", "--db", e.db().to_str().unwrap()]);
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&o.stderr).contains("already serving"));
    // So does a rebuild of the live database.
    e.expect(1, &["debug", "rebuild", "--db", e.db().to_str().unwrap()]);
}

#[test]
fn run_completes_with_the_result_file_and_fails_on_exit() {
    let e = Env::new();
    let ok = e.ok(&["tasks", "create", "work", "do it"]);
    let out = e.ok(&[
        "--as",
        "w",
        "tasks",
        "run",
        &ok,
        "--",
        "sh",
        "-c",
        "test \"$CORK_ATTEMPT\" = 1 && cork progress step1 && echo from-file > \"$CORK_RESULT\"",
    ]);
    assert!(!out.contains("from-file"));
    assert_eq!(
        e.json(&["tasks", "show", &ok])["task"]["result"],
        "from-file\n"
    );

    let bad = e.ok(&["tasks", "create", "work", "fail it"]);
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
            "echo oops >&2; exit 3",
        ],
    );
    let t = e.json(&["tasks", "show", &bad])["task"].clone();
    assert_eq!(t["state"], "failed", "an explicit fail is final (6.5)");
    assert!(
        t["reason"]
            .as_str()
            .unwrap()
            .starts_with("command exited with status 3\noops")
    );

    // Blocked from inside: the wrapper settles nothing and exits 9.
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
            "cork tasks block \"$CORK_TASK\" 'which branch?' && sleep 1",
        ],
    );
    assert_eq!(e.state(&q), "blocked");

    // --max-runtime fails the task.
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
            "300ms",
            "--",
            "sleep",
            "30",
        ],
    );
}

#[test]
fn kill_9_worker_then_another_finishes_and_the_late_complete_is_claim_lost() {
    let e = Env::new();
    let task = e.ok(&["tasks", "create", "work", "crash me", "--lease", "1s"]);
    let claim_file = e.home().join("claim");
    let pid_file = e.home().join("pid");
    let script = format!(
        "echo $$ > {}; echo $CORK_CLAIM > {}; sleep 60",
        pid_file.display(),
        claim_file.display()
    );
    let mut w1 = e.spawn(&[
        "--as", "w1", "tasks", "run", &task, "--", "sh", "-c", &script,
    ]);
    let claim = wait_file(&claim_file, Duration::from_secs(10));
    let sh: i32 = wait_file(&pid_file, Duration::from_secs(10))
        .parse()
        .unwrap();
    // The worker and its command die at once.
    unsafe {
        libc::kill(w1.id() as i32, libc::SIGKILL);
        libc::kill(-sh, libc::SIGKILL);
    }
    let _ = w1.wait();

    // Another worker takes it once the lease ends.
    let started = Instant::now();
    e.ok(&[
        "--as", "w2", "tasks", "run", "--ready", "work", "--wait", "10s", "--", "true",
    ]);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(e.state(&task), "completed");
    e.expect(
        5,
        &[
            "--as", "w1", "--claim", &claim, "tasks", "complete", &task, "late",
        ],
    );
}

#[test]
fn a_worker_that_kills_itself_uses_up_the_attempts() {
    let e = Env::new();
    let task = e.ok(&[
        "tasks",
        "create",
        "work",
        "poison",
        "--lease",
        "300ms",
        "--max-attempts",
        "2",
    ]);
    for _ in 0..2 {
        e.wait_state(&task, "ready", Duration::from_secs(5));
        let o = e.run(&[
            "--as",
            "w",
            "tasks",
            "run",
            &task,
            "--",
            "sh",
            "-c",
            "kill -9 $PPID",
        ]);
        assert_eq!(o.status.code(), None, "the wrapper died by signal");
    }
    e.wait_state(&task, "failed", Duration::from_secs(5));
    let t = e.json(&["tasks", "show", &task])["task"].clone();
    assert_eq!(t["attempts"], 2);
    let hist = e.json(&["tasks", "history", &task]);
    let types: Vec<&str> = hist["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["type"].as_str().unwrap())
        .collect();
    assert_eq!(types[types.len() - 2..], ["LeaseExpired", "TaskFailed"]);
}

#[test]
fn ten_wrappers_survive_a_daemon_restart() {
    let e = Env::new();
    let ids: Vec<String> = (0..10)
        .map(|i| {
            e.ok(&[
                "tasks",
                "create",
                "work",
                &format!("task {i}"),
                "--lease",
                "4s",
            ])
        })
        .collect();
    let mut kids: Vec<Child> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            e.spawn(&[
                "--as",
                &format!("w{i}"),
                "tasks",
                "run",
                id,
                "--",
                "sleep",
                "3",
            ])
        })
        .collect();
    std::thread::sleep(Duration::from_millis(800));
    for id in &ids {
        assert_eq!(e.state(id), "claimed");
    }
    // Stop the daemon and start a new one (on a new port) a second later.
    e.stop_daemons(libc::SIGTERM);
    std::thread::sleep(Duration::from_secs(1));
    e.ok(&["whoami"]);
    for k in &mut kids {
        assert_eq!(wait_exit(k, Duration::from_secs(30)), 0);
    }
    for id in &ids {
        assert_eq!(e.state(id), "completed");
    }
}

#[test]
fn export_then_import_gives_the_same_log_and_rebuild_finds_no_diff() {
    let e = Env::new();
    let m = e.ok(&[
        "post",
        "work",
        "hi @bob",
        "--label",
        "k=v",
        "--data",
        "{\"a\":1}",
    ]);
    e.ok(&["reply", &m, "a reply"]);
    let t = e.ok(&["tasks", "create", "work/sub", "do", "--label", "step=fix"]);
    e.ok(&["--as", "a", "tasks", "claim", &t]);
    e.ok(&["--as", "a", "tasks", "block", &t, "why?"]);
    e.ok(&["tasks", "unblock", &t, "because"]);
    let export = e.home().join("export.jsonl");
    e.ok(&["export", "--out", export.to_str().unwrap()]);
    let original = std::fs::read_to_string(&export).unwrap();
    assert!(original.lines().count() >= 7);

    // Import into a fresh home.
    let e2 = Env::new();
    let mut d = e2.spawn(&[
        "serve",
        "--db",
        e2.db().to_str().unwrap(),
        "--import",
        export.to_str().unwrap(),
    ]);
    wait_file(&e2.home().join("daemon.json"), Duration::from_secs(10));
    let again = e2.ok(&["export"]);
    assert_eq!(again, original.trim());
    assert_eq!(e2.json(&["tasks", "show", &t])["task"]["state"], "ready");
    unsafe {
        libc::kill(d.id() as i32, libc::SIGTERM);
    }
    assert_eq!(wait_exit(&mut d, Duration::from_secs(10)), 0);

    // Rebuild a copy of a stopped database.
    e.stop_daemons(libc::SIGTERM);
    let copy = e.home().join("copy.db");
    std::fs::copy(e.db(), &copy).unwrap();
    for ext in ["db-wal", "db-shm"] {
        let side = e.db().with_extension(ext);
        if side.exists() {
            std::fs::copy(&side, copy.with_extension(ext)).unwrap();
        }
    }
    let out = e.ok(&["debug", "rebuild", "--db", copy.to_str().unwrap()]);
    assert_eq!(out, "projections match the log");

    // debug export with a thread filter has only that thread.
    let lines = e.ok(&["debug", "export", "--thread", &t]);
    assert!(
        lines
            .lines()
            .all(|l| serde_json::from_str::<Value>(l).unwrap()["thread_id"] == t.as_str())
    );
    assert!(lines.lines().count() >= 4);
}

#[test]
fn sigterm_releases_and_a_lost_daemon_exits_11() {
    let e = Env::new();
    let task = e.ok(&["tasks", "create", "work", "stop me", "--lease", "1s"]);
    let mut w = e.spawn(&["--as", "w", "tasks", "run", &task, "--", "sleep", "30"]);
    e.wait_state(&task, "claimed", Duration::from_secs(5));
    unsafe {
        libc::kill(w.id() as i32, libc::SIGTERM);
    }
    assert_eq!(wait_exit(&mut w, Duration::from_secs(15)), 143);
    assert_eq!(e.state(&task), "ready", "released, not failed");

    // The daemon goes away for good: the wrapper stops the command near the lease end.
    let mut w = e.spawn(&["--as", "w", "tasks", "run", &task, "--", "sleep", "30"]);
    e.wait_state(&task, "claimed", Duration::from_secs(5));
    e.stop_daemons(libc::SIGKILL);
    let started = Instant::now();
    assert_eq!(wait_exit(&mut w, Duration::from_secs(15)), 11);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "within one lease"
    );
}
