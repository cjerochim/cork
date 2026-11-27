//! The harness the CLI tests share: a private CORK_HOME per test, the real
//! `cork` binary, and helpers to wait on files, processes and task states.
#![allow(dead_code)]

use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

pub const BIN: &str = env!("CARGO_BIN_EXE_cork");

pub struct Env {
    pub dir: tempfile::TempDir,
}

impl Env {
    pub fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
    }

    pub fn home(&self) -> &Path {
        self.dir.path()
    }

    pub fn db(&self) -> PathBuf {
        self.home().join("cork.db")
    }

    pub fn cmd(&self) -> Command {
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
            .env_remove("CORK_MODELS")
            .env_remove("CORK_WORKERS")
            .stdin(Stdio::null());
        c
    }

    pub fn run(&self, args: &[&str]) -> Output {
        self.cmd().args(args).output().unwrap()
    }

    /// Run, expect `code`, return stdout.
    pub fn ok(&self, args: &[&str]) -> String {
        self.expect(0, args)
    }

    pub fn expect(&self, code: i32, args: &[&str]) -> String {
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

    pub fn json(&self, args: &[&str]) -> Value {
        let mut a = vec!["--json"];
        a.extend_from_slice(args);
        serde_json::from_str(&self.ok(&a)).unwrap()
    }

    pub fn spawn(&self, args: &[&str]) -> Child {
        self.cmd()
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    }

    pub fn state(&self, id: &str) -> String {
        self.json(&["tasks", "show", id])["task"]["state"]
            .as_str()
            .unwrap()
            .to_string()
    }

    pub fn wait_state(&self, id: &str, want: &str, within: Duration) {
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
    pub fn daemons(&self) -> Vec<i32> {
        daemons_for(&self.db())
    }

    pub fn stop_daemons(&self, signal: i32) {
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

pub fn path_with_cork() -> String {
    let dir = Path::new(BIN).parent().unwrap().display().to_string();
    format!("{dir}:{}", std::env::var("PATH").unwrap_or_default())
}

pub fn daemons_for(db: &Path) -> Vec<i32> {
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

pub fn wait_file(p: &Path, within: Duration) -> String {
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

pub fn wait_exit(c: &mut Child, within: Duration) -> i32 {
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
