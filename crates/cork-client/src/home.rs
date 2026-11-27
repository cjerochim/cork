//! The client's files under `CORK_HOME` (default `~/.config/cork`):
//! `daemon.json`, the auto-start lock, and claim files (HANDOFF 8.3, 8.4).

use crate::{Client, ClientError, Result};
use cork_api::API_VERSION;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub const DEFAULT_PORT: u16 = 7420;

pub fn cork_home() -> PathBuf {
    if let Some(h) = std::env::var_os("CORK_HOME").filter(|h| !h.is_empty()) {
        return PathBuf::from(h);
    }
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("cork")
}

/// What `cork serve` writes once it is listening (mode 0600).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonInfo {
    pub url: String,
    pub port: u16,
    pub token: String,
}

pub fn daemon_file(home: &Path) -> PathBuf {
    home.join("daemon.json")
}

pub fn read_daemon(home: &Path) -> Option<DaemonInfo> {
    let s = std::fs::read_to_string(daemon_file(home)).ok()?;
    serde_json::from_str(&s).ok()
}

/// Write a file readable only by its owner, atomically.
pub fn write_private(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension(format!("tmp{}", std::process::id()));
    {
        let mut o = OpenOptions::new();
        o.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            o.mode(0o600);
        }
        let mut f = o.open(&tmp)?;
        std::io::Write::write_all(&mut f, contents)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

/// An exclusive advisory lock on a file, held until dropped.
pub struct FileLock(#[allow(dead_code)] File);

impl FileLock {
    fn open(path: &Path) -> std::io::Result<File> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)
    }

    /// Wait for the lock.
    pub fn acquire(path: &Path) -> std::io::Result<Self> {
        let f = Self::open(path)?;
        flock(&f, false)?;
        Ok(Self(f))
    }

    /// Take the lock now, or `None` if another process holds it.
    pub fn try_acquire(path: &Path) -> std::io::Result<Option<Self>> {
        let f = Self::open(path)?;
        match flock(&f, true) {
            Ok(()) => Ok(Some(Self(f))),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(unix)]
fn flock(f: &File, nonblocking: bool) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let op = libc::LOCK_EX | if nonblocking { libc::LOCK_NB } else { 0 };
    loop {
        if unsafe { libc::flock(f.as_raw_fd(), op) } == 0 {
            return Ok(());
        }
        let e = std::io::Error::last_os_error();
        if e.kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        if e.raw_os_error() == Some(libc::EWOULDBLOCK) {
            return Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, e));
        }
        return Err(e);
    }
}

#[cfg(not(unix))]
fn flock(_f: &File, _nonblocking: bool) -> std::io::Result<()> {
    Ok(())
}

/// How to reach a daemon.
#[derive(Debug, Clone)]
pub struct Connect {
    pub home: PathBuf,
    pub profile: Option<String>,
    /// Start a local daemon if none answers.
    pub auto_start: bool,
}

impl Connect {
    pub fn from_env(profile: Option<String>) -> Self {
        Self {
            home: cork_home(),
            profile,
            auto_start: true,
        }
    }
}

fn remote() -> Option<(String, String)> {
    let url = std::env::var("CORK_URL").ok().filter(|s| !s.is_empty())?;
    let token = std::env::var("CORK_TOKEN").ok().filter(|s| !s.is_empty())?;
    Some((url, token))
}

/// Find a running daemon, starting one if allowed (8.4). Checks the API version.
pub async fn connect(c: &Connect) -> Result<Client> {
    if let Some((url, token)) = remote() {
        let client = Client::new(&url, &token, c.profile.clone());
        check_version(&client).await?;
        return Ok(client);
    }
    if let Some(client) = try_local(c).await {
        check_version(&client).await?;
        return Ok(client);
    }
    if !c.auto_start {
        return Err(ClientError::new(
            "transport",
            "the cork daemon is not running",
        ));
    }
    // Take the start lock, look again, and only then start one.
    let home = c.home.clone();
    let lock = tokio::task::spawn_blocking(move || FileLock::acquire(&home.join("daemon.lock")))
        .await
        .map_err(|e| ClientError::new("internal", e.to_string()))?
        .map_err(|e| ClientError::new("internal", format!("daemon.lock: {e}")))?;
    if let Some(client) = try_local(c).await {
        drop(lock);
        check_version(&client).await?;
        return Ok(client);
    }
    start_daemon(&c.home)?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Some(client) = try_local(c).await {
            drop(lock);
            check_version(&client).await?;
            return Ok(client);
        }
    }
    Err(ClientError::new(
        "transport",
        format!(
            "started `cork serve` but it did not answer within 5 s (see {})",
            c.home.join("serve.log").display()
        ),
    ))
}

/// Re-read `daemon.json` and connect without starting anything. Used by
/// `tasks run` while it holds a task.
pub async fn reconnect(c: &Connect) -> Option<Client> {
    if let Some((url, token)) = remote() {
        return Some(Client::new(&url, &token, c.profile.clone()));
    }
    try_local(c).await
}

async fn try_local(c: &Connect) -> Option<Client> {
    let info = read_daemon(&c.home)?;
    let client = Client::new(&info.url, &info.token, c.profile.clone());
    client.health().await.ok()?;
    Some(client)
}

async fn check_version(client: &Client) -> Result<()> {
    let h = client.health().await?;
    if h.api_version != API_VERSION {
        return Err(ClientError::new(
            "internal",
            format!(
                "the daemon speaks API v{}, this cork speaks v{API_VERSION}",
                h.api_version
            ),
        ));
    }
    if h.server_version != env!("CARGO_PKG_VERSION") {
        eprintln!(
            "cork: warning: daemon is version {}, this cork is {}",
            h.server_version,
            env!("CARGO_PKG_VERSION")
        );
    }
    Ok(())
}

/// Start `cork serve` in the background, detached, logging to `serve.log`.
fn start_daemon(home: &Path) -> Result<()> {
    let exe = std::env::current_exe().map_err(|e| ClientError::new("internal", e.to_string()))?;
    std::fs::create_dir_all(home).map_err(|e| ClientError::new("internal", e.to_string()))?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(home.join("serve.log"))
        .map_err(|e| ClientError::new("internal", e.to_string()))?;
    let mut cmd = std::process::Command::new(exe);
    cmd.arg("serve")
        .arg("--db")
        .arg(home.join("cork.db"))
        .env("CORK_HOME", home)
        .stdin(std::process::Stdio::null())
        .stdout(
            log.try_clone()
                .map_err(|e| ClientError::new("internal", e.to_string()))?,
        )
        .stderr(log);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    cmd.spawn()
        .map_err(|e| ClientError::new("internal", format!("could not start cork serve: {e}")))?;
    Ok(())
}

// ---------- claim files ----------

/// A claim kept between commands (8.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimFile {
    pub task_id: String,
    pub claim_id: String,
}

pub fn claim_path(home: &Path, board_id: &str, handle: &str, task_id: &str) -> PathBuf {
    home.join("claims")
        .join(board_id)
        .join(handle)
        .join(format!("{task_id}.json"))
}

pub fn save_claim(home: &Path, board_id: &str, handle: &str, c: &ClaimFile) -> std::io::Result<()> {
    let body = serde_json::to_vec(c).expect("claim serializes");
    write_private(&claim_path(home, board_id, handle, &c.task_id), &body)
}

pub fn load_claim(home: &Path, board_id: &str, handle: &str, task_id: &str) -> Option<ClaimFile> {
    let s = std::fs::read_to_string(claim_path(home, board_id, handle, task_id)).ok()?;
    serde_json::from_str(&s).ok()
}

pub fn remove_claim(home: &Path, board_id: &str, handle: &str, task_id: &str) {
    let _ = std::fs::remove_file(claim_path(home, board_id, handle, task_id));
}
