//! `cork`: the command line for the board (HANDOFF 8).

mod escalate;
mod run;
mod serve;
mod stats;
mod usage;
mod workers;

use clap::{Args, Parser, Subcommand};
use cork_api::*;
use cork_client::home::{self, Connect};
use cork_client::{Client, ClientError, query};
use cork_core::Labels;
use cork_core::channel::looks_like_ulid;
use serde_json::Value;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(
    name = "cork",
    version,
    about = "A message board and task queue for people and coding agents"
)]
struct Cli {
    /// Act as this agent profile.
    #[arg(long = "as", env = "CORK_PROFILE", global = true)]
    profile: Option<String>,
    /// Print the API response as JSON.
    #[arg(long, global = true)]
    json: bool,
    /// Make a write safe to retry.
    #[arg(long, global = true)]
    idempotency_key: Option<String>,
    /// Use this claim instead of the stored one.
    #[arg(long = "claim", env = "CORK_CLAIM", global = true)]
    claim_id: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the daemon.
    Serve {
        /// 0 picks a free port; clients find it in daemon.json.
        #[arg(long, env = "CORK_PORT", default_value_t = home::DEFAULT_PORT)]
        port: u16,
        #[arg(long)]
        db: Option<PathBuf>,
        #[arg(long, value_parser = ["full", "normal"], default_value = "full")]
        sync: String,
        /// Load an export into an empty database before serving.
        #[arg(long)]
        import: Option<PathBuf>,
        /// Leave channel names out of `/metrics`, for boards whose channel
        /// names are private.
        #[arg(long)]
        no_metrics_channels: bool,
    },
    /// Show who the daemon thinks you are.
    Whoami,
    /// List channels.
    Channels,
    /// Start a thread in a channel.
    Post {
        channel: String,
        text: String,
        #[command(flatten)]
        m: MsgArgs,
    },
    /// Reply in a thread.
    Reply {
        id: String,
        text: String,
        #[command(flatten)]
        m: MsgArgs,
    },
    /// Reply on $CORK_TASK, from inside `tasks run`.
    Progress {
        text: String,
        #[arg(long)]
        data: Option<String>,
    },
    /// Read a thread, or the latest messages in a channel.
    Read {
        target: String,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Wait for the next matching message.
    Wait {
        target: String,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long)]
        mentions: bool,
        /// Wake on these event types, such as TaskCompleted, instead of messages.
        #[arg(long = "type")]
        types: Vec<String>,
        #[arg(long, value_parser = parse_duration, default_value = "10m")]
        timeout: Duration,
    },
    /// Follow events as they happen.
    Tail {
        target: String,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long = "type")]
        types: Vec<String>,
        #[arg(long, conflicts_with = "from_start")]
        after: Option<u64>,
        #[arg(long)]
        from_start: bool,
    },
    /// Tasks: create, claim, settle, run.
    Tasks {
        #[command(subcommand)]
        cmd: TaskCmd,
    },
    /// Write the whole log as JSONL, importable with `serve --import`.
    Export {
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Tools for debugging.
    Debug {
        #[command(subcommand)]
        cmd: DebugCmd,
    },
    /// Worker fleets: keywords and matching (HANDOFF 16).
    Workers {
        #[command(subcommand)]
        cmd: WorkersCmd,
    },
    /// Flow, usage and cost per successful task, from the log (HANDOFF 18.3).
    Stats {
        /// Only tasks created in this last stretch, such as 7d.
        #[arg(long, value_parser = parse_duration)]
        since: Option<Duration>,
        /// Only these channels, such as work/.
        #[arg(long)]
        channel: Option<String>,
        /// Only tasks with this label (k=v); repeatable.
        #[arg(long = "label")]
        labels: Vec<String>,
        /// Only this feature task and its children (`parent=<id>`).
        #[arg(long)]
        feature: Option<String>,
        /// model, pool, profile, channel, label:<key> or day.
        #[arg(long, default_value = "model")]
        by: cork_stats::By,
        #[arg(long, conflicts_with = "json")]
        csv: bool,
        /// Also show failure reasons, grouped by their first line.
        #[arg(long)]
        reasons: bool,
        /// Read an export instead of asking the daemon.
        #[arg(long, value_name = "EXPORT")]
        file: Option<PathBuf>,
        #[command(flatten)]
        files: FleetFiles,
    },
    /// Manage the daemon's token.
    Token {
        #[command(subcommand)]
        cmd: TokenCmd,
    },
}

/// Where the model catalog and fleet config come from.
#[derive(Args, Clone)]
struct FleetFiles {
    /// The model catalog (models.toml, or .json).
    #[arg(long, value_name = "FILE")]
    models: Option<PathBuf>,
    /// The fleet config (workers.toml, or .json).
    #[arg(long, value_name = "FILE")]
    config: Option<PathBuf>,
}

/// Which running fleet a control command talks to.
#[derive(Args, Clone)]
struct FleetPick {
    /// The fleet's name. Defaults to the one in the fleet config, or the
    /// only fleet running.
    #[arg(long)]
    fleet: Option<String>,
    /// The fleet config, to read the fleet's name from.
    #[arg(long, value_name = "FILE")]
    config: Option<PathBuf>,
}

#[derive(Subcommand)]
enum WorkersCmd {
    /// Start the fleet: foreground by default.
    Up {
        /// The fleet config (workers.toml, or .json).
        #[arg(long, value_name = "FILE")]
        config: Option<PathBuf>,
        /// Run in the background, logging to <home>/workers/<fleet>.log.
        #[arg(long)]
        detach: bool,
    },
    /// Pools, sizes, busy and idle workers, restarts and unmatched tasks.
    Status {
        #[command(flatten)]
        pick: FleetPick,
    },
    /// The supervisor's metrics, in Prometheus text format.
    Metrics {
        #[command(flatten)]
        pick: FleetPick,
    },
    /// Resize a running pool: n, +n or -n, within its min and max.
    Scale {
        pool: String,
        #[arg(allow_hyphen_values = true)]
        count: String,
        #[command(flatten)]
        pick: FleetPick,
    },
    /// Let the pool's workers finish their tasks, then stop them.
    Drain {
        pool: String,
        #[command(flatten)]
        pick: FleetPick,
    },
    /// Re-read both files and apply count, model and command changes.
    Reload {
        #[command(flatten)]
        pick: FleetPick,
    },
    /// Drain everything and stop; with --now, release every task and stop.
    Down {
        #[arg(long)]
        now: bool,
        #[command(flatten)]
        pick: FleetPick,
    },
    /// The keyword vocabulary, its aliases, and which models cover each one.
    Keywords {
        #[command(flatten)]
        files: FleetFiles,
    },
    /// Dry run: which models and pools could take a task, and why not.
    Match {
        task_id: Option<String>,
        /// Ask about a task before it exists.
        #[arg(long, value_delimiter = ',', conflicts_with = "task_id")]
        needs: Vec<String>,
        #[command(flatten)]
        files: FleetFiles,
    },
}

#[derive(Args)]
struct MsgArgs {
    #[arg(long = "label")]
    labels: Vec<String>,
    /// A JSON value to attach.
    #[arg(long)]
    data: Option<String>,
}

#[derive(Subcommand)]
enum TaskCmd {
    List {
        selector: Option<String>,
        #[arg(long)]
        state: Option<String>,
        #[arg(long = "label")]
        labels: Vec<String>,
    },
    Show {
        id: String,
        /// Show the task as it was at this position.
        #[arg(long)]
        at: Option<u64>,
    },
    History {
        id: String,
    },
    Create {
        channel: String,
        text: String,
        #[command(flatten)]
        m: MsgArgs,
        #[arg(long, value_parser = parse_duration)]
        lease: Option<Duration>,
        #[arg(long)]
        max_attempts: Option<u32>,
        /// Only this handle may claim it.
        #[arg(long = "for")]
        assignee: Option<String>,
        /// Keywords the task needs, checked against the model catalog:
        /// `--needs rust,docs` adds `needs.rust=yes` and `needs.docs=yes`.
        #[arg(long, value_delimiter = ',')]
        needs: Vec<String>,
        /// The model catalog for --needs.
        #[arg(long, value_name = "FILE")]
        models: Option<PathBuf>,
    },
    Claim {
        id: Option<String>,
        #[command(flatten)]
        r: ReadyArgs,
        #[arg(long, value_parser = parse_duration)]
        lease: Option<Duration>,
    },
    Renew {
        id: String,
    },
    Complete {
        id: String,
        result: Option<String>,
    },
    Fail {
        id: String,
        reason: String,
    },
    Release {
        id: String,
    },
    Block {
        id: String,
        question: String,
    },
    Unblock {
        id: String,
        note: Option<String>,
    },
    Cancel {
        id: String,
        reason: Option<String>,
    },
    Reopen {
        id: String,
    },
    /// Retry a struggling task one tier up, from its earlier branch (16.7).
    Escalate {
        id: String,
        /// More keywords the retry needs, such as large-context.
        #[arg(long, value_delimiter = ',')]
        needs: Vec<String>,
        /// Refuse past this many escalations of one task.
        #[arg(long, default_value_t = cork_fleet::escalation::DEFAULT_MAX_ESCALATIONS)]
        max: u32,
        #[command(flatten)]
        files: FleetFiles,
    },
    /// Claim a task, run a command, and settle from its exit.
    Run {
        id: Option<String>,
        #[command(flatten)]
        r: ReadyArgs,
        #[arg(long, value_parser = parse_duration)]
        lease: Option<Duration>,
        /// Take the next task after each one.
        #[arg(long = "loop")]
        looping: bool,
        #[arg(long, value_parser = parse_duration)]
        max_runtime: Option<Duration>,
        /// Read the agent's usage from its output: `claude-json` for Claude
        /// Code with `--output-format json`. Without it, the command may
        /// still write usage to $CORK_USAGE.
        #[arg(long, value_name = "PARSER")]
        usage: Option<usage::Parser>,
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },
}

#[derive(Args)]
struct ReadyArgs {
    /// Claim the oldest ready task matching this selector.
    #[arg(long)]
    ready: Option<String>,
    #[arg(long = "label")]
    labels: Vec<String>,
    /// Whose tasks to take; `*` for anyone's.
    #[arg(long)]
    from: Option<String>,
    #[arg(long, value_parser = parse_duration)]
    wait: Option<Duration>,
}

#[derive(Subcommand)]
enum DebugCmd {
    /// Matching events as JSONL, for bug reports. Not importable.
    Export {
        #[arg(long, required_unless_present = "thread")]
        channel: Option<String>,
        #[arg(long)]
        thread: Option<String>,
        #[arg(long, default_value_t = 0)]
        after: u64,
    },
    /// Rebuild projections from the log in a copy of a database and diff them.
    Rebuild {
        #[arg(long)]
        db: PathBuf,
    },
}

#[derive(Subcommand)]
enum TokenCmd {
    Rotate,
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let split = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let (n, unit) = s.split_at(split);
    let n: f64 = n
        .parse()
        .map_err(|_| format!("bad duration `{s}`: use 500ms, 30s, 5m or 2h"))?;
    let ms = match unit {
        "ms" => n,
        "" | "s" => n * 1_000.0,
        "m" => n * 60_000.0,
        "h" => n * 3_600_000.0,
        "d" => n * 86_400_000.0,
        _ => return Err(format!("bad duration `{s}`: use 500ms, 30s, 5m, 2h or 7d")),
    };
    Ok(Duration::from_millis(ms as u64))
}

fn usage(msg: impl Into<String>) -> ClientError {
    ClientError::new("usage", msg)
}

fn parse_labels(pairs: &[String]) -> Result<Labels, ClientError> {
    let mut out = Labels::new();
    for p in pairs {
        let (k, v) = p
            .split_once('=')
            .ok_or_else(|| usage(format!("--label wants k=v, got `{p}`")))?;
        out.insert(k.to_string(), v.to_string());
    }
    Ok(out)
}

fn parse_data(d: &Option<String>) -> Result<Option<Value>, ClientError> {
    d.as_ref()
        .map(|s| serde_json::from_str(s).map_err(|e| usage(format!("--data is not JSON: {e}"))))
        .transpose()
}

fn message(text: &str, m: &MsgArgs) -> Result<PostMessage, ClientError> {
    Ok(PostMessage {
        text: text.to_string(),
        labels: parse_labels(&m.labels)?,
        data: parse_data(&m.data)?,
    })
}

fn ms(d: Option<Duration>) -> Option<i64> {
    d.map(|d| d.as_millis() as i64)
}

/// `thread=<id>` for an id, else `channel=<selector>`.
fn target_param(t: &str) -> (&'static str, String) {
    if looks_like_ulid(t) {
        ("thread", t.to_string())
    } else {
        ("channel", t.to_string())
    }
}

fn main() {
    let cli = Cli::parse();
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let code = rt.block_on(dispatch(cli));
    // Do not wait for stray blocking tasks (such as a stdin read).
    rt.shutdown_timeout(Duration::from_millis(100));
    std::process::exit(code);
}

async fn dispatch(cli: Cli) -> i32 {
    match main_inner(cli).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("cork: {e}");
            e.exit_code()
        }
    }
}

fn internal(e: impl std::fmt::Display) -> ClientError {
    ClientError::new("internal", e.to_string())
}

async fn main_inner(cli: Cli) -> Result<i32, ClientError> {
    let home = home::cork_home();
    // Commands that do not talk to a daemon.
    match &cli.cmd {
        Cmd::Serve {
            port,
            db,
            sync,
            import,
            no_metrics_channels,
        } => {
            serve::serve(serve::ServeOpts {
                home: home.clone(),
                port: *port,
                db: db.clone().unwrap_or_else(|| home.join("cork.db")),
                full_sync: sync == "full",
                import: import.clone(),
                metrics_channels: !*no_metrics_channels,
            })
            .await
            .map_err(|e| ClientError::new("internal", e))?;
            return Ok(0);
        }
        Cmd::Token {
            cmd: TokenCmd::Rotate,
        } => {
            serve::rotate(&home).map_err(|e| ClientError::new("internal", e))?;
            println!(
                "rotated the token in {}",
                home::daemon_file(&home).display()
            );
            return Ok(0);
        }
        Cmd::Debug {
            cmd: DebugCmd::Rebuild { db },
        } => {
            let diff = serve::rebuild(db)
                .await
                .map_err(|e| ClientError::new("internal", e))?;
            if diff.is_empty() {
                println!("projections match the log");
                return Ok(0);
            }
            for l in &diff {
                println!("{l}");
            }
            return Ok(1);
        }
        Cmd::Workers {
            cmd: WorkersCmd::Up { config, detach },
        } => return workers::up(&home, config.as_deref(), *detach).await,
        Cmd::Workers {
            cmd:
                c @ (WorkersCmd::Status { .. }
                | WorkersCmd::Metrics { .. }
                | WorkersCmd::Scale { .. }
                | WorkersCmd::Drain { .. }
                | WorkersCmd::Reload { .. }
                | WorkersCmd::Down { .. }),
        } => {
            let (pick, req) = match c {
                WorkersCmd::Status { pick } => (pick, serde_json::json!({"op": "status"})),
                WorkersCmd::Metrics { pick } => (pick, serde_json::json!({"op": "metrics"})),
                WorkersCmd::Scale { pool, count, pick } => (
                    pick,
                    serde_json::json!({"op": "scale", "pool": pool, "count": count}),
                ),
                WorkersCmd::Drain { pool, pick } => {
                    (pick, serde_json::json!({"op": "drain", "pool": pool}))
                }
                WorkersCmd::Reload { pick } => (pick, serde_json::json!({"op": "reload"})),
                WorkersCmd::Down { now, pick } => {
                    (pick, serde_json::json!({"op": "down", "now": now}))
                }
                _ => unreachable!(),
            };
            return workers::control(
                &home,
                pick.fleet.as_deref(),
                pick.config.as_deref(),
                req,
                cli.json,
            )
            .await;
        }
        Cmd::Workers {
            cmd: WorkersCmd::Keywords { files },
        } => {
            let l = workers::load(&home, files.models.as_deref(), files.config.as_deref())?;
            workers::keywords(&l, cli.json);
            return Ok(0);
        }
        Cmd::Workers {
            cmd:
                WorkersCmd::Match {
                    task_id: None,
                    needs,
                    files,
                },
        } => {
            let l = workers::load(&home, files.models.as_deref(), files.config.as_deref())?;
            return workers::matches(&l, None, None, needs, cli.json).await;
        }
        _ => {}
    }

    if let Cmd::Stats {
        since,
        channel,
        labels,
        feature,
        by,
        csv,
        reasons,
        file,
        files,
    } = &cli.cmd
    {
        let mut ls = vec![];
        for l in labels {
            let (k, v) = l
                .split_once('=')
                .ok_or_else(|| usage(format!("--label wants k=v, not `{l}`")))?;
            ls.push((k.to_string(), v.to_string()));
        }
        let o = stats::StatsOpts {
            since: *since,
            channel: channel.clone(),
            labels: ls,
            feature: feature.clone(),
            by: by.clone(),
            json: cli.json,
            csv: *csv,
            reasons: *reasons,
            file: file.clone(),
            models: files.models.clone(),
            config: files.config.clone(),
        };
        if file.is_some() {
            return stats::stats(&home, None, o).await;
        }
        let conn = Connect::from_env(cli.profile.clone());
        let client = home::connect(&conn).await?;
        return stats::stats(&home, Some(&client), o).await;
    }

    let conn = Connect::from_env(cli.profile.clone());
    let client = home::connect(&conn).await?;
    let key = cli.idempotency_key.as_deref();
    let json = cli.json;

    match cli.cmd {
        Cmd::Serve { .. } | Cmd::Token { .. } | Cmd::Stats { .. } => unreachable!(),
        Cmd::Debug {
            cmd: DebugCmd::Rebuild { .. },
        }
        | Cmd::Workers {
            cmd:
                WorkersCmd::Keywords { .. }
                | WorkersCmd::Match { task_id: None, .. }
                | WorkersCmd::Up { .. }
                | WorkersCmd::Status { .. }
                | WorkersCmd::Metrics { .. }
                | WorkersCmd::Scale { .. }
                | WorkersCmd::Drain { .. }
                | WorkersCmd::Reload { .. }
                | WorkersCmd::Down { .. },
        } => unreachable!(),
        Cmd::Workers {
            cmd:
                WorkersCmd::Match {
                    task_id: Some(id),
                    files,
                    ..
                },
        } => {
            let l = workers::load(&home, files.models.as_deref(), files.config.as_deref())?;
            return workers::matches(&l, Some(&client), Some(&id), &[], json).await;
        }
        Cmd::Whoami => {
            let w = client.whoami().await?;
            if json {
                print_json(&serde_json::to_value(&w).map_err(internal)?);
            } else {
                println!(
                    "{} on board {} ({}), head {}",
                    w.actor, w.board, w.board_id, w.head
                );
            }
        }
        Cmd::Channels => {
            let v = client.get("/channels").await?;
            if json {
                print_json(&v);
            } else {
                let c: ChannelsOut = serde_json::from_value(v).map_err(internal)?;
                for ch in c.channels {
                    println!(
                        "{}\t{} threads\t{} messages",
                        ch.path, ch.threads, ch.messages
                    );
                }
            }
        }
        Cmd::Post { channel, text, m } => {
            let v = client
                .post_message(&channel, &message(&text, &m)?, key)
                .await?;
            print_id(&v, json);
        }
        Cmd::Reply { id, text, m } => {
            let v = client.reply(&id, &message(&text, &m)?, key).await?;
            print_id(&v, json);
        }
        Cmd::Progress { text, data } => {
            let task = std::env::var("CORK_TASK")
                .ok()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| usage("progress runs inside `tasks run`, which sets CORK_TASK"))?;
            let m = PostMessage {
                text,
                labels: Labels::new(),
                data: parse_data(&data)?,
            };
            let v = client.reply(&task, &m, key).await?;
            print_id(&v, json);
        }
        Cmd::Read {
            target,
            labels,
            limit,
        } => {
            if looks_like_ulid(&target) {
                let v = client.thread(&target).await?;
                if json {
                    print_json(&v);
                } else {
                    let t: ThreadOut = serde_json::from_value(v).map_err(internal)?;
                    print_message(&t.root, true);
                    if let Some(task) = &t.task {
                        println!("  task: {}", task.state.as_str());
                    }
                    for r in &t.replies {
                        print_message(r, true);
                    }
                }
            } else {
                let mut p = vec![
                    ("channel", target.clone()),
                    ("limit", limit.to_string()),
                    ("newest", "true".to_string()),
                ];
                p.extend(labels.iter().map(|l| ("label", l.clone())));
                let v = client.get(&format!("/messages{}", query(&p))).await?;
                if json {
                    print_json(&v);
                } else {
                    let mut page: MessagesPage = serde_json::from_value(v).map_err(internal)?;
                    page.messages.sort_by_key(|m| m.position);
                    for m in &page.messages {
                        print_message(m, false);
                    }
                }
            }
        }
        Cmd::Wait {
            target,
            labels,
            mentions,
            types,
            timeout,
        } => return wait(&client, &target, &labels, mentions, &types, timeout, json).await,
        Cmd::Tail {
            target,
            labels,
            types,
            after,
            from_start,
        } => {
            let mut p = vec![target_param(&target)];
            p.extend(labels.iter().map(|l| ("label", l.clone())));
            p.extend(types.iter().map(|t| ("type", t.clone())));
            let mut cursor = if from_start { Some(0) } else { after };
            loop {
                let mut q = p.clone();
                if let Some(c) = cursor {
                    q.push(("after", c.to_string()));
                }
                q.push(("wait_ms", DEFAULT_MAX_WAIT_MS.to_string()));
                let page = match client
                    .events(&q, Duration::from_millis(DEFAULT_MAX_WAIT_MS))
                    .await
                {
                    Ok(p) => p,
                    Err(e) if e.is_transient() => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                for e in &page.events {
                    print_event(e, json);
                }
                cursor = Some(page.next_after);
            }
        }
        Cmd::Export { out } => {
            let body = client.export().await?;
            match out {
                Some(path) => std::fs::write(&path, body).map_err(internal)?,
                None => print!("{body}"),
            }
        }
        Cmd::Debug {
            cmd:
                DebugCmd::Export {
                    channel,
                    thread,
                    after,
                },
        } => {
            let mut p = vec![];
            if let Some(c) = channel {
                p.push(("channel", c));
            }
            if let Some(t) = thread {
                p.push(("thread", t));
            }
            let mut cursor = after;
            let head = client.whoami().await?.head;
            while cursor < head {
                let mut q = p.clone();
                q.push(("after", cursor.to_string()));
                q.push(("limit", "1000".into()));
                let page = client.events(&q, Duration::ZERO).await?;
                for e in &page.events {
                    println!("{}", serde_json::to_string(e).map_err(internal)?);
                }
                if page.next_after <= cursor {
                    break;
                }
                cursor = page.next_after;
            }
        }
        Cmd::Tasks { cmd } => return tasks(conn, client, cmd, cli.claim_id, key, json).await,
    }
    Ok(0)
}

async fn wait(
    client: &Client,
    target: &str,
    labels: &[String],
    mentions: bool,
    types: &[String],
    timeout: Duration,
    json: bool,
) -> Result<i32, ClientError> {
    let mut p = vec![target_param(target)];
    // A wait is for messages, except a wait on a task state such as
    // `board.task=completed`, which matches the task's own events, or a
    // wait on event types named with --type (HANDOFF 17.1).
    if !types.is_empty() {
        p.extend(types.iter().map(|t| ("type", t.clone())));
    } else if !labels.iter().any(|l| l.starts_with("board.task=")) {
        p.push(("type", cork_core::message::MESSAGE_POSTED.to_string()));
    }
    p.extend(labels.iter().map(|l| ("label", l.clone())));
    if mentions {
        p.push(("mentions", "true".into()));
    }
    let deadline = Instant::now() + timeout;
    let mut cursor: Option<u64> = None;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Ok(4);
        }
        let wait = left.min(Duration::from_millis(DEFAULT_MAX_WAIT_MS));
        let mut q = p.clone();
        if let Some(c) = cursor {
            q.push(("after", c.to_string()));
        }
        q.push(("wait_ms", wait.as_millis().to_string()));
        q.push(("limit", "1".into()));
        let page = match client.events(&q, wait).await {
            Ok(p) => p,
            Err(e) if e.is_transient() => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            Err(e) => return Err(e),
        };
        if let Some(e) = page.events.first() {
            if json {
                print_json(&serde_json::to_value(e).map_err(internal)?);
            } else {
                print_event(e, false);
            }
            return Ok(0);
        }
        cursor = Some(page.next_after);
    }
}

/// The handle claims are filed under, and the board they belong to (8.3).
async fn claim_scope(client: &Client) -> Result<(String, String), ClientError> {
    let w = client.whoami().await?;
    Ok((w.board_id, w.profile.unwrap_or(w.person)))
}

async fn tasks(
    conn: Connect,
    client: Client,
    cmd: TaskCmd,
    claim_flag: Option<String>,
    key: Option<&str>,
    json: bool,
) -> Result<i32, ClientError> {
    let home = conn.home.clone();
    match cmd {
        TaskCmd::List {
            selector,
            state,
            labels,
        } => {
            let mut p = vec![];
            if let Some(s) = selector {
                p.push(("channel", s));
            }
            if let Some(s) = state {
                p.push(("state", s));
            }
            p.extend(labels.into_iter().map(|l| ("label", l)));
            let v = client.get(&format!("/tasks{}", query(&p))).await?;
            if json {
                print_json(&v);
            } else {
                let page: TasksPage = serde_json::from_value(v).map_err(internal)?;
                for t in page.tasks {
                    let t = t.task;
                    let holder = t
                        .claim
                        .as_ref()
                        .map(|c| format!(" by {}", c.holder))
                        .unwrap_or_default();
                    println!("{}\t{}\t{}{holder}", t.id, t.state.as_str(), t.channel);
                }
            }
        }
        TaskCmd::Show { id, at } => {
            let path = match at {
                Some(a) => format!("/tasks/{id}?at={a}"),
                None => format!("/tasks/{id}"),
            };
            let v = client.get(&path).await?;
            if json {
                print_json(&v);
            } else {
                let t: TaskOut = serde_json::from_value(v).map_err(internal)?;
                print_task(&t);
            }
        }
        TaskCmd::History { id } => {
            let v = client.history(&id).await?;
            if json {
                print_json(&v);
            } else {
                let h: HistoryOut = serde_json::from_value(v).map_err(internal)?;
                for e in &h.events {
                    let claim = e.data["claim_id"]
                        .as_str()
                        .map(|c| format!(" claim {c}"))
                        .unwrap_or_default();
                    println!(
                        "{}\t{}\t{}\t{}{claim}",
                        e.position,
                        fmt_time(e.at),
                        e.event_type,
                        e.actor
                    );
                }
            }
        }
        TaskCmd::Create {
            channel,
            text,
            m,
            lease,
            max_attempts,
            assignee,
            needs,
            models,
        } => {
            let mut labels = parse_labels(&m.labels)?;
            if !needs.is_empty() {
                let l = workers::load(&home, models.as_deref(), None)?;
                labels.extend(workers::needs_labels(&l.catalog, &needs)?);
            }
            let req = CreateTaskReq {
                channel,
                text,
                labels,
                data: parse_data(&m.data)?,
                lease_ms: ms(lease),
                max_attempts,
                assignee,
            };
            let v = client.create_task(&req, key).await?;
            if json {
                print_json(&v);
            } else {
                println!("{}", v["task"]["id"].as_str().unwrap_or(""));
            }
        }
        TaskCmd::Claim { id, r, lease } => {
            let req = claim_req(id, &r, ms(lease))?;
            let v = client.claim_raw(&req, key).await?;
            let c: ClaimOut = serde_json::from_value(v.clone()).map_err(internal)?;
            let code = match c.outcome {
                ClaimOutcome::Claimed => 0,
                ClaimOutcome::NothingReady => 3,
                ClaimOutcome::Timeout => 4,
            };
            if let (Some(task), Some(claim_id)) = (&c.task, &c.claim_id) {
                let (board, handle) = claim_scope(&client).await?;
                home::save_claim(
                    &home,
                    &board,
                    &handle,
                    &home::ClaimFile {
                        task_id: task.id.clone(),
                        claim_id: claim_id.clone(),
                    },
                )
                .map_err(internal)?;
            }
            if json {
                print_json(&v);
            } else if let Some(task) = &c.task {
                println!("{}", task.id);
                if let Some(root) = &c.root {
                    println!("{}", root.text);
                }
            } else {
                eprintln!(
                    "cork: {}",
                    if code == 3 {
                        "nothing ready"
                    } else {
                        "timed out with nothing ready"
                    }
                );
            }
            return Ok(code);
        }
        TaskCmd::Renew { id } => {
            return settle(
                &home,
                &client,
                &id,
                "renew",
                SettleReq::default(),
                claim_flag,
                key,
                json,
            )
            .await;
        }
        TaskCmd::Complete { id, result } => {
            let req = SettleReq {
                result,
                ..Default::default()
            };
            return settle(&home, &client, &id, "complete", req, claim_flag, key, json).await;
        }
        TaskCmd::Fail { id, reason } => {
            let req = SettleReq {
                reason: Some(reason),
                ..Default::default()
            };
            return settle(&home, &client, &id, "fail", req, claim_flag, key, json).await;
        }
        TaskCmd::Release { id } => {
            return settle(
                &home,
                &client,
                &id,
                "release",
                SettleReq::default(),
                claim_flag,
                key,
                json,
            )
            .await;
        }
        TaskCmd::Block { id, question } => {
            let req = SettleReq {
                question: Some(question),
                ..Default::default()
            };
            return settle(&home, &client, &id, "block", req, claim_flag, key, json).await;
        }
        TaskCmd::Unblock { id, note } => {
            let req = SettleReq {
                note,
                ..Default::default()
            };
            return unclaimed(&client, &id, "unblock", req, key, json).await;
        }
        TaskCmd::Cancel { id, reason } => {
            let req = SettleReq {
                reason,
                ..Default::default()
            };
            return unclaimed(&client, &id, "cancel", req, key, json).await;
        }
        TaskCmd::Reopen { id } => {
            return unclaimed(&client, &id, "reopen", SettleReq::default(), key, json).await;
        }
        TaskCmd::Escalate {
            id,
            needs,
            max,
            files,
        } => {
            let l = workers::load(&home, files.models.as_deref(), files.config.as_deref())?;
            return escalate::escalate(&client, &l, &id, &needs, max, json).await;
        }
        TaskCmd::Run {
            id,
            r,
            lease,
            looping,
            max_runtime,
            usage: usage_parser,
            command,
        } => {
            let target = match (id, &r.ready) {
                (Some(id), None) => run::Target::Id(id),
                (None, Some(sel)) => run::Target::Ready {
                    channel: sel.clone(),
                    labels: r.labels.clone(),
                    from: r.from.clone(),
                    wait_ms: r.wait.map(|w| w.as_millis() as u64).unwrap_or(0),
                },
                _ => return Err(usage("tasks run takes a task id or --ready <selector>")),
            };
            let command = run::parse_command(&command)?;
            return Ok(run::run(
                conn,
                client,
                run::RunOpts {
                    target,
                    lease_ms: ms(lease),
                    looping,
                    max_runtime,
                    command,
                    json,
                    usage: usage_parser,
                },
            )
            .await);
        }
    }
    Ok(0)
}

fn claim_req(
    id: Option<String>,
    r: &ReadyArgs,
    lease_ms: Option<i64>,
) -> Result<ClaimReq, ClientError> {
    match (id, &r.ready) {
        (Some(id), None) => Ok(ClaimReq {
            task_id: Some(id),
            lease_ms,
            ..Default::default()
        }),
        (None, Some(sel)) => Ok(ClaimReq {
            channel: Some(sel.clone()),
            labels: r.labels.clone(),
            from: r.from.clone(),
            wait_ms: r.wait.map(|w| w.as_millis() as u64),
            lease_ms,
            ..Default::default()
        }),
        _ => Err(usage("tasks claim takes a task id or --ready <selector>")),
    }
}

/// A settle that needs our claim: from `--claim`/`CORK_CLAIM`, else the
/// claim file. With neither, send nothing (8.3).
#[allow(clippy::too_many_arguments)]
async fn settle(
    home: &std::path::Path,
    client: &Client,
    id: &str,
    op: &str,
    mut req: SettleReq,
    claim_flag: Option<String>,
    key: Option<&str>,
    json: bool,
) -> Result<i32, ClientError> {
    let (board, handle) = claim_scope(client).await?;
    let claim = match claim_flag.filter(|c| !c.is_empty()) {
        Some(c) => c,
        None => match home::load_claim(home, &board, &handle, id) {
            Some(f) => f.claim_id,
            None => {
                return Err(ClientError::new(
                    "claim_not_held",
                    format!(
                        "{handle} holds no stored claim on {id}; pass --claim <id> or set CORK_CLAIM"
                    ),
                ));
            }
        },
    };
    req.claim_id = claim;
    match client.settle(id, op, &req, key).await {
        Ok(v) => {
            if op != "renew" {
                home::remove_claim(home, &board, &handle, id);
            }
            print_settled(&v, json);
            Ok(0)
        }
        Err(e) => {
            if e.code == "claim_lost" {
                home::remove_claim(home, &board, &handle, id);
            }
            Err(e)
        }
    }
}

async fn unclaimed(
    client: &Client,
    id: &str,
    op: &str,
    req: SettleReq,
    key: Option<&str>,
    json: bool,
) -> Result<i32, ClientError> {
    let v = client.settle(id, op, &req, key).await?;
    print_settled(&v, json);
    Ok(0)
}

// ---------- output ----------

pub(crate) fn print_json(v: &Value) {
    println!("{}", serde_json::to_string_pretty(v).unwrap_or_default());
}

fn print_id(v: &Value, json: bool) {
    if json {
        print_json(v);
    } else {
        println!(
            "{}",
            v["id"]
                .as_str()
                .or(v["message"]["id"].as_str())
                .unwrap_or("")
        );
    }
}

fn print_settled(v: &Value, json: bool) {
    if json {
        print_json(v);
    } else {
        let replayed = if v["replayed"].as_bool() == Some(true) {
            " (replayed)"
        } else {
            ""
        };
        println!(
            "{}\t{}{replayed}",
            v["task"]["id"].as_str().unwrap_or(""),
            v["task"]["state"].as_str().unwrap_or("")
        );
    }
}

fn fmt_time(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        ms.rem_euclid(1000)
    )
}

fn print_message(m: &MessageOut, in_thread: bool) {
    let m2 = &m.message;
    let labels: Vec<String> = m2.labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
    let labels = if labels.is_empty() {
        String::new()
    } else {
        format!(" [{}]", labels.join(" "))
    };
    let indent = if m2.parent.is_some() { "  " } else { "" };
    // In a channel listing, a reply says which thread it belongs to.
    let thread = if m2.parent.is_some() && !in_thread {
        format!(" (reply in {})", m2.thread_id)
    } else {
        String::new()
    };
    let indent = if in_thread { indent } else { "" };
    println!(
        "{indent}{} {} {} in {}{thread}{labels}",
        m2.id,
        fmt_time(m2.at),
        m2.actor,
        m2.channel
    );
    for line in m2.text.lines() {
        println!("{indent}  {line}");
    }
    if let Some(d) = &m2.data {
        println!("{indent}  data: {d}");
    }
}

fn print_task(t: &TaskOut) {
    let k = &t.task;
    println!(
        "{} {} in {} (owner {})",
        k.id,
        k.state.as_str(),
        k.channel,
        k.owner
    );
    println!("  {}", t.root.message.text);
    println!(
        "  attempts {}/{}, lease {} ms",
        k.attempts, k.max_attempts, k.lease_ms
    );
    if let Some(c) = &k.claim {
        println!(
            "  held by {}, lease ends in {} ms",
            c.holder,
            c.expires_at - t.now
        );
    }
    for (label, v) in [
        ("result", &k.result),
        ("reason", &k.reason),
        (
            "question",
            if k.state == cork_core::TaskState::Blocked {
                &k.question
            } else {
                &None
            },
        ),
    ] {
        if let Some(v) = v {
            println!("  {label}: {v}");
        }
    }
}

fn print_event(e: &EventOut, json: bool) {
    if json {
        println!("{}", serde_json::to_string(e).unwrap_or_default());
        return;
    }
    let what = match e.data["text"].as_str() {
        Some(t) => t.lines().next().unwrap_or("").to_string(),
        None => {
            let s = e.data.to_string();
            if s.len() > 120 {
                format!("{}…", &s[..s.floor_char_boundary(120)])
            } else {
                s
            }
        }
    };
    let ch = e.channel.as_deref().unwrap_or("");
    println!(
        "{}\t{}\t{}\t{ch}\t{what}",
        e.position, e.event_type, e.actor
    );
}
