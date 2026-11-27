//! `cork stats` (HANDOFF 18): metrics derived by replaying the event log.
//!
//! Two kinds of data, kept apart:
//!
//! - **Flow** comes from task events alone: time to claim, work time, time
//!   to done, attempts, lease expiries, escalations, time blocked.
//! - **Usage** comes from the `usage=attempt` replies the wrapper posts:
//!   ordinary messages, matched to attempts by `claim_id`.
//!
//! Nothing here writes, and nothing needs a daemon: the input is a list of
//! events, from `/events` or from an export. The same log gives the same
//! report, so a metric can be recomputed for any past period.
//!
//! The headline number is **cost per successful task**: all cost spent on a
//! task, its failed attempts and every retry in its `retry_of` chain,
//! divided by the chains that completed. A chain is charged to the group
//! its first task started in, and also shown split by the model that
//! finished it.

use cork_api::EventOut;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub type Millis = i64;

/// The label on a retry made by `cork tasks escalate` (16.7).
pub const RETRY_OF: &str = "retry_of";
/// The label children of a feature carry (17).
pub const PARENT: &str = "parent";
/// A key for attempts or tasks with nothing to group by.
pub const UNKNOWN: &str = "unknown";
pub const UNCLAIMED: &str = "(unclaimed)";
pub const NONE: &str = "(none)";

/// What to group by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum By {
    Model,
    Pool,
    Profile,
    Channel,
    Label(String),
    Day,
}

impl std::str::FromStr for By {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        Ok(match s {
            "model" => Self::Model,
            "pool" => Self::Pool,
            "profile" => Self::Profile,
            "channel" => Self::Channel,
            "day" => Self::Day,
            _ => match s.strip_prefix("label:") {
                Some(k) if !k.is_empty() => Self::Label(k.to_string()),
                _ => {
                    return Err(format!(
                        "--by takes model, pool, profile, channel, label:<key> or day, not `{s}`"
                    ));
                }
            },
        })
    }
}

impl std::fmt::Display for By {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            By::Model => f.write_str("model"),
            By::Pool => f.write_str("pool"),
            By::Profile => f.write_str("profile"),
            By::Channel => f.write_str("channel"),
            By::Label(k) => write!(f, "label:{k}"),
            By::Day => f.write_str("day"),
        }
    }
}

/// Dollars per million tokens, from `models.toml` (18.2).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Price {
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
    pub cache_write: f64,
}

#[derive(Debug, Clone, Default)]
pub struct Options {
    pub by: Option<By>,
    /// Only tasks created at or after this time.
    pub since: Option<Millis>,
    /// Only tasks with all of these labels.
    pub labels: Vec<(String, String)>,
    /// Only this feature task and the tasks with `parent=<id>`.
    pub feature: Option<String>,
    /// Model name to price, for usage that reports tokens but no cost.
    pub prices: BTreeMap<String, Price>,
    /// Pool name to model name, from the fleet config. Used for attempts
    /// with no usage reply, whose pool is read from the profile name.
    pub pool_models: BTreeMap<String, String>,
    /// Model names in the catalog: a pool named like a model runs it.
    pub models: BTreeSet<String>,
    /// Fleet names, to read `<fleet>-<pool>-<n>` profiles exactly.
    pub fleets: Vec<String>,
    /// Group failure reasons (18.6: text is shown only when asked).
    pub reasons: bool,
}

// ---------- replay ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum End {
    Open,
    Completed,
    Failed,
    Released,
    Blocked,
    Expired,
    Canceled,
}

#[derive(Debug, Clone)]
pub struct Attempt {
    pub task: String,
    pub claim_id: String,
    pub n: u32,
    /// The holder's profile, or the person when there is none.
    pub profile: String,
    pub claimed_at: Millis,
    /// How long the task was ready before this claim.
    pub waited_ms: Option<i64>,
    pub ended_at: Option<Millis>,
    pub end: End,
}

#[derive(Debug, Clone)]
pub struct TaskRec {
    pub id: String,
    pub channel: String,
    pub labels: BTreeMap<String, String>,
    pub created_at: Millis,
    pub state: &'static str,
    pub completed_at: Option<Millis>,
    /// Indexes into [`Replay::attempts`], in order.
    pub attempts: Vec<usize>,
    pub blocked_ms: i64,
    pub reasons: Vec<String>,
    ready_since: Option<Millis>,
    blocked_since: Option<Millis>,
    open: Option<usize>,
}

impl TaskRec {
    pub fn retry_of(&self) -> Option<&str> {
        self.labels.get(RETRY_OF).map(String::as_str)
    }
}

/// The log, replayed into tasks, attempts and usage records.
#[derive(Debug, Default)]
pub struct Replay {
    pub tasks: BTreeMap<String, TaskRec>,
    /// Task ids in creation order.
    pub order: Vec<String>,
    pub attempts: Vec<Attempt>,
    /// Usage replies by claim id; the first one wins.
    pub usage: HashMap<String, Value>,
    /// (time, ready tasks after it), one point per change.
    pub depth: Vec<(Millis, i64)>,
    pub first_at: Option<Millis>,
    pub last_at: Option<Millis>,
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or("").to_string()
}

impl Replay {
    /// Replay events, in position order.
    pub fn new(events: &[EventOut]) -> Self {
        let mut r = Replay::default();
        let mut sorted: Vec<&EventOut> = events.iter().collect();
        sorted.sort_by_key(|e| e.position);
        let mut ready: i64 = 0;
        for e in sorted {
            r.first_at.get_or_insert(e.at);
            r.last_at = Some(e.at);
            if e.event_type == "MessagePosted" {
                if e.data["labels"]["usage"] == "attempt"
                    && let Some(claim) = e.data["data"]["claim_id"].as_str()
                {
                    r.usage
                        .entry(claim.to_string())
                        .or_insert_with(|| e.data["data"].clone());
                }
                continue;
            }
            let Some(id) = e.stream.strip_prefix("task:") else {
                continue;
            };
            let d = &e.data;
            if e.event_type == "TaskCreated" {
                let labels = d["labels"]
                    .as_object()
                    .map(|m| {
                        m.iter()
                            .map(|(k, v)| (k.clone(), s(v)))
                            .collect::<BTreeMap<_, _>>()
                    })
                    .unwrap_or_default();
                r.tasks.insert(
                    id.to_string(),
                    TaskRec {
                        id: id.to_string(),
                        channel: s(&d["channel"]),
                        labels,
                        created_at: e.at,
                        state: "ready",
                        completed_at: None,
                        attempts: vec![],
                        blocked_ms: 0,
                        reasons: vec![],
                        ready_since: Some(e.at),
                        blocked_since: None,
                        open: None,
                    },
                );
                r.order.push(id.to_string());
                ready += 1;
                r.depth.push((e.at, ready));
                continue;
            }
            let Some(t) = r.tasks.get_mut(id) else {
                continue;
            };
            let was_ready = t.ready_since.is_some();
            let end_open = |t: &mut TaskRec, atts: &mut Vec<Attempt>, end: End| {
                if let Some(i) = t.open.take() {
                    atts[i].end = end;
                    atts[i].ended_at = Some(e.at);
                }
            };
            match e.event_type.as_str() {
                "TaskClaimed" => {
                    let holder = &d["holder"];
                    let profile = holder["profile"]
                        .as_str()
                        .or(holder["person"].as_str())
                        .unwrap_or(UNKNOWN)
                        .to_string();
                    r.attempts.push(Attempt {
                        task: id.to_string(),
                        claim_id: s(&d["claim_id"]),
                        n: d["attempt"].as_u64().unwrap_or(0) as u32,
                        profile,
                        claimed_at: e.at,
                        waited_ms: t.ready_since.map(|s| e.at - s),
                        ended_at: None,
                        end: End::Open,
                    });
                    let i = r.attempts.len() - 1;
                    t.attempts.push(i);
                    t.open = Some(i);
                    t.ready_since = None;
                    t.state = "claimed";
                }
                "LeaseExpired" => {
                    end_open(t, &mut r.attempts, End::Expired);
                    t.ready_since = Some(e.at);
                    t.state = "ready";
                }
                "TaskReleased" => {
                    end_open(t, &mut r.attempts, End::Released);
                    t.ready_since = Some(e.at);
                    t.state = "ready";
                }
                "TaskCompleted" => {
                    end_open(t, &mut r.attempts, End::Completed);
                    t.ready_since = None;
                    t.state = "completed";
                    t.completed_at = Some(e.at);
                }
                "TaskFailed" => {
                    end_open(t, &mut r.attempts, End::Failed);
                    t.ready_since = None;
                    t.state = "failed";
                    t.reasons.push(s(&d["reason"]));
                }
                "TaskBlocked" => {
                    end_open(t, &mut r.attempts, End::Blocked);
                    t.ready_since = None;
                    t.blocked_since = Some(e.at);
                    t.state = "blocked";
                }
                "TaskUnblocked" => {
                    if let Some(b) = t.blocked_since.take() {
                        t.blocked_ms += e.at - b;
                    }
                    t.ready_since = Some(e.at);
                    t.state = "ready";
                }
                "TaskCanceled" => {
                    end_open(t, &mut r.attempts, End::Canceled);
                    if let Some(b) = t.blocked_since.take() {
                        t.blocked_ms += e.at - b;
                    }
                    t.ready_since = None;
                    t.state = "canceled";
                }
                "TaskReopened" => {
                    t.ready_since = Some(e.at);
                    t.completed_at = None;
                    t.state = "ready";
                }
                _ => {}
            }
            let now_ready = t.ready_since.is_some();
            if was_ready != now_ready {
                ready += if now_ready { 1 } else { -1 };
                r.depth.push((e.at, ready));
            }
        }
        r
    }
}

// ---------- the report ----------

/// Nearest-rank percentiles of a list of durations.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Pcts {
    pub n: usize,
    pub p50_ms: Option<i64>,
    pub p95_ms: Option<i64>,
}

fn pcts(mut v: Vec<i64>) -> Pcts {
    v.sort_unstable();
    let at = |p: f64| -> Option<i64> {
        if v.is_empty() {
            return None;
        }
        let rank = ((p * v.len() as f64).ceil() as usize).clamp(1, v.len());
        Some(v[rank - 1])
    };
    Pcts {
        n: v.len(),
        p50_ms: at(0.50),
        p95_ms: at(0.95),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Usage {
    /// Attempts with a usage reply.
    pub reported: u64,
    /// Finished attempts with no usage reply, such as a wrapper killed with
    /// `kill -9`. Counted, never guessed (18.2).
    pub unknown: u64,
    /// Attempts with a usage reply but no cost and no price for the model.
    pub cost_unknown: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub turns: u64,
    pub cost_usd: f64,
    /// Where each costed attempt's number came from: `agent` or `price`.
    pub cost_sources: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Group {
    pub key: String,
    pub tasks: u64,
    pub completed: u64,
    pub failed: u64,
    pub canceled: u64,
    /// Not yet completed, failed or canceled.
    pub open: u64,
    /// completed / (completed + failed).
    pub success_rate: Option<f64>,
    pub time_to_claim: Pcts,
    pub work_time: Pcts,
    pub time_to_done: Pcts,
    pub attempts: u64,
    pub attempts_per_task: Option<f64>,
    /// Of the tasks whose first attempt ended, the share it completed.
    pub first_attempt_success: Option<f64>,
    pub lease_expiries: u64,
    /// Tasks that are an escalated retry (`retry_of`), and how many completed.
    pub escalated: u64,
    pub escalated_completed: u64,
    pub blocked_ms: i64,
    pub usage: Usage,
    /// Chains (a task and its retries) that started in this group.
    pub chains: u64,
    pub chains_completed: u64,
    /// All cost spent on those chains, every attempt and retry included.
    pub chain_cost_usd: f64,
    /// chain_cost_usd / chains_completed.
    pub cost_per_success: Option<f64>,
    /// True when some attempt in those chains has unknown cost, so the
    /// numbers are a lower bound.
    pub cost_incomplete: bool,
}

/// Completed chains split by where they started and which model finished.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Split {
    pub started: String,
    pub finished_model: String,
    pub completed: u64,
    pub cost_usd: f64,
    pub cost_per_success: f64,
    pub cost_incomplete: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Queue {
    /// The most tasks ready at once.
    pub peak: i64,
    /// Ready tasks, averaged over time.
    pub mean: f64,
    /// Ready at the end of the log.
    pub last: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Report {
    pub by: String,
    pub from: Option<Millis>,
    pub to: Option<Millis>,
    pub total: Group,
    pub groups: Vec<Group>,
    /// Cost per successful task by starting group and finishing model.
    pub finished_by: Vec<Split>,
    pub queue: Queue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasons: Option<Vec<(String, u64)>>,
}

/// The cost of one attempt, and where it came from.
fn attempt_cost(
    u: &Value,
    model: &str,
    prices: &BTreeMap<String, Price>,
) -> Option<(f64, &'static str)> {
    if let Some(c) = u["cost_usd"].as_f64() {
        return Some((c, "agent"));
    }
    let p = prices.get(model)?;
    let n = |k: &str| u[k].as_u64();
    let (i, o) = (n("input_tokens"), n("output_tokens"));
    if i.is_none() && o.is_none() {
        return None;
    }
    let c = (i.unwrap_or(0) as f64 * p.input
        + o.unwrap_or(0) as f64 * p.output
        + n("cache_read_tokens").unwrap_or(0) as f64 * p.cache_read
        + n("cache_write_tokens").unwrap_or(0) as f64 * p.cache_write)
        / 1_000_000.0;
    Some((c, "price"))
}

struct Ctx<'a> {
    r: &'a Replay,
    o: &'a Options,
}

impl Ctx<'_> {
    fn usage(&self, a: &Attempt) -> Option<&Value> {
        self.r.usage.get(&a.claim_id)
    }

    fn pool(&self, a: &Attempt) -> String {
        if let Some(p) = self.usage(a).and_then(|u| u["pool"].as_str()) {
            return p.to_string();
        }
        pool_of_profile(&a.profile, self.o).unwrap_or_else(|| UNKNOWN.to_string())
    }

    fn model(&self, a: &Attempt) -> String {
        if let Some(m) = self.usage(a).and_then(|u| u["model"].as_str()) {
            return m.to_string();
        }
        let pool = self.pool(a);
        if let Some(m) = self.o.pool_models.get(&pool) {
            return m.clone();
        }
        if self.o.models.contains(&pool) {
            return pool;
        }
        UNKNOWN.to_string()
    }

    fn cost(&self, a: &Attempt) -> Option<(f64, &'static str)> {
        let u = self.usage(a)?;
        attempt_cost(u, &self.model(a), &self.o.prices)
    }

    fn task_key(&self, t: &TaskRec, by: &Option<By>) -> String {
        let first = t.attempts.first().map(|i| &self.r.attempts[*i]);
        match by {
            None => "all".into(),
            Some(By::Model) => first.map_or(UNCLAIMED.into(), |a| self.model(a)),
            Some(By::Pool) => first.map_or(UNCLAIMED.into(), |a| self.pool(a)),
            Some(By::Profile) => first.map_or(UNCLAIMED.into(), |a| a.profile.clone()),
            Some(By::Channel) => t.channel.clone(),
            Some(By::Label(k)) => t.labels.get(k).cloned().unwrap_or_else(|| NONE.into()),
            Some(By::Day) => day(t.created_at),
        }
    }

    fn attempt_key(&self, a: &Attempt, by: &Option<By>) -> String {
        match by {
            Some(By::Model) => self.model(a),
            Some(By::Pool) => self.pool(a),
            Some(By::Profile) => a.profile.clone(),
            _ => self.task_key(&self.r.tasks[&a.task], by),
        }
    }

    fn wanted(&self, t: &TaskRec) -> bool {
        if self.o.since.is_some_and(|s| t.created_at < s) {
            return false;
        }
        if !self
            .o
            .labels
            .iter()
            .all(|(k, v)| t.labels.get(k) == Some(v))
        {
            return false;
        }
        match &self.o.feature {
            Some(f) => &t.id == f || t.labels.get(PARENT) == Some(f),
            None => true,
        }
    }
}

/// The pool in a fleet worker's profile, `<fleet>-<pool>-<n>`.
pub fn pool_of_profile(profile: &str, o: &Options) -> Option<String> {
    let (rest, n) = profile.rsplit_once('-')?;
    if n.is_empty() || !n.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    for f in &o.fleets {
        if let Some(p) = rest.strip_prefix(&format!("{f}-"))
            && !p.is_empty()
        {
            return Some(p.to_string());
        }
    }
    // Without the fleet's name, prefer the longest known pool or model
    // that ends the name.
    let known = o.pool_models.keys().chain(o.models.iter());
    if let Some(p) = known
        .filter(|p| rest.ends_with(&format!("-{p}")))
        .max_by_key(|p| p.len())
    {
        return Some(p.clone());
    }
    rest.split_once('-').map(|(_, p)| p.to_string())
}

/// `YYYY-MM-DD` in UTC.
pub fn day(ms: Millis) -> String {
    let days = ms.div_euclid(86_400_000);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    format!("{:04}-{m:02}-{d:02}", if m <= 2 { y + 1 } else { y })
}

fn ratio(a: u64, b: u64) -> Option<f64> {
    (b > 0).then(|| a as f64 / b as f64)
}

/// Build the report.
pub fn report(events: &[EventOut], o: &Options) -> Report {
    let r = Replay::new(events);
    let cx = Ctx { r: &r, o };
    let tasks: Vec<&TaskRec> = r
        .order
        .iter()
        .map(|id| &r.tasks[id])
        .filter(|t| cx.wanted(t))
        .collect();
    let kept: BTreeSet<&str> = tasks.iter().map(|t| t.id.as_str()).collect();

    let mut groups: BTreeMap<String, Group> = BTreeMap::new();
    let mut total = Group {
        key: "all".into(),
        ..Default::default()
    };
    let mut lists: BTreeMap<String, [Vec<i64>; 3]> = BTreeMap::new();
    let mut total_lists: [Vec<i64>; 3] = Default::default();
    let mut first_ended: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    let mut total_first = (0u64, 0u64);

    for t in &tasks {
        let key = cx.task_key(t, &o.by);
        let first = t.attempts.first().map(|i| &r.attempts[*i]);
        for (g, l, fe) in [
            (
                groups.entry(key.clone()).or_insert_with(|| Group {
                    key: key.clone(),
                    ..Default::default()
                }),
                lists.entry(key.clone()).or_default(),
                first_ended.entry(key.clone()).or_default(),
            ),
            (&mut total, &mut total_lists, &mut total_first),
        ] {
            g.tasks += 1;
            match t.state {
                "completed" => g.completed += 1,
                "failed" => g.failed += 1,
                "canceled" => g.canceled += 1,
                _ => g.open += 1,
            }
            if let Some(c) = t.completed_at {
                l[2].push(c - t.created_at);
            }
            if t.retry_of().is_some() {
                g.escalated += 1;
                if t.state == "completed" {
                    g.escalated_completed += 1;
                }
            }
            g.blocked_ms += t.blocked_ms;
            if let Some(a) = first
                && a.end != End::Open
            {
                fe.1 += 1;
                if a.end == End::Completed {
                    fe.0 += 1;
                }
            }
        }
    }

    // Attempts, each under its own key.
    for t in &tasks {
        for i in &t.attempts {
            let a = &r.attempts[*i];
            let key = cx.attempt_key(a, &o.by);
            let u = cx.usage(a);
            let cost = cx.cost(a);
            for (g, l) in [
                (
                    groups.entry(key.clone()).or_insert_with(|| Group {
                        key: key.clone(),
                        ..Default::default()
                    }),
                    lists.entry(key.clone()).or_default(),
                ),
                (&mut total, &mut total_lists),
            ] {
                g.attempts += 1;
                if let Some(w) = a.waited_ms {
                    l[0].push(w);
                }
                if let Some(e) = a.ended_at {
                    l[1].push(e - a.claimed_at);
                }
                if a.end == End::Expired {
                    g.lease_expiries += 1;
                }
                match u {
                    Some(u) => {
                        g.usage.reported += 1;
                        let n = |k: &str| u[k].as_u64().unwrap_or(0);
                        g.usage.input_tokens += n("input_tokens");
                        g.usage.output_tokens += n("output_tokens");
                        g.usage.cache_read_tokens += n("cache_read_tokens");
                        g.usage.cache_write_tokens += n("cache_write_tokens");
                        g.usage.turns += n("turns");
                        match cost {
                            Some((c, src)) => {
                                g.usage.cost_usd += c;
                                *g.usage.cost_sources.entry(src.into()).or_default() += 1;
                            }
                            None => g.usage.cost_unknown += 1,
                        }
                    }
                    None if a.end != End::Open => g.usage.unknown += 1,
                    None => {}
                }
            }
        }
    }

    // Chains: a task and every retry made from it, charged where it started.
    let mut finished: BTreeMap<(String, String), (u64, f64, bool)> = BTreeMap::new();
    let mut chains: BTreeMap<&str, Vec<&TaskRec>> = BTreeMap::new();
    for t in &tasks {
        let mut root = t.id.as_str();
        let mut seen = BTreeSet::new();
        while let Some(p) = r.tasks.get(root).and_then(|x| x.retry_of())
            && kept.contains(p)
            && seen.insert(root)
        {
            root = p;
        }
        chains.entry(root).or_default().push(t);
    }
    for (root, members) in &chains {
        let key = cx.task_key(&r.tasks[*root], &o.by);
        let mut cost = 0.0;
        let mut incomplete = false;
        let mut finisher: Option<String> = None;
        for m in members {
            for i in &m.attempts {
                let a = &r.attempts[*i];
                match cx.cost(a) {
                    Some((c, _)) => cost += c,
                    None if a.end != End::Open || cx.usage(a).is_some() => incomplete = true,
                    None => {}
                }
                if a.end == End::Completed {
                    finisher = Some(cx.model(a));
                }
            }
        }
        for g in [groups.get_mut(&key).expect("group"), &mut total] {
            g.chains += 1;
            g.chain_cost_usd += cost;
            g.cost_incomplete |= incomplete;
            if finisher.is_some() {
                g.chains_completed += 1;
            }
        }
        if let Some(f) = finisher {
            let e = finished.entry((key, f)).or_insert((0, 0.0, false));
            e.0 += 1;
            e.1 += cost;
            e.2 |= incomplete;
        }
    }

    let fill = |g: &mut Group, l: &[Vec<i64>; 3], fe: (u64, u64)| {
        g.success_rate = ratio(g.completed, g.completed + g.failed);
        g.attempts_per_task = ratio(g.attempts, g.tasks);
        g.first_attempt_success = ratio(fe.0, fe.1);
        g.time_to_claim = pcts(l[0].clone());
        g.work_time = pcts(l[1].clone());
        g.time_to_done = pcts(l[2].clone());
        g.cost_per_success =
            (g.chains_completed > 0).then(|| g.chain_cost_usd / g.chains_completed as f64);
    };
    for (k, g) in groups.iter_mut() {
        fill(
            g,
            &lists.get(k).cloned().unwrap_or_default(),
            first_ended.get(k).copied().unwrap_or_default(),
        );
    }
    fill(&mut total, &total_lists, total_first);

    let reasons = o.reasons.then(|| {
        let mut m: BTreeMap<String, u64> = BTreeMap::new();
        for t in &tasks {
            for reason in &t.reasons {
                let mut line = reason.lines().next().unwrap_or("").trim().to_string();
                if line.len() > 120 {
                    let mut cut = 120;
                    while !line.is_char_boundary(cut) {
                        cut -= 1;
                    }
                    line.truncate(cut);
                }
                *m.entry(line).or_default() += 1;
            }
        }
        let mut v: Vec<(String, u64)> = m.into_iter().collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        v
    });

    Report {
        by: o.by.as_ref().map_or("all".into(), |b| b.to_string()),
        from: r.first_at,
        to: r.last_at,
        total,
        groups: groups.into_values().collect(),
        finished_by: finished
            .into_iter()
            .map(|((started, finished_model), (n, c, inc))| Split {
                started,
                finished_model,
                completed: n,
                cost_usd: c,
                cost_per_success: c / n as f64,
                cost_incomplete: inc,
            })
            .collect(),
        queue: queue(&r.depth, r.first_at, r.last_at),
        reasons,
    }
}

/// Queue depth over the whole log. Filters do not apply to it: it is the
/// board's queue, every channel the events cover.
fn queue(depth: &[(Millis, i64)], from: Option<Millis>, to: Option<Millis>) -> Queue {
    let mut q = Queue::default();
    let (Some(from), Some(to)) = (from, to) else {
        return q;
    };
    let mut area = 0i128;
    let mut prev = (from, 0i64);
    for &(at, n) in depth {
        area += (at - prev.0) as i128 * prev.1 as i128;
        prev = (at, n);
        q.peak = q.peak.max(n);
    }
    area += (to - prev.0) as i128 * prev.1 as i128;
    q.last = prev.1;
    q.mean = if to > from {
        area as f64 / (to - from) as f64
    } else {
        prev.1 as f64
    };
    q
}

// ---------- printing ----------

pub fn fmt_ms(ms: Option<i64>) -> String {
    let Some(ms) = ms else {
        return "-".into();
    };
    let s = ms as f64 / 1000.0;
    if ms < 1000 {
        format!("{ms}ms")
    } else if s < 60.0 {
        format!("{s:.1}s")
    } else if s < 3600.0 {
        format!("{:.1}m", s / 60.0)
    } else {
        format!("{:.1}h", s / 3600.0)
    }
}

pub fn fmt_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1e6)
    } else if n >= 1000 {
        format!("{:.1}k", n as f64 / 1e3)
    } else {
        n.to_string()
    }
}

fn pct(v: Option<f64>) -> String {
    v.map_or("-".into(), |v| format!("{:.0}%", v * 100.0))
}

fn usd(v: f64) -> String {
    format!("${v:.2}")
}

const COLUMNS: &[&str] = &[
    "tasks",
    "done",
    "failed",
    "success",
    "p50 claim",
    "p50 work",
    "p95 work",
    "p50 done",
    "attempts/task",
    "1st ok",
    "expired",
    "escalated",
    "tokens in/out",
    "turns",
    "unknown",
    "cost",
    "cost/done",
];

fn row(g: &Group) -> Vec<String> {
    let mark = if g.cost_incomplete { "?" } else { "" };
    vec![
        g.key.clone(),
        g.tasks.to_string(),
        g.completed.to_string(),
        g.failed.to_string(),
        pct(g.success_rate),
        fmt_ms(g.time_to_claim.p50_ms),
        fmt_ms(g.work_time.p50_ms),
        fmt_ms(g.work_time.p95_ms),
        fmt_ms(g.time_to_done.p50_ms),
        g.attempts_per_task
            .map_or("-".into(), |v| format!("{v:.2}")),
        pct(g.first_attempt_success),
        g.lease_expiries.to_string(),
        if g.escalated > 0 {
            format!("{} ({} done)", g.escalated, g.escalated_completed)
        } else {
            "0".into()
        },
        format!(
            "{} / {}",
            fmt_tokens(g.usage.input_tokens),
            fmt_tokens(g.usage.output_tokens)
        ),
        g.usage.turns.to_string(),
        g.usage.unknown.to_string(),
        format!("{}{mark}", usd(g.usage.cost_usd)),
        g.cost_per_success
            .map_or("-".into(), |c| format!("{}{mark}", usd(c))),
    ]
}

fn table(head: Vec<String>, rows: Vec<Vec<String>>) -> String {
    let mut w: Vec<usize> = head.iter().map(|h| h.chars().count()).collect();
    for r in &rows {
        for (i, c) in r.iter().enumerate() {
            w[i] = w[i].max(c.chars().count());
        }
    }
    let line = |r: &[String]| {
        r.iter()
            .enumerate()
            .map(|(i, c)| {
                let pad = w[i] - c.chars().count();
                if i == 0 {
                    format!("{c}{}", " ".repeat(pad))
                } else {
                    format!("{}{c}", " ".repeat(pad))
                }
            })
            .collect::<Vec<_>>()
            .join("  ")
            .trim_end()
            .to_string()
    };
    let mut out = line(&head);
    out.push('\n');
    for r in &rows {
        out.push_str(&line(r));
        out.push('\n');
    }
    out
}

/// The report as text tables.
pub fn render_text(r: &Report) -> String {
    let mut head = vec![r.by.clone()];
    head.extend(COLUMNS.iter().map(|c| c.to_string()));
    let mut rows: Vec<Vec<String>> = r.groups.iter().map(row).collect();
    if r.groups.len() != 1 || r.groups[0].key != "all" {
        rows.push(row(&r.total));
    }
    let mut out = table(head, rows);
    if r.total.cost_incomplete || r.total.usage.unknown > 0 {
        out.push_str(&format!(
            "? {} attempts have unknown usage (no usage reply, such as a wrapper killed with kill -9) and {} have no cost; costs shown are a lower bound\n",
            r.total.usage.unknown, r.total.usage.cost_unknown
        ));
    }
    if !r.total.usage.cost_sources.is_empty() {
        let src: Vec<String> = r
            .total
            .usage
            .cost_sources
            .iter()
            .map(|(k, v)| {
                format!(
                    "{v} from {}",
                    if k == "price" {
                        "models.toml prices"
                    } else {
                        "the agent"
                    }
                )
            })
            .collect();
        out.push_str(&format!(
            "cost of {} attempts: {}. Estimates; the real bill is the provider's.\n",
            r.total.usage.cost_sources.values().sum::<u64>(),
            src.join(", ")
        ));
    }
    if !r.finished_by.is_empty() {
        out.push_str(&format!(
            "\ncost per successful task, by the {} it started on and the model that finished it\n",
            r.by
        ));
        let rows = r
            .finished_by
            .iter()
            .map(|s| {
                let mark = if s.cost_incomplete { "?" } else { "" };
                vec![
                    s.started.clone(),
                    s.finished_model.clone(),
                    s.completed.to_string(),
                    format!("{}{mark}", usd(s.cost_usd)),
                    format!("{}{mark}", usd(s.cost_per_success)),
                ]
            })
            .collect();
        out.push_str(&table(
            ["started", "finished", "done", "cost", "cost/done"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            rows,
        ));
    }
    out.push_str(&format!(
        "\nqueue: peak {} ready, mean {:.2}, {} ready at the end\n",
        r.queue.peak, r.queue.mean, r.queue.last
    ));
    if let Some(reasons) = &r.reasons {
        out.push_str("\nfailure reasons\n");
        if reasons.is_empty() {
            out.push_str("  none\n");
        }
        for (reason, n) in reasons {
            out.push_str(&format!("  {n:>5}  {reason}\n"));
        }
    }
    out
}

fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// One row per group plus `all`, raw numbers, milliseconds and dollars.
pub fn render_csv(r: &Report) -> String {
    let head = [
        r.by.as_str(),
        "tasks",
        "completed",
        "failed",
        "canceled",
        "open",
        "success_rate",
        "time_to_claim_p50_ms",
        "time_to_claim_p95_ms",
        "work_time_p50_ms",
        "work_time_p95_ms",
        "time_to_done_p50_ms",
        "time_to_done_p95_ms",
        "attempts",
        "attempts_per_task",
        "first_attempt_success",
        "lease_expiries",
        "escalated",
        "escalated_completed",
        "blocked_ms",
        "usage_reported",
        "usage_unknown",
        "cost_unknown",
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "turns",
        "cost_usd",
        "chains",
        "chains_completed",
        "chain_cost_usd",
        "cost_per_success",
        "cost_incomplete",
    ];
    let o = |v: Option<i64>| v.map(|v| v.to_string()).unwrap_or_default();
    let f = |v: Option<f64>| v.map(|v| format!("{v}")).unwrap_or_default();
    let mut out = head.join(",");
    out.push('\n');
    for g in r.groups.iter().chain(std::iter::once(&r.total)) {
        let cells = [
            csv_field(&g.key),
            g.tasks.to_string(),
            g.completed.to_string(),
            g.failed.to_string(),
            g.canceled.to_string(),
            g.open.to_string(),
            f(g.success_rate),
            o(g.time_to_claim.p50_ms),
            o(g.time_to_claim.p95_ms),
            o(g.work_time.p50_ms),
            o(g.work_time.p95_ms),
            o(g.time_to_done.p50_ms),
            o(g.time_to_done.p95_ms),
            g.attempts.to_string(),
            f(g.attempts_per_task),
            f(g.first_attempt_success),
            g.lease_expiries.to_string(),
            g.escalated.to_string(),
            g.escalated_completed.to_string(),
            g.blocked_ms.to_string(),
            g.usage.reported.to_string(),
            g.usage.unknown.to_string(),
            g.usage.cost_unknown.to_string(),
            g.usage.input_tokens.to_string(),
            g.usage.output_tokens.to_string(),
            g.usage.cache_read_tokens.to_string(),
            g.usage.cache_write_tokens.to_string(),
            g.usage.turns.to_string(),
            format!("{}", g.usage.cost_usd),
            g.chains.to_string(),
            g.chains_completed.to_string(),
            format!("{}", g.chain_cost_usd),
            f(g.cost_per_success),
            g.cost_incomplete.to_string(),
        ];
        out.push_str(&cells.join(","));
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests;
