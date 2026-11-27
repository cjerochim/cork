//! Usage capture for `cork tasks run` (HANDOFF 18.2).
//!
//! Only the agent knows its tokens, cost and turns. The wrapper reads them
//! at exit, from `$CORK_USAGE` or from a parser for a known agent's output,
//! and posts them as one ordinary reply in the task's thread, labelled
//! `usage=attempt`. The board stores it like any other message: the core
//! knows nothing of usage.
//!
//! A usage file or parser that fails never changes how the task settles.

use serde_json::{Map, Value, json};

/// The label key and value on every usage reply.
pub const LABEL_KEY: &str = "usage";
pub const LABEL_VALUE: &str = "attempt";

/// A usage record bigger than this keeps only the known fields, so the reply
/// stays well inside the board's 256 KiB data limit.
const MAX_RECORD: usize = 32 * 1024;

/// The fields `cork stats` reads. Everything else in a usage file is kept
/// unless the record grows too big.
const KNOWN: &[&str] = &[
    "model",
    "agent_model",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "cost_usd",
    "turns",
    "models",
    "agent_error",
    "agent_duration_ms",
];

/// Parsers for agents whose output holds their usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Parser {
    /// Claude Code with `--output-format json`.
    ClaudeJson,
}

impl std::str::FromStr for Parser {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "claude-json" => Ok(Self::ClaudeJson),
            _ => Err(format!("unknown usage parser `{s}`; known: claude-json")),
        }
    }
}

impl Parser {
    pub fn name(self) -> &'static str {
        match self {
            Self::ClaudeJson => "claude-json",
        }
    }

    /// Usage and result text from the command's whole stdout.
    pub fn parse(self, stdout: &str) -> Option<Parsed> {
        match self {
            Self::ClaudeJson => parse_claude_json(stdout),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Parsed {
    pub usage: Map<String, Value>,
    /// The agent's answer, used as the task result.
    pub result: Option<String>,
}

/// Read `$CORK_USAGE`: a JSON object, or nothing. An empty, missing or
/// broken file is nothing.
pub fn read_file(path: &std::path::Path) -> Option<Map<String, Value>> {
    let s = std::fs::read_to_string(path).ok()?;
    match serde_json::from_str::<Value>(s.trim()).ok()? {
        Value::Object(m) => Some(m),
        _ => None,
    }
}

/// The last JSON value in `out`: the whole text, or else the last line that
/// parses.
fn last_json(out: &str) -> Option<Value> {
    let t = out.trim();
    if let Ok(v) = serde_json::from_str::<Value>(t) {
        return Some(v);
    }
    t.lines()
        .rev()
        .find_map(|l| serde_json::from_str::<Value>(l.trim()).ok())
}

fn num(v: &Value) -> Option<f64> {
    v.as_f64()
}

/// Claude Code's `--output-format json` prints one object at the end (or,
/// with `--verbose`, an array whose last `result` item is that object). It
/// holds `total_cost_usd`, `num_turns`, `usage`, `modelUsage` per model and
/// the `result` text.
pub fn parse_claude_json(stdout: &str) -> Option<Parsed> {
    let v = last_json(stdout)?;
    let o = match v {
        Value::Array(items) => items.into_iter().rev().find(|i| i["type"] == "result")?,
        o @ Value::Object(_) => o,
        _ => return None,
    };
    if !o.is_object() || (o.get("type").is_some() && o["type"] != "result") {
        return None;
    }
    let mut u = Map::new();
    if let Some(c) = num(&o["total_cost_usd"]).or_else(|| num(&o["cost_usd"])) {
        u.insert("cost_usd".into(), json!(c));
    }
    if let Some(t) = o["num_turns"].as_u64() {
        u.insert("turns".into(), json!(t));
    }
    if let Some(d) = o["duration_ms"].as_u64() {
        u.insert("agent_duration_ms".into(), json!(d));
    }
    if let Some(e) = o["is_error"].as_bool() {
        u.insert("agent_error".into(), json!(e));
    }

    // Per model, when present, is the full picture: the top-level `usage`
    // can leave out the small models Claude Code uses on the side.
    let mut models = Map::new();
    let (mut inp, mut out, mut cr, mut cw) = (0u64, 0u64, 0u64, 0u64);
    let mut main: Option<(String, f64, u64)> = None;
    if let Some(mu) = o["modelUsage"].as_object() {
        for (name, m) in mu {
            let i = m["inputTokens"].as_u64().unwrap_or(0);
            let ot = m["outputTokens"].as_u64().unwrap_or(0);
            let r = m["cacheReadInputTokens"].as_u64().unwrap_or(0);
            let w = m["cacheCreationInputTokens"].as_u64().unwrap_or(0);
            let c = num(&m["costUSD"]).unwrap_or(0.0);
            inp += i;
            out += ot;
            cr += r;
            cw += w;
            let mut e = Map::new();
            e.insert("input_tokens".into(), json!(i));
            e.insert("output_tokens".into(), json!(ot));
            e.insert("cache_read_tokens".into(), json!(r));
            e.insert("cache_write_tokens".into(), json!(w));
            if m.get("costUSD").is_some() {
                e.insert("cost_usd".into(), json!(c));
            }
            models.insert(name.clone(), Value::Object(e));
            // The model that cost the most did the work; tokens break ties.
            let better = match &main {
                None => true,
                Some((_, bc, bt)) => c > *bc || (c == *bc && i + ot > *bt),
            };
            if better {
                main = Some((name.clone(), c, i + ot));
            }
        }
    }
    if !models.is_empty() {
        u.insert("input_tokens".into(), json!(inp));
        u.insert("output_tokens".into(), json!(out));
        u.insert("cache_read_tokens".into(), json!(cr));
        u.insert("cache_write_tokens".into(), json!(cw));
        u.insert("models".into(), Value::Object(models));
    } else if let Some(us) = o["usage"].as_object() {
        for (from, to) in [
            ("input_tokens", "input_tokens"),
            ("output_tokens", "output_tokens"),
            ("cache_read_input_tokens", "cache_read_tokens"),
            ("cache_creation_input_tokens", "cache_write_tokens"),
        ] {
            if let Some(n) = us.get(from).and_then(|n| n.as_u64()) {
                u.insert(to.into(), json!(n));
            }
        }
    }
    if let Some((m, _, _)) = main {
        u.insert("model".into(), json!(m));
    } else if let Some(m) = o["model"].as_str() {
        u.insert("model".into(), json!(m));
    }
    let result = o["result"].as_str().map(str::to_string);
    if u.is_empty() && result.is_none() {
        return None;
    }
    Some(Parsed { usage: u, result })
}

/// What the wrapper knows about the attempt, added to every record.
#[derive(Debug, Clone, Default)]
pub struct Attempt {
    pub task: String,
    pub attempt: u32,
    pub claim_id: String,
    pub profile: Option<String>,
    /// From `$CORK_POOL`, set by the fleet supervisor.
    pub pool: Option<String>,
    /// From `$CORK_MODEL`: the catalog's name for the model.
    pub model: Option<String>,
    /// The command's exit status, or `None` if it was killed or never ran.
    pub exit: Option<i32>,
    /// completed, failed, blocked, max_runtime, released or lost.
    pub outcome: &'static str,
    pub duration_ms: u64,
    /// `file`, or a parser's name.
    pub source: Option<String>,
}

/// One usage record: the agent's fields, then the wrapper's. When the fleet
/// names the model (`$CORK_MODEL`), that catalog name is `model`, and the
/// agent's own name for it moves to `agent_model`, so `--by model` groups
/// by the catalog.
pub fn record(agent: Map<String, Value>, a: &Attempt) -> Value {
    let mut r = agent;
    if MAX_RECORD
        < serde_json::to_vec(&r)
            .map(|v| v.len())
            .unwrap_or(usize::MAX)
    {
        r.retain(|k, _| KNOWN.contains(&k.as_str()) && k != "models");
        r.insert("truncated".into(), json!(true));
    }
    if let Some(m) = &a.model {
        if let Some(agent_model) = r.remove("model")
            && agent_model.as_str() != Some(m.as_str())
        {
            r.insert("agent_model".into(), agent_model);
        }
        r.insert("model".into(), json!(m));
    }
    if r.contains_key("cost_usd") {
        r.insert("cost_source".into(), json!("agent"));
    }
    r.insert("task".into(), json!(a.task));
    r.insert("attempt".into(), json!(a.attempt));
    r.insert("claim_id".into(), json!(a.claim_id));
    r.insert("profile".into(), json!(a.profile));
    r.insert("pool".into(), json!(a.pool));
    r.insert("exit".into(), json!(a.exit));
    r.insert("outcome".into(), json!(a.outcome));
    r.insert("duration_ms".into(), json!(a.duration_ms));
    r.insert("source".into(), json!(a.source));
    if !r.contains_key("model") {
        r.insert("model".into(), Value::Null);
    }
    Value::Object(r)
}

/// The reply's text: numbers only, never prompts or output (18.6).
pub fn summary(r: &Value) -> String {
    let mut s = format!(
        "usage: attempt {} {} after {:.1}s",
        r["attempt"],
        r["outcome"].as_str().unwrap_or("ended"),
        r["duration_ms"].as_u64().unwrap_or(0) as f64 / 1000.0
    );
    if let Some(m) = r["model"].as_str() {
        s.push_str(&format!(" on {m}"));
    }
    if let (Some(i), Some(o)) = (r["input_tokens"].as_u64(), r["output_tokens"].as_u64()) {
        s.push_str(&format!(", {i} in / {o} out tokens"));
    }
    if let Some(t) = r["turns"].as_u64() {
        s.push_str(&format!(", {t} turns"));
    }
    if let Some(c) = r["cost_usd"].as_f64() {
        s.push_str(&format!(", ${c:.4}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A result in the shape `claude -p --output-format json` prints: the
    /// fields and nesting of Claude Code's output, with made-up numbers.
    pub const CLAUDE_JSON: &str = r#"{"type":"result","subtype":"success","is_error":false,"duration_ms":48211,"duration_api_ms":51002,"num_turns":7,"result":"Added the test and it passes.","session_id":"0b6f3c1e-2f4e-4c55-9a51-6e8d1b7e2f10","total_cost_usd":0.1873,"usage":{"input_tokens":21,"cache_creation_input_tokens":18210,"cache_read_input_tokens":90112,"output_tokens":1904,"server_tool_use":{"web_search_requests":0},"service_tier":"standard"},"modelUsage":{"claude-haiku-4-5-20251001":{"inputTokens":3120,"outputTokens":210,"cacheReadInputTokens":0,"cacheCreationInputTokens":0,"webSearchRequests":0,"costUSD":0.0042,"contextWindow":200000},"claude-sonnet-4-5-20250929":{"inputTokens":21,"outputTokens":1904,"cacheReadInputTokens":90112,"cacheCreationInputTokens":18210,"webSearchRequests":0,"costUSD":0.1831,"contextWindow":200000}},"permission_denials":[]}"#;

    #[test]
    fn claude_json_gives_cost_tokens_turns_model_and_result() {
        let p = parse_claude_json(&format!("some log line\n{CLAUDE_JSON}\n")).unwrap();
        assert_eq!(p.result.as_deref(), Some("Added the test and it passes."));
        let u = Value::Object(p.usage);
        assert_eq!(u["cost_usd"], 0.1873);
        assert_eq!(u["turns"], 7);
        assert_eq!(u["model"], "claude-sonnet-4-5-20250929");
        assert_eq!(u["input_tokens"], 3141);
        assert_eq!(u["output_tokens"], 2114);
        assert_eq!(u["cache_read_tokens"], 90112);
        assert_eq!(u["cache_write_tokens"], 18210);
        assert_eq!(u["models"]["claude-haiku-4-5-20251001"]["cost_usd"], 0.0042);
        assert_eq!(u["agent_error"], false);
    }

    #[test]
    fn claude_json_verbose_array_takes_the_result_item() {
        let arr = format!(r#"[{{"type":"system","subtype":"init"}},{CLAUDE_JSON}]"#);
        let p = parse_claude_json(&arr).unwrap();
        assert_eq!(p.usage["turns"], 7);
    }

    #[test]
    fn claude_json_without_model_usage_reads_usage() {
        let p = parse_claude_json(
            r#"{"type":"result","result":"ok","total_cost_usd":0.5,"num_turns":2,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":7}}"#,
        )
        .unwrap();
        assert_eq!(p.usage["input_tokens"], 10);
        assert_eq!(p.usage["cache_read_tokens"], 7);
        assert!(!p.usage.contains_key("model"));
    }

    #[test]
    fn junk_parses_to_nothing() {
        assert!(parse_claude_json("").is_none());
        assert!(parse_claude_json("hello\nworld").is_none());
        assert!(parse_claude_json("[1,2]").is_none());
        assert!(parse_claude_json(r#"{"type":"assistant"}"#).is_none());
    }

    #[test]
    fn the_fleet_model_wins_and_the_agent_model_is_kept() {
        let mut agent = Map::new();
        agent.insert("model".into(), json!("claude-sonnet-4-5"));
        agent.insert("cost_usd".into(), json!(0.3));
        agent.insert("extra".into(), json!({"kept": true}));
        let a = Attempt {
            task: "T".into(),
            attempt: 2,
            claim_id: "C".into(),
            model: Some("sonnet".into()),
            pool: Some("sonnet".into()),
            exit: Some(0),
            outcome: "completed",
            duration_ms: 1500,
            ..Default::default()
        };
        let r = record(agent, &a);
        assert_eq!(r["model"], "sonnet");
        assert_eq!(r["agent_model"], "claude-sonnet-4-5");
        assert_eq!(r["cost_source"], "agent");
        assert_eq!(r["extra"]["kept"], true);
        assert_eq!(r["attempt"], 2);
        assert_eq!(
            summary(&r),
            "usage: attempt 2 completed after 1.5s on sonnet, $0.3000"
        );
    }

    #[test]
    fn a_huge_record_keeps_only_known_fields() {
        let mut agent = Map::new();
        agent.insert("cost_usd".into(), json!(1.0));
        agent.insert("blob".into(), json!("x".repeat(MAX_RECORD)));
        let r = record(agent, &Attempt::default());
        assert!(r.get("blob").is_none());
        assert_eq!(r["truncated"], true);
        assert_eq!(r["cost_usd"], 1.0);
    }
}
