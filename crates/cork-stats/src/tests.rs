//! A scripted log with known timings (18.7 items 1, 3 and 4): every number
//! below is worked out by hand from the script.

use super::*;
use serde_json::json;

struct Log {
    events: Vec<EventOut>,
}

impl Log {
    fn new() -> Self {
        Log { events: vec![] }
    }
    fn push(&mut self, stream: String, ty: &str, at: i64, data: Value) {
        let position = self.events.len() as u64 + 1;
        self.events.push(EventOut {
            schema: 1,
            position,
            stream,
            version: 0,
            event_type: ty.into(),
            data,
            actor: "tester".into(),
            at,
            channel: Some("work".into()),
            thread_id: None,
            labels: vec![],
        });
    }
    fn task(&mut self, id: &str, at: i64, ty: &str, data: Value) {
        self.push(format!("task:{id}"), ty, at, data);
    }
    fn create(&mut self, id: &str, at: i64, labels: Value) {
        self.task(
            id,
            at,
            "TaskCreated",
            json!({"task_id": id, "channel": "work", "owner": "tester", "lease_ms": 1000, "max_attempts": 3, "labels": labels}),
        );
    }
    fn claim(&mut self, id: &str, at: i64, claim: &str, profile: &str, n: u32) {
        self.task(
            id,
            at,
            "TaskClaimed",
            json!({"claim_id": claim, "holder": {"person": "tester", "profile": profile}, "expires_at": at + 1000, "attempt": n}),
        );
    }
    fn usage(&mut self, task: &str, at: i64, data: Value) {
        self.push(
            format!("thread:{task}"),
            "MessagePosted",
            at,
            json!({"id": format!("m{at}"), "thread_id": task, "channel": "work", "parent": task,
                   "text": "usage", "labels": {"usage": "attempt"}, "data": data}),
        );
    }
}

/// A: haiku fails at 1100, escalated as B, which sonnet completes.
/// C: haiku's lease expires (wrapper killed, no usage), haiku again completes.
/// D: never claimed. E: sonnet blocks for 1000 ms, then completes.
fn script() -> Vec<EventOut> {
    let mut l = Log::new();
    l.create("A", 0, json!({}));
    l.create("C", 0, json!({"parent": "F"}));
    l.create("D", 10, json!({}));
    l.create("E", 20, json!({"parent": "F"}));
    l.claim("E", 30, "e1", "f-sonnet-1", 1);
    l.task(
        "E",
        40,
        "TaskBlocked",
        json!({"claim_id": "e1", "question": "which?"}),
    );
    l.usage(
        "E",
        40,
        json!({"claim_id": "e1", "model": "sonnet", "cost_usd": 0.2}),
    );
    l.claim("A", 100, "a1", "f-haiku-1", 1);
    l.claim("C", 200, "c1", "f-haiku-2", 1);
    l.task("E", 1040, "TaskUnblocked", json!({}));
    l.usage("A", 1100, json!({"claim_id": "a1", "model": "haiku", "cost_usd": 0.1, "input_tokens": 1000, "output_tokens": 100, "turns": 4}));
    l.task(
        "A",
        1100,
        "TaskFailed",
        json!({"claim_id": "a1", "reason": "tests fail\nlong log"}),
    );
    l.claim("E", 1100, "e2", "f-sonnet-1", 2);
    l.usage(
        "E",
        1200,
        json!({"claim_id": "e2", "model": "sonnet", "cost_usd": 0.2}),
    );
    l.task("E", 1200, "TaskCompleted", json!({"claim_id": "e2"}));
    l.create("B", 1200, json!({"retry_of": "A", "escalation": "1"}));
    l.claim("B", 1500, "b1", "f-sonnet-1", 1);
    l.task("C", 2200, "LeaseExpired", json!({"claim_id": "c1"}));
    l.claim("C", 2300, "c2", "f-haiku-1", 2);
    // Tokens and no cost: priced from the table.
    l.usage(
        "C",
        3300,
        json!({"claim_id": "c2", "model": "haiku", "input_tokens": 1_000_000, "output_tokens": 0}),
    );
    l.task("C", 3300, "TaskCompleted", json!({"claim_id": "c2"}));
    l.usage("B", 4500, json!({"claim_id": "b1", "model": "sonnet", "cost_usd": 0.5, "input_tokens": 2000, "output_tokens": 300, "turns": 9}));
    l.task("B", 4500, "TaskCompleted", json!({"claim_id": "b1"}));
    l.events
}

fn opts(by: By) -> Options {
    Options {
        by: Some(by),
        prices: [(
            "haiku".to_string(),
            Price {
                input: 1.0,
                ..Default::default()
            },
        )]
        .into_iter()
        .collect(),
        fleets: vec!["f".into()],
        models: ["haiku".to_string(), "sonnet".to_string()].into(),
        ..Default::default()
    }
}

fn group<'a>(r: &'a Report, k: &str) -> &'a Group {
    r.groups.iter().find(|g| g.key == k).unwrap()
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

#[test]
fn flow_metrics_match_the_script() {
    let r = report(&script(), &opts(By::Model));
    let t = &r.total;
    assert_eq!((t.tasks, t.completed, t.failed, t.open), (5, 3, 1, 1));
    assert_eq!(t.success_rate, Some(0.75));
    assert_eq!(t.attempts, 6);
    assert_eq!(t.lease_expiries, 1);
    assert_eq!(t.blocked_ms, 1000);
    assert_eq!((t.escalated, t.escalated_completed), (1, 1));

    let h = group(&r, "haiku");
    assert_eq!(h.tasks, 2);
    // Claims waited 100, 200 and 100 ms; work took 1000, 2000 and 1000.
    assert_eq!(
        h.time_to_claim,
        Pcts {
            n: 3,
            p50_ms: Some(100),
            p95_ms: Some(200)
        }
    );
    assert_eq!(
        h.work_time,
        Pcts {
            n: 3,
            p50_ms: Some(1000),
            p95_ms: Some(2000)
        }
    );
    assert_eq!(
        h.time_to_done,
        Pcts {
            n: 1,
            p50_ms: Some(3300),
            p95_ms: Some(3300)
        }
    );
    assert_eq!(h.lease_expiries, 1);
    assert_eq!(h.attempts_per_task, Some(1.5));
    // A's first attempt failed, C's expired.
    assert_eq!(h.first_attempt_success, Some(0.0));

    let s = group(&r, "sonnet");
    assert_eq!(s.tasks, 2);
    assert_eq!(
        s.time_to_claim,
        Pcts {
            n: 3,
            p50_ms: Some(60),
            p95_ms: Some(300)
        }
    );
    assert_eq!(
        s.work_time,
        Pcts {
            n: 3,
            p50_ms: Some(100),
            p95_ms: Some(3000)
        }
    );
    assert_eq!(s.time_to_done.p50_ms, Some(1180));
    assert_eq!(s.time_to_done.p95_ms, Some(3300));
    assert_eq!((s.escalated, s.escalated_completed), (1, 1));
    assert_eq!(s.blocked_ms, 1000);
    assert_eq!(group(&r, UNCLAIMED).tasks, 1);

    assert_eq!(r.queue.peak, 4);
    assert_eq!(r.queue.last, 1, "D is still ready");
    // Ready-task milliseconds, worked out from the script, over 4500 ms.
    let area = 2 * 10
        + 3 * 10
        + 4 * 10
        + 3 * 70
        + 2 * 100
        + 840
        + 2 * 60
        + 100
        + 2 * 300
        + 700
        + 2 * 100
        + 2200;
    assert!(
        close(r.queue.mean, area as f64 / 4500.0),
        "{}",
        r.queue.mean
    );
}

#[test]
fn an_escalation_chain_is_charged_to_the_model_it_started_on() {
    let r = report(&script(), &opts(By::Model));
    let h = group(&r, "haiku");
    // Chain A+B: 0.1 + 0.5. Chain C: 1.0 priced, plus an attempt of unknown cost.
    assert_eq!((h.chains, h.chains_completed), (2, 2));
    assert!(close(h.chain_cost_usd, 1.6));
    assert!(close(h.cost_per_success.unwrap(), 0.8));
    assert!(
        h.cost_incomplete,
        "C's killed attempt makes it a lower bound"
    );
    let s = group(&r, "sonnet");
    assert_eq!((s.chains, s.chains_completed), (1, 1));
    assert!(close(s.cost_per_success.unwrap(), 0.4));
    assert!(!s.cost_incomplete);
    assert_eq!(group(&r, UNCLAIMED).chains_completed, 0);

    let split: Vec<(&str, &str, u64)> = r
        .finished_by
        .iter()
        .map(|s| (s.started.as_str(), s.finished_model.as_str(), s.completed))
        .collect();
    assert_eq!(
        split,
        vec![
            ("haiku", "haiku", 1),
            ("haiku", "sonnet", 1),
            ("sonnet", "sonnet", 1)
        ]
    );
    assert!(close(r.finished_by[1].cost_usd, 0.6));
    assert!(close(r.total.cost_per_success.unwrap(), 2.0 / 3.0));
}

#[test]
fn unknown_usage_is_counted_not_zero() {
    let r = report(&script(), &opts(By::Model));
    let h = group(&r, "haiku");
    assert_eq!(h.usage.unknown, 1);
    assert_eq!(h.usage.reported, 2);
    assert_eq!(h.usage.cost_sources["agent"], 1);
    assert_eq!(h.usage.cost_sources["price"], 1);
    assert!(close(h.usage.cost_usd, 1.1));
    assert_eq!(h.usage.input_tokens, 1_001_000);
    let text = render_text(&r);
    assert!(text.contains("1 attempts have unknown usage"), "{text}");
    assert!(text.contains("$0.80?"), "{text}");
}

#[test]
fn without_a_usage_reply_the_pool_in_the_profile_names_the_model() {
    let mut o = opts(By::Model);
    o.fleets.clear();
    o.models = ["haiku".to_string(), "sonnet".to_string()].into();
    let mut ev = script();
    ev.retain(|e| e.event_type != "MessagePosted");
    let r = report(&ev, &o);
    assert_eq!(group(&r, "haiku").attempts, 3);
    assert_eq!(group(&r, "sonnet").usage.unknown, 3);
    let r = report(&ev, &opts(By::Pool));
    assert_eq!(group(&r, "haiku").attempts, 3);
}

#[test]
fn other_groupings_and_filters() {
    let r = report(&script(), &opts(By::Profile));
    assert_eq!(group(&r, "f-haiku-1").attempts, 2);
    let r = report(&script(), &opts(By::Label("parent".into())));
    assert_eq!(group(&r, "F").tasks, 2);
    assert_eq!(group(&r, NONE).tasks, 3);
    let r = report(&script(), &opts(By::Day));
    assert_eq!(r.groups[0].key, "1970-01-01");

    let mut o = opts(By::Model);
    o.feature = Some("F".into());
    let r = report(&script(), &o);
    assert_eq!(r.total.tasks, 2);
    let mut o = opts(By::Model);
    o.since = Some(1000);
    assert_eq!(report(&script(), &o).total.tasks, 1);
    let mut o = opts(By::Model);
    o.labels = vec![("retry_of".into(), "A".into())];
    assert_eq!(report(&script(), &o).total.tasks, 1);
}

#[test]
fn reasons_only_when_asked() {
    let r = report(&script(), &opts(By::Model));
    assert!(r.reasons.is_none());
    assert!(!render_text(&r).contains("tests fail"));
    let mut o = opts(By::Model);
    o.reasons = true;
    let r = report(&script(), &o);
    assert_eq!(r.reasons, Some(vec![("tests fail".to_string(), 1)]));
}

#[test]
fn csv_has_a_row_per_group_and_the_total() {
    let r = report(&script(), &opts(By::Model));
    let csv = render_csv(&r);
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(lines.len(), 1 + r.groups.len() + 1);
    assert!(lines[0].starts_with("model,tasks,completed"));
    assert!(lines.last().unwrap().starts_with("all,5,3,1,0,1,0.75"));
}

#[test]
fn helpers() {
    assert_eq!(day(0), "1970-01-01");
    assert_eq!(day(1_795_770_556_611), "2026-11-27");
    assert_eq!(day(951_782_400_000), "2000-02-29");
    assert_eq!(fmt_ms(Some(850)), "850ms");
    assert_eq!(fmt_ms(Some(4_200)), "4.2s");
    assert_eq!(fmt_ms(Some(240_000)), "4.0m");
    assert_eq!(fmt_tokens(21_000_000), "21.0M");
    let o = Options {
        fleets: vec!["my-fleet".into()],
        ..Default::default()
    };
    assert_eq!(
        pool_of_profile("my-fleet-big-pool-12", &o).as_deref(),
        Some("big-pool")
    );
    assert_eq!(pool_of_profile("w", &o), None);
    assert_eq!(
        pool_of_profile("f-sonnet-1", &Options::default()).as_deref(),
        Some("sonnet")
    );
    assert_eq!(pcts(vec![]), Pcts::default());
    assert_eq!(pcts(vec![5]).p95_ms, Some(5));
}
