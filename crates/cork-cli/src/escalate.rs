//! `cork tasks escalate <id>`: retry a struggling task one tier up, from the
//! branch the earlier attempt left (HANDOFF 16.7). Safe to re-run: the
//! write carries an idempotency key made from the old task's id.
//!
//! The retry declares `replaces` (6.8.3): the board ends the old task, links
//! both ways, and tasks waiting on the old one now wait on the retry.

use crate::workers::Loaded;
use cork_api::{CreateTaskReq, TaskOut};
use cork_client::{Client, ClientError};
use cork_core::{Actor, TaskState};
use cork_fleet::escalation::{self, Trail};
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn internal(e: impl std::fmt::Display) -> ClientError {
    ClientError::new("internal", e.to_string())
}

pub async fn escalate(
    client: &Client,
    l: &Loaded,
    id: &str,
    extra: &[String],
    max: u32,
    json_out: bool,
) -> Result<i32, ClientError> {
    let c = &l.catalog;
    let extra: BTreeSet<String> = c.resolve_list(extra).map_err(|bad| {
        ClientError::new(
            "usage",
            bad.iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("\n"),
        )
    })?;
    let t: TaskOut = serde_json::from_value(client.task(id).await?).map_err(internal)?;
    let history = client.history(id).await?;
    let thread = client.thread(id).await?;
    let trail = trail(&t, &history, &thread);
    let last_tier = l
        .fleet
        .as_ref()
        .zip(trail.claimers.last())
        .and_then(|(f, p)| escalation::tier_of_profile(f, p));
    let tried = escalation::tried_tier(c, &t.task, last_tier);
    let plan = match escalation::plan(c, &t.task, &t.root.message.text, &trail, tried, &extra, max)
    {
        Ok(p) => p,
        Err(e) => {
            eprintln!("cork: {e}");
            return Ok(3);
        }
    };

    // A live worker is never cut off, and finished work is never redone.
    let state = t.task.effective_state(t.now);
    if matches!(state, TaskState::Claimed | TaskState::Completed) {
        eprintln!(
            "cork: task {id} is {state}; escalate a task once nobody holds it and it did not complete"
        );
        return Ok(3);
    }
    let key = escalation::idempotency_key(id);
    let req = CreateTaskReq {
        channel: t.task.channel.clone(),
        text: plan.text.clone(),
        labels: plan.labels.clone(),
        data: None,
        lease_ms: Some(t.task.lease_ms),
        max_attempts: Some(t.task.max_attempts),
        assignee: None,
        depends_on: vec![],
        replaces: Some(id.to_string()),
    };
    let v = client.create_task(&req, Some(&key)).await?;
    let new = v["task"]["id"].as_str().unwrap_or_default().to_string();
    // The board canceled it unless it had already failed or been canceled.
    let canceled = !matches!(state, TaskState::Failed | TaskState::Canceled);

    if json_out {
        crate::print_json(&json!({
            "retry": new,
            "retry_of": id,
            "tried_tier": plan.tried_tier,
            "tier": plan.tier,
            "escalation": plan.escalation,
            "needs": plan.needs,
            "models": plan.models,
            "canceled_old": canceled,
            "replaced": true,
        }));
    } else {
        println!("{new}");
        eprintln!(
            "cork: retry of {id} on tier {} (cheapest: {}){}",
            plan.tier,
            plan.models.first().map(String::as_str).unwrap_or("?"),
            if canceled {
                "; canceled the old task"
            } else {
                ""
            }
        );
    }
    Ok(0)
}

/// Attempts, claimers, the last failure, and progress posts from claim
/// holders and people (as `tasks run` trusts them for CORK_PREVIOUS).
fn trail(t: &TaskOut, history: &Value, thread: &Value) -> Trail {
    let mut out = Trail {
        reason: t.task.reason.clone(),
        ..Default::default()
    };
    let mut trusted: Vec<String> = vec![];
    for e in history["events"].as_array().into_iter().flatten() {
        if e["type"] == "TaskClaimed"
            && let Ok(a) = serde_json::from_value::<Actor>(e["data"]["holder"].clone())
        {
            out.attempts += 1;
            out.claimers.push(a.handle().to_string());
            trusted.push(a.to_string());
        }
        if e["type"] == "TaskFailed"
            && let Some(r) = e["data"]["reason"].as_str()
        {
            out.reason = Some(r.to_string());
        }
    }
    for r in thread["replies"].as_array().into_iter().flatten() {
        let Ok(a) = serde_json::from_value::<Actor>(r["actor"].clone()) else {
            continue;
        };
        if trusted.contains(&a.to_string()) {
            let mut s = r["text"].as_str().unwrap_or("").to_string();
            if !r["data"].is_null() {
                s.push_str(&format!(" {}", r["data"]));
            }
            out.notes.push(format!("{}: {s}", a.handle()));
        }
    }
    out
}
