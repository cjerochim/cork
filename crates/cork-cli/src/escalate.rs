//! `cork tasks escalate <id>`: retry a struggling task one tier up, from the
//! branch the earlier attempt left (HANDOFF 16.7). Safe to re-run: every
//! write carries an idempotency key made from the old task's id.

use crate::workers::Loaded;
use cork_api::{CreateTaskReq, PostMessage, SettleReq, TaskOut};
use cork_client::{Client, ClientError};
use cork_core::{Actor, Labels, TaskState};
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

    let key = escalation::idempotency_key(id);
    let req = CreateTaskReq {
        channel: t.task.channel.clone(),
        text: plan.text.clone(),
        labels: plan.labels.clone(),
        data: None,
        lease_ms: Some(t.task.lease_ms),
        max_attempts: Some(t.task.max_attempts),
        assignee: None,
    };
    let v = client.create_task(&req, Some(&key)).await?;
    let new = v["task"]["id"].as_str().unwrap_or_default().to_string();

    // Tidy the old task: cancel it if nobody holds it, and link both ways.
    let state = t.task.effective_state(t.now);
    let canceled = if matches!(state, TaskState::Ready | TaskState::Blocked) {
        let req = SettleReq {
            reason: Some(format!("escalated to {new}")),
            ..Default::default()
        };
        match client
            .settle(id, "cancel", &req, Some(&format!("{key}-cancel")))
            .await
        {
            Ok(_) => true,
            // Claimed or settled since we looked: leave it be.
            Err(e) if e.code == "invalid_state" || e.code == "task_claimed" => false,
            Err(e) => return Err(e),
        }
    } else {
        false
    };
    let note = PostMessage {
        text: format!(
            "Escalated to {new}: a retry on tier {} (needs {}).",
            plan.tier,
            plan.needs.iter().cloned().collect::<Vec<_>>().join(", ")
        ),
        labels: Labels::new(),
        data: Some(json!({"retry": new, "tier": plan.tier})),
    };
    client
        .reply(id, &note, Some(&format!("{key}-reply")))
        .await?;

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
