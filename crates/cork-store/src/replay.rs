//! Projections derived from the log alone, for import and rebuild
//! (HANDOFF 8.2, 13.4). Adapters write these rows; they never decide them.

use crate::{Position, StoredEvent};
use cork_core::message::MESSAGE_POSTED;
use cork_core::{BoardError, Message, Result, Task, TaskEvent};
use std::collections::BTreeMap;

/// Every projection row the log implies.
#[derive(Debug, Default)]
pub struct Derived {
    /// Each message with its position, in log order. Task roots carry
    /// `board.task` with the task's stored state.
    pub messages: Vec<(Message, Position)>,
    /// Each task folded from its stream, with its queue place (the position
    /// of its first event).
    pub tasks: Vec<(Task, Position)>,
}

pub fn derive(events: &[StoredEvent]) -> Result<Derived> {
    let mut messages = vec![];
    let mut streams: BTreeMap<&str, Vec<&StoredEvent>> = BTreeMap::new();
    for e in events {
        if e.event_type == MESSAGE_POSTED {
            let m: Message = serde_json::from_value(e.data.clone())
                .map_err(|err| BoardError::internal(format!("message at {}: {err}", e.position)))?;
            messages.push((m, e.position));
        } else if e.stream_id.starts_with("task:") {
            streams.entry(&e.stream_id).or_default().push(e);
        }
    }
    let mut tasks = vec![];
    for evs in streams.values() {
        let parsed: Vec<(TaskEvent, i64)> = evs
            .iter()
            .map(|e| {
                serde_json::from_value(e.data.clone())
                    .map(|t| (t, e.at))
                    .map_err(|err| {
                        BoardError::internal(format!("task event at {}: {err}", e.position))
                    })
            })
            .collect::<Result<_>>()?;
        let task = Task::from_events(parsed.iter().map(|(e, at)| (e, *at)))?;
        tasks.push((task, evs[0].position));
    }
    let state: BTreeMap<&str, &str> = tasks
        .iter()
        .map(|(t, _)| (t.id.as_str(), t.state.as_str()))
        .collect();
    for (m, _) in &mut messages {
        if let Some(s) = state.get(m.id.as_str()) {
            m.labels
                .insert(cork_core::labels::TASK_LABEL.into(), (*s).into());
        }
    }
    tasks.sort_by_key(|(_, p)| *p);
    Ok(Derived { messages, tasks })
}
