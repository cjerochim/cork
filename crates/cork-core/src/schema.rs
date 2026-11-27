//! Event schema versions and upcasters.
//!
//! Every stored event carries `schema`, the version of its `data` shape.
//! When a shape changes, bump its entry in [`current`] and add a step to
//! [`upcast`] that turns the old shape into the next one. Readers always see
//! the current shape.

use serde_json::Value;

use crate::error::{BoardError, Result};

/// The current schema version for an event type. Every type starts at 1.
///
/// - `TaskCreated` 2: adds `depends_on`, `replaces` and `open_dependencies`.
/// - `TaskReopened` 2: adds `open_dependencies` (HANDOFF 6.8.4).
pub fn current(event_type: &str) -> u32 {
    match event_type {
        "TaskCreated" | "TaskReopened" => 2,
        _ => 1,
    }
}

/// Bring `data` stored at `schema` up to the current shape for `event_type`.
pub fn upcast(event_type: &str, schema: u32, data: Value) -> Result<Value> {
    let target = current(event_type);
    if schema == 0 || schema > target {
        return Err(BoardError::internal(format!(
            "event {event_type} has schema {schema}, this build knows up to {target}"
        )));
    }
    let mut data = data;
    let mut at = schema;
    while at < target {
        data = match (event_type, at) {
            // Schema 1 had no dependencies: empty lists and no `replaces`,
            // which is what the missing fields already read as.
            ("TaskCreated" | "TaskReopened", 1) => data,
            _ => {
                return Err(BoardError::internal(format!(
                    "no upcast for {event_type} from schema {at}"
                )));
            }
        };
        at += 1;
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn current_shape_passes_through() {
        let d = json!({"claim_id": "c1"});
        assert_eq!(upcast("TaskReleased", 1, d.clone()).unwrap(), d);
    }

    #[test]
    fn milestone_one_task_events_upcast() {
        let d = json!({"task_id": "t", "channel": "dev"});
        assert_eq!(upcast("TaskCreated", 1, d.clone()).unwrap(), d);
        assert_eq!(upcast("TaskReopened", 1, json!({})).unwrap(), json!({}));
        assert_eq!(current("TaskCreated"), 2);
    }

    #[test]
    fn unknown_future_schema_is_internal() {
        let e = upcast("TaskReleased", 2, json!({})).unwrap_err();
        assert_eq!(e.code, crate::ErrorCode::Internal);
    }
}
