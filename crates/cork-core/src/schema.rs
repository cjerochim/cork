//! Event schema versions and upcasters.
//!
//! Every stored event carries `schema`, the version of its `data` shape.
//! When a shape changes, bump its entry in [`current`] and add a step to
//! [`upcast`] that turns the old shape into the next one. Readers always see
//! the current shape.

use serde_json::Value;

use crate::error::{BoardError, Result};

/// The current schema version for an event type. Every type starts at 1.
pub fn current(_event_type: &str) -> u32 {
    1
}

/// Bring `data` stored at `schema` up to the current shape for `event_type`.
pub fn upcast(event_type: &str, schema: u32, data: Value) -> Result<Value> {
    let target = current(event_type);
    if schema == 0 || schema > target {
        return Err(BoardError::internal(format!(
            "event {event_type} has schema {schema}, this build knows up to {target}"
        )));
    }
    // No shape has changed yet, so there are no steps. Add them here as
    // `(type, from) => transform`, one version at a time.
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
    fn unknown_future_schema_is_internal() {
        let e = upcast("TaskReleased", 2, json!({})).unwrap_err();
        assert_eq!(e.code, crate::ErrorCode::Internal);
    }
}
