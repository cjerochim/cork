use crate::error::{BoardError, ErrorCode, Result};
use std::collections::BTreeMap;
use unicode_normalization::UnicodeNormalization;

/// `key=value` pairs on a message. One value per key.
pub type Labels = BTreeMap<String, String>;

/// Prefix of labels owned by the board. Users cannot set these.
pub const RESERVED_PREFIX: &str = "board.";
/// The task state label on a task's root message.
pub const TASK_LABEL: &str = "board.task";
/// Index label for each `@name` in a message. May repeat on one message.
pub const MENTION_LABEL: &str = "board.mention";

pub const MAX_LABELS: usize = 32;
pub const MAX_KEY_LEN: usize = 64;
pub const MAX_VALUE_LEN: usize = 256;

pub fn is_reserved(key: &str) -> bool {
    key.starts_with(RESERVED_PREFIX)
}

fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_KEY_LEN
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/'))
}

/// Parse `k=v`. Does not check for reserved keys; use [`validate_user_labels`].
pub fn parse_label(s: &str) -> Result<(String, String)> {
    let (k, v) = s
        .split_once('=')
        .ok_or_else(|| BoardError::invalid(format!("label `{s}` must be key=value")))?;
    let (k, v) = (k.trim(), v.trim());
    if !valid_key(k) {
        return Err(BoardError::invalid(format!(
            "label key `{k}` must be 1-{MAX_KEY_LEN} chars of [A-Za-z0-9_./-]"
        )));
    }
    if v.len() > MAX_VALUE_LEN {
        return Err(BoardError::invalid(format!(
            "label value for `{k}` is over {MAX_VALUE_LEN} bytes"
        )));
    }
    Ok((k.to_string(), v.nfc().collect()))
}

/// Labels for a filter. Reserved keys are allowed (agents filter on `board.task`).
pub fn parse_filter(items: &[String]) -> Result<Vec<(String, String)>> {
    items.iter().map(|s| parse_label(s)).collect()
}

/// Check labels a user wants to write, and return them normalized (values NFC).
pub fn normalize_user_labels(labels: Labels) -> Result<Labels> {
    validate_user_labels(&labels)?;
    Ok(labels
        .into_iter()
        .map(|(k, v)| (k, v.nfc().collect()))
        .collect())
}

/// Check labels a user wants to write.
pub fn validate_user_labels(labels: &Labels) -> Result<()> {
    if labels.len() > MAX_LABELS {
        return Err(BoardError::invalid(format!(
            "at most {MAX_LABELS} labels per message"
        )));
    }
    for (k, v) in labels {
        if is_reserved(k) {
            return Err(BoardError::new(
                ErrorCode::ReservedLabel,
                format!("labels under `{RESERVED_PREFIX}` belong to the board: `{k}`"),
            ));
        }
        parse_label(&format!("{k}={v}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses() {
        assert_eq!(
            parse_label("step=fix").unwrap(),
            ("step".into(), "fix".into())
        );
        assert_eq!(parse_label("a=").unwrap(), ("a".into(), "".into()));
        assert!(parse_label("nokey").is_err());
        assert!(parse_label("=v").is_err());
        assert!(parse_label("sp ace=v").is_err());
    }

    #[test]
    fn rejects_reserved() {
        let mut l = Labels::new();
        l.insert("board.task".into(), "completed".into());
        assert_eq!(
            validate_user_labels(&l).unwrap_err().code,
            ErrorCode::ReservedLabel
        );
        let mut ok = Labels::new();
        ok.insert("step".into(), "fix".into());
        validate_user_labels(&ok).unwrap();
    }

    #[test]
    fn values_are_nfc() {
        // "é" as e + combining acute becomes the single code point.
        let (_, v) = parse_label("name=cafe\u{301}").unwrap();
        assert_eq!(v, "caf\u{e9}");
        let mut l = Labels::new();
        l.insert("name".into(), "cafe\u{301}".into());
        assert_eq!(normalize_user_labels(l).unwrap()["name"], "caf\u{e9}");
    }

    #[test]
    fn limits_count() {
        let l: Labels = (0..=MAX_LABELS)
            .map(|i| (format!("k{i}"), "v".to_string()))
            .collect();
        assert_eq!(
            validate_user_labels(&l).unwrap_err().code,
            ErrorCode::InvalidInput
        );
    }
}
