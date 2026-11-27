use crate::actor::{Actor, normalize_name};
use crate::error::{BoardError, Result};
use crate::labels::{Labels, normalize_user_labels};
use crate::{Millis, normalize_channel};
use serde::{Deserialize, Serialize};

pub const MAX_TEXT: usize = 64 * 1024;
pub const MAX_DATA: usize = 256 * 1024;

/// What a caller asks to post.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NewMessage {
    pub text: String,
    #[serde(default)]
    pub labels: Labels,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// A message as stored. This is also the body of the `MessagePosted` event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub thread_id: String,
    pub channel: String,
    /// `None` for a thread's root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub text: String,
    /// User labels, plus board labels such as `board.task` on a task root.
    #[serde(default)]
    pub labels: Labels,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<String>,
    pub actor: Actor,
    pub at: Millis,
}

pub const MESSAGE_POSTED: &str = "MessagePosted";

impl Message {
    pub fn is_root(&self) -> bool {
        self.parent.is_none()
    }
}

pub const MAX_MENTIONS: usize = 32;

/// Reserved message event types (Milestone 2). Named now so the log format is stable.
pub const MESSAGE_EDITED: &str = "MessageEdited";
pub const MESSAGE_DELETED: &str = "MessageDeleted";
pub const MESSAGE_REDACTED: &str = "MessageRedacted";

/// Every `@handle` in the text, lowercased, in order, without duplicates.
/// A mention starts a word and sits outside code spans and fenced code blocks.
pub fn extract_mentions(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut in_fence = false;
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence {
            continue;
        }
        let bytes = line.as_bytes();
        let mut in_code = false;
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if b == b'`' {
                in_code = !in_code;
                i += 1;
                continue;
            }
            let word_start = i == 0
                || !(bytes[i - 1].is_ascii_alphanumeric()
                    || matches!(bytes[i - 1], b'_' | b'.' | b'-' | b'@'));
            if !in_code && b == b'@' && word_start {
                let start = i + 1;
                let mut end = start;
                while end < bytes.len()
                    && (bytes[end].is_ascii_alphanumeric()
                        || matches!(bytes[end], b'_' | b'-' | b'.'))
                {
                    end += 1;
                }
                // A trailing `.` is punctuation, not part of the name.
                while end > start && bytes[end - 1] == b'.' {
                    end -= 1;
                }
                if end > start
                    && let Ok(name) = normalize_name("mention", &line[start..end])
                    && !out.contains(&name)
                {
                    out.push(name);
                }
                i = end.max(i + 1);
            } else {
                i += 1;
            }
        }
    }
    out
}

/// Validate a new message and return its normalized labels and mentions.
fn validate(new: &NewMessage) -> Result<(Labels, Vec<String>)> {
    if new.text.trim().is_empty() {
        return Err(BoardError::invalid("message text is empty"));
    }
    if new.text.len() > MAX_TEXT {
        return Err(BoardError::invalid(format!(
            "message text is over {MAX_TEXT} bytes"
        )));
    }
    if let Some(d) = &new.data
        && serde_json::to_vec(d).map(|v| v.len()).unwrap_or(usize::MAX) > MAX_DATA
    {
        return Err(BoardError::invalid(format!(
            "message data is over {MAX_DATA} bytes"
        )));
    }
    let labels = normalize_user_labels(new.labels.clone())?;
    let mentions = extract_mentions(&new.text);
    if mentions.len() > MAX_MENTIONS {
        return Err(BoardError::invalid(format!(
            "at most {MAX_MENTIONS} mentions per message"
        )));
    }
    Ok((labels, mentions))
}

/// Decide a new thread root.
pub fn post_root(
    id: String,
    channel: &str,
    new: NewMessage,
    actor: Actor,
    now: Millis,
) -> Result<Message> {
    let (labels, mentions) = validate(&new)?;
    let channel = normalize_channel(channel)?;
    Ok(Message {
        thread_id: id.clone(),
        id,
        channel,
        parent: None,
        mentions,
        text: new.text,
        labels,
        data: new.data,
        actor,
        at: now,
    })
}

/// Decide a reply in the thread of `root`.
pub fn post_reply(
    id: String,
    root: &Message,
    new: NewMessage,
    actor: Actor,
    now: Millis,
) -> Result<Message> {
    let (labels, mentions) = validate(&new)?;
    Ok(Message {
        id,
        thread_id: root.thread_id.clone(),
        channel: root.channel.clone(),
        parent: Some(root.thread_id.clone()),
        mentions,
        text: new.text,
        labels,
        data: new.data,
        actor,
        at: now,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorCode;

    #[test]
    fn mentions() {
        assert_eq!(
            extract_mentions("hi @chris and @fixer-1. @chris again"),
            vec!["chris", "fixer-1"]
        );
        assert!(extract_mentions("mail a@b.com").is_empty());
        assert!(extract_mentions("@ alone").is_empty());
        assert_eq!(extract_mentions("Hey @Chris"), vec!["chris"]);
        assert!(extract_mentions("run `@notme` please").is_empty());
        assert!(extract_mentions("```\n@notme\n```\n").is_empty());
        assert_eq!(extract_mentions("```\n@notme\n```\n@me"), vec!["me"]);
    }

    #[test]
    fn caps_mentions() {
        let text: String = (0..=MAX_MENTIONS).map(|i| format!("@u{i} ")).collect();
        let e = post_root(
            "m".into(),
            "dev",
            NewMessage {
                text,
                ..Default::default()
            },
            Actor::person("chris"),
            1,
        )
        .unwrap_err();
        assert_eq!(e.code, crate::ErrorCode::InvalidInput);
    }

    #[test]
    fn validates() {
        let a = Actor::person("chris");
        let ok = post_root(
            "m1".into(),
            "dev",
            NewMessage {
                text: "hi".into(),
                ..Default::default()
            },
            a.clone(),
            1,
        )
        .unwrap();
        assert_eq!(ok.thread_id, "m1");
        assert!(ok.is_root());
        let e = post_root(
            "m2".into(),
            "dev",
            NewMessage {
                text: "  ".into(),
                ..Default::default()
            },
            a.clone(),
            1,
        )
        .unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        let mut labels = Labels::new();
        labels.insert("board.task".into(), "ready".into());
        let e = post_root(
            "m3".into(),
            "dev",
            NewMessage {
                text: "x".into(),
                labels,
                data: None,
            },
            a.clone(),
            1,
        )
        .unwrap_err();
        assert_eq!(e.code, ErrorCode::ReservedLabel);
        let r = post_reply(
            "m4".into(),
            &ok,
            NewMessage {
                text: "re".into(),
                ..Default::default()
            },
            a,
            2,
        )
        .unwrap();
        assert_eq!(r.thread_id, "m1");
        assert_eq!(r.channel, "dev");
        assert_eq!(r.parent.as_deref(), Some("m1"));
    }
}
