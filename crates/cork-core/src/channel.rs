use crate::error::{BoardError, Result};
use serde::{Deserialize, Serialize};

pub const MAX_DEPTH: usize = 8;
pub const MAX_LEN: usize = 200;

/// True if `s` reads as a ULID (26 Crockford base32 chars). Such a string is
/// always an id, never a channel segment, so the CLI can tell them apart.
pub fn looks_like_ulid(s: &str) -> bool {
    s.len() == 26
        && s.bytes().all(|b| {
            let b = b.to_ascii_uppercase();
            b.is_ascii_digit()
                || (b.is_ascii_uppercase() && !matches!(b, b'I' | b'L' | b'O' | b'U'))
        })
}

/// Normalize a channel path: `/work/fix/` becomes `work/fix`.
pub fn normalize_channel(s: &str) -> Result<String> {
    let trimmed = s.trim().trim_matches('/');
    if trimmed.is_empty() {
        return Err(BoardError::invalid("channel path is empty"));
    }
    if trimmed.len() > MAX_LEN {
        return Err(BoardError::invalid(format!(
            "channel path is over {MAX_LEN} bytes"
        )));
    }
    let segs: Vec<&str> = trimmed.split('/').collect();
    if segs.len() > MAX_DEPTH {
        return Err(BoardError::invalid(format!(
            "channel path is deeper than {MAX_DEPTH}"
        )));
    }
    for seg in &segs {
        if seg.is_empty()
            || !seg
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
            || *seg == "."
            || *seg == ".."
        {
            return Err(BoardError::invalid(format!(
                "channel segment `{seg}` must be [A-Za-z0-9_.-]"
            )));
        }
        if looks_like_ulid(seg) {
            return Err(BoardError::invalid(format!(
                "channel segment `{seg}` looks like an id"
            )));
        }
    }
    Ok(segs.join("/"))
}

/// Which channels a read, wait or claim covers.
/// `work` means that channel only. `work/` means `work` and everything under it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelSelector {
    Exact(String),
    Subtree(String),
}

impl ChannelSelector {
    pub fn parse(s: &str) -> Result<Self> {
        let path = normalize_channel(s)?;
        Ok(if s.trim().ends_with('/') {
            Self::Subtree(path)
        } else {
            Self::Exact(path)
        })
    }
    pub fn matches(&self, channel: &str) -> bool {
        match self {
            Self::Exact(p) => channel == p,
            Self::Subtree(p) => {
                channel == p
                    || (channel.len() > p.len()
                        && channel.starts_with(p.as_str())
                        && channel.as_bytes()[p.len()] == b'/')
            }
        }
    }
    /// The form a client sends: with a trailing `/` for a subtree.
    pub fn to_param(&self) -> String {
        match self {
            Self::Exact(p) => p.clone(),
            Self::Subtree(p) => format!("{p}/"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn normalizes() {
        assert_eq!(normalize_channel("/work/fix/").unwrap(), "work/fix");
        assert!(normalize_channel("").is_err());
        assert!(normalize_channel("a//b").is_err());
        assert!(normalize_channel("a/../b").is_err());
        assert!(normalize_channel("a b").is_err());
        assert!(normalize_channel("work/01JB2X3Y4Z5A6B7C8D9E0F1G2H").is_err());
        assert!(looks_like_ulid("01JB2X3Y4Z5A6B7C8D9E0F1G2H"));
        assert!(!looks_like_ulid("work"));
    }
    #[test]
    fn selects() {
        let s = ChannelSelector::parse("work/").unwrap();
        assert!(s.matches("work"));
        assert!(s.matches("work/fix"));
        assert!(!s.matches("workshop"));
        let e = ChannelSelector::parse("work").unwrap();
        assert!(e.matches("work"));
        assert!(!e.matches("work/fix"));
    }
}
