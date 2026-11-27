use crate::error::{BoardError, Result};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Who wrote something: a person, or a profile acting for a person.
/// A profile is only a name. It has no access of its own.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Actor {
    pub person: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

/// The person name used for writes the daemon makes itself (lease expiry, restart grace).
pub const SYSTEM_PERSON: &str = "cork";

/// Names are ASCII `[A-Za-z0-9_.-]`, 1-64 chars, stored and compared in lowercase.
pub fn normalize_name(kind: &str, s: &str) -> Result<String> {
    validate_name(kind, s)?;
    Ok(s.to_ascii_lowercase())
}

pub fn validate_name(kind: &str, s: &str) -> Result<()> {
    if s.is_empty()
        || s.len() > 64
        || !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
    {
        return Err(BoardError::invalid(format!(
            "{kind} `{s}` must be 1-64 chars of [A-Za-z0-9_.-]"
        )));
    }
    Ok(())
}

impl Actor {
    /// A person with no profile. The name must already be valid; use [`Actor::new`] for input.
    pub fn person(person: impl Into<String>) -> Self {
        Self {
            person: person.into().to_ascii_lowercase(),
            profile: None,
        }
    }
    pub fn new(person: impl Into<String>, profile: Option<String>) -> Result<Self> {
        let person = normalize_name("person", &person.into())?;
        let profile = match profile.filter(|p| !p.is_empty()) {
            Some(p) => Some(normalize_name("profile", &p)?),
            None => None,
        };
        Ok(Self { person, profile })
    }
    pub fn system() -> Self {
        Self::person(SYSTEM_PERSON)
    }
    /// The name `@mentions` and `--for` match against: the profile if set, else the person.
    pub fn handle(&self) -> &str {
        self.profile.as_deref().unwrap_or(&self.person)
    }
}

impl fmt::Display for Actor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.profile {
            Some(p) => write!(f, "{p} via {}", self.person),
            None => f.write_str(&self.person),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn display() {
        assert_eq!(
            Actor::new("chris", Some("fixer".into()))
                .unwrap()
                .to_string(),
            "fixer via chris"
        );
        assert_eq!(Actor::new("chris", None).unwrap().to_string(), "chris");
        assert_eq!(
            Actor::new("chris", Some("".into())).unwrap().to_string(),
            "chris"
        );
        assert!(Actor::new("bad name", None).is_err());
        assert_eq!(
            Actor::new("Chris", Some("Fixer".into()))
                .unwrap()
                .to_string(),
            "fixer via chris"
        );
        assert!(Actor::new("chrís", None).is_err());
    }
}
