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

/// Check one literal channel segment.
fn check_segment(seg: &str) -> Result<()> {
    if seg.is_empty()
        || !seg
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        || seg == "."
        || seg == ".."
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
    Ok(())
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
        check_segment(seg)?;
    }
    Ok(segs.join("/"))
}

/// One segment of a pattern.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Segment {
    /// A literal segment, such as `work`.
    Lit(String),
    /// `*`: exactly one segment.
    One,
    /// `**`: any number of segments, including none.
    Many,
}

/// One channel pattern (HANDOFF 19.1): a channel path in which a whole
/// segment may be `*` or `**`. `work/` is short for `work/**`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pattern {
    segs: Vec<Segment>,
}

/// What a pattern looks like to a store that wants to turn it into a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Shape<'a> {
    /// No wildcards: exactly this channel.
    Exact(String),
    /// `path/**`: this channel and everything under it.
    Subtree(String),
    /// `**`: every channel.
    All,
    /// Anything else. `prefix` is the literal segments before the first
    /// wildcard, joined by `/` (empty if it starts with one); every match is
    /// that channel or under it.
    Wild { prefix: String, pattern: &'a Pattern },
}

impl Pattern {
    /// Parse one pattern, without a leading `!`.
    pub fn parse(s: &str) -> Result<Self> {
        let raw = s.trim();
        let subtree = raw.ends_with('/');
        let trimmed = raw.trim_matches('/');
        if trimmed.is_empty() {
            return Err(BoardError::invalid("channel pattern is empty"));
        }
        if trimmed.len() > MAX_LEN {
            return Err(BoardError::invalid(format!(
                "channel pattern is over {MAX_LEN} bytes"
            )));
        }
        let mut segs: Vec<Segment> = Vec::new();
        for seg in trimmed.split('/') {
            let next = match seg {
                "*" => Segment::One,
                "**" => Segment::Many,
                s if s.contains('*') => {
                    return Err(BoardError::invalid(format!(
                        "pattern segment `{s}`: a wildcard must be a whole segment, `*` or `**`"
                    )));
                }
                s => {
                    check_segment(s)?;
                    Segment::Lit(s.to_string())
                }
            };
            // `**/**` is the same as `**`.
            if next == Segment::Many && segs.last() == Some(&Segment::Many) {
                continue;
            }
            segs.push(next);
        }
        if subtree && segs.last() != Some(&Segment::Many) {
            segs.push(Segment::Many);
        }
        let fixed = segs.iter().filter(|s| **s != Segment::Many).count();
        if fixed > MAX_DEPTH {
            return Err(BoardError::invalid(format!(
                "channel pattern is deeper than {MAX_DEPTH}"
            )));
        }
        Ok(Self { segs })
    }

    pub fn segments(&self) -> &[Segment] {
        &self.segs
    }

    /// True if the pattern has a `*` or `**` in it.
    pub fn is_wild(&self) -> bool {
        self.segs.iter().any(|s| !matches!(s, Segment::Lit(_)))
    }

    pub fn shape(&self) -> Shape<'_> {
        let lits: Vec<&str> = self
            .segs
            .iter()
            .map_while(|s| match s {
                Segment::Lit(l) => Some(l.as_str()),
                _ => None,
            })
            .collect();
        let prefix = lits.join("/");
        let rest = &self.segs[lits.len()..];
        match rest {
            [] => Shape::Exact(prefix),
            [Segment::Many] if lits.is_empty() => Shape::All,
            [Segment::Many] => Shape::Subtree(prefix),
            _ => Shape::Wild {
                prefix,
                pattern: self,
            },
        }
    }

    /// Does this pattern match the (already normalized) channel?
    pub fn matches(&self, channel: &str) -> bool {
        let parts: Vec<&str> = channel.split('/').collect();
        match_segs(&self.segs, &parts)
    }
}

/// Plain backtracking; patterns and channels are at most a few segments.
fn match_segs(pat: &[Segment], ch: &[&str]) -> bool {
    match pat.split_first() {
        None => ch.is_empty(),
        Some((Segment::Many, rest)) => (0..=ch.len()).any(|i| match_segs(rest, &ch[i..])),
        Some((Segment::One, rest)) => !ch.is_empty() && match_segs(rest, &ch[1..]),
        Some((Segment::Lit(l), rest)) => {
            ch.first().is_some_and(|c| c == l) && match_segs(rest, &ch[1..])
        }
    }
}

impl std::fmt::Display for Pattern {
    /// The canonical form: `work/**` is written `work/`, as it always was.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.segs.len();
        let subtree = n > 1 && self.segs[n - 1] == Segment::Many;
        let shown = if subtree { &self.segs[..n - 1] } else { &self.segs[..] };
        let parts: Vec<&str> = shown
            .iter()
            .map(|s| match s {
                Segment::Lit(l) => l.as_str(),
                Segment::One => "*",
                Segment::Many => "**",
            })
            .collect();
        write!(f, "{}", parts.join("/"))?;
        if subtree {
            write!(f, "/")?;
        }
        Ok(())
    }
}

/// The most patterns one selector may hold (HANDOFF 19.2).
pub const MAX_PATTERNS: usize = 16;

/// Which channels a read, wait or claim covers (HANDOFF 4, 19).
///
/// A set of patterns: a channel matches if it matches at least one ordinary
/// pattern and none of the `!` patterns. Order and repeats don't matter, so
/// two selectors with the same patterns are equal.
///
/// `work` means that channel only; `work/` means `work` and everything under
/// it. Written as text, patterns are separated by commas: `work/*/tests,docs/`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ChannelSelector {
    include: Vec<Pattern>,
    exclude: Vec<Pattern>,
}

impl ChannelSelector {
    /// Parse a comma-separated list of patterns.
    pub fn parse(s: &str) -> Result<Self> {
        Self::parse_all(s.split(','))
    }

    /// Parse several pattern lists (a repeated `channel` parameter, a pool's
    /// `select` list); each may itself hold commas.
    pub fn parse_all<I, S>(parts: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut include = Vec::new();
        let mut exclude = Vec::new();
        let mut count = 0;
        for part in parts {
            for item in part.as_ref().split(',') {
                count += 1;
                if count > MAX_PATTERNS {
                    return Err(BoardError::invalid(format!(
                        "a channel selector holds at most {MAX_PATTERNS} patterns"
                    )));
                }
                let item = item.trim();
                match item.strip_prefix('!') {
                    Some(rest) => exclude.push(Pattern::parse(rest)?),
                    None => include.push(Pattern::parse(item)?),
                }
            }
        }
        if include.is_empty() {
            return Err(BoardError::invalid(if exclude.is_empty() {
                "channel path is empty"
            } else {
                "a channel selector needs a pattern that is not an exclusion"
            }));
        }
        include.sort();
        include.dedup();
        exclude.sort();
        exclude.dedup();
        Ok(Self { include, exclude })
    }

    /// Exactly this channel.
    pub fn exact(channel: &str) -> Result<Self> {
        Self::parse(&normalize_channel(channel)?)
    }

    /// This channel and everything under it.
    pub fn subtree(channel: &str) -> Result<Self> {
        Self::parse(&format!("{}/", normalize_channel(channel)?))
    }

    pub fn matches(&self, channel: &str) -> bool {
        self.matched_by(channel).is_some()
    }

    /// The ordinary pattern that matched, if the channel matches. A channel
    /// that an exclusion takes away gives `None`.
    pub fn matched_by(&self, channel: &str) -> Option<&Pattern> {
        if self.exclude.iter().any(|p| p.matches(channel)) {
            return None;
        }
        self.include.iter().find(|p| p.matches(channel))
    }

    /// Why a channel does not match: the exclusion that took it away, if any.
    pub fn excluded_by(&self, channel: &str) -> Option<&Pattern> {
        self.exclude.iter().find(|p| p.matches(channel))
    }

    pub fn includes(&self) -> &[Pattern] {
        &self.include
    }

    pub fn excludes(&self) -> &[Pattern] {
        &self.exclude
    }

    /// True if any pattern has a wildcard or an exclusion is present: the
    /// selector is more than the `work` / `work/` of Milestone 1.
    pub fn is_plain(&self) -> bool {
        self.exclude.is_empty()
            && self.include.len() == 1
            && matches!(self.include[0].shape(), Shape::Exact(_) | Shape::Subtree(_))
    }

    /// Could this selector match `root` or a channel under it? Used to keep
    /// worker pools off `features/` (HANDOFF 19.6). It errs on the side of
    /// yes: a pattern that can reach under `root` counts unless an exclusion
    /// takes away all of `root/`.
    pub fn may_reach(&self, root: &str) -> bool {
        let root_segs: Vec<&str> = root.split('/').collect();
        let covers_all = |p: &Pattern| covers_subtree(&p.segs, &root_segs);
        if self.exclude.iter().any(covers_all) {
            return false;
        }
        self.include.iter().any(|p| reaches(&p.segs, &root_segs))
    }

    /// True if one ordinary pattern is exactly `root/` (the lead pool names
    /// `features/`).
    pub fn names_subtree(&self, root: &str) -> bool {
        self.include
            .iter()
            .any(|p| p.shape() == Shape::Subtree(root.to_string()))
    }

    /// The form a client sends: canonical patterns separated by commas, with a
    /// trailing `/` for a subtree. Parsing it gives back an equal selector.
    pub fn to_param(&self) -> String {
        self.to_string()
    }
}

/// Can `pat` match `root` or something under it?
fn reaches(pat: &[Segment], root: &[&str]) -> bool {
    match (pat.split_first(), root.split_first()) {
        // The root is used up: anything left can still match (the root itself
        // if what is left can be empty, or a channel under it otherwise).
        (_, None) => true,
        (None, Some(_)) => false,
        (Some((Segment::Many, _)), Some(_)) => true,
        (Some((Segment::One, rest)), Some((_, r))) => reaches(rest, r),
        (Some((Segment::Lit(l), rest)), Some((c, r))) => l == c && reaches(rest, r),
    }
}

/// Does `pat` match every channel that is `root` or under it?
fn covers_subtree(pat: &[Segment], root: &[&str]) -> bool {
    match (pat.split_first(), root.split_first()) {
        (Some((Segment::Many, rest)), _) if rest.is_empty() => true,
        (Some((Segment::One, rest)), Some((_, r))) => covers_subtree(rest, r),
        (Some((Segment::Lit(l), rest)), Some((c, r))) => l == c && covers_subtree(rest, r),
        _ => false,
    }
}

impl std::fmt::Display for ChannelSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for p in &self.include {
            if !first {
                write!(f, ",")?;
            }
            first = false;
            write!(f, "{p}")?;
        }
        for p in &self.exclude {
            write!(f, ",!{p}")?;
        }
        Ok(())
    }
}

impl TryFrom<String> for ChannelSelector {
    type Error = BoardError;
    fn try_from(s: String) -> Result<Self> {
        Self::parse(&s)
    }
}

impl From<ChannelSelector> for String {
    fn from(s: ChannelSelector) -> String {
        s.to_string()
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

    fn sel(s: &str) -> ChannelSelector {
        ChannelSelector::parse(s).unwrap_or_else(|e| panic!("{s}: {}", e.message))
    }

    /// Every row of HANDOFF 19.1, and a few more.
    #[test]
    fn pattern_table() {
        let rows: &[(&str, &[&str], &[&str])] = &[
            ("work", &["work"], &["work/fix", "workshop"]),
            ("work/*", &["work/fix", "work/docs"], &["work", "work/fix/tests"]),
            (
                "work/*/tests",
                &["work/api/tests", "work/cli/tests"],
                &["work/tests", "work/api/unit/tests"],
            ),
            (
                "work/**",
                &["work", "work/fix", "work/api/tests"],
                &["workshop"],
            ),
            (
                "**/tests",
                &["tests", "work/tests", "work/api/tests"],
                &["work/tests/unit"],
            ),
            ("**", &["a", "work/fix", "a/b/c/d/e/f/g/h"], &[]),
            ("*", &["work", "docs"], &["work/fix"]),
            ("work/**/tests", &["work/tests", "work/a/b/tests"], &["work/a/tests/x"]),
            ("*/**", &["a", "a/b"], &[]),
            ("**/**/x", &["x", "a/x"], &["a"]),
        ];
        for (pat, yes, no) in rows {
            let s = sel(pat);
            for c in *yes {
                assert!(s.matches(c), "{pat} should match {c}");
            }
            for c in *no {
                assert!(!s.matches(c), "{pat} should not match {c}");
            }
        }
    }

    #[test]
    fn subtree_shorthand_is_double_star() {
        assert_eq!(sel("work/"), sel("work/**"));
        assert_eq!(sel("work/").to_param(), "work/");
        assert_eq!(sel("work/**").to_param(), "work/");
        assert_eq!(sel("**").to_param(), "**");
        assert_eq!(sel("work/**/**").to_param(), "work/");
        assert_eq!(sel(" /work/ ").to_param(), "work/");
        assert_eq!(
            sel("work/").includes()[0].shape(),
            Shape::Subtree("work".into())
        );
        assert_eq!(sel("work").includes()[0].shape(), Shape::Exact("work".into()));
        assert_eq!(sel("**").includes()[0].shape(), Shape::All);
        assert!(matches!(
            sel("work/*/tests").includes()[0].shape(),
            Shape::Wild { prefix, .. } if prefix == "work"
        ));
        assert!(matches!(
            sel("**/tests").includes()[0].shape(),
            Shape::Wild { prefix, .. } if prefix.is_empty()
        ));
    }

    #[test]
    fn lists_and_exclusions() {
        let s = sel("work/*/tests, docs/");
        assert!(s.matches("work/api/tests"));
        assert!(s.matches("docs/howto"));
        assert!(!s.matches("work/tests"));
        // Order and repeats don't matter.
        assert_eq!(s, sel("docs/,work/*/tests,docs/**"));
        assert_eq!(
            ChannelSelector::parse_all(["docs/", "work/*/tests"]).unwrap(),
            s
        );
        // The text form round-trips.
        assert_eq!(sel(&s.to_param()), s);

        let x = sel("**,!features/");
        assert!(x.matches("work"));
        assert!(!x.matches("features"));
        assert!(!x.matches("features/a/b"));
        assert!(x.matches("featuresx"));
        assert_eq!(x.excluded_by("features/a").unwrap().to_string(), "features/");
        assert_eq!(x.to_param(), "**,!features/");
        assert_eq!(sel("!features/,**"), x);

        let y = sel("work/,!work/*/secret");
        assert!(y.matches("work/a/public"));
        assert!(!y.matches("work/a/secret"));
        assert_eq!(y.matched_by("work/x").unwrap().to_string(), "work/");
    }

    #[test]
    fn refusals() {
        for bad in [
            "",
            ",",
            "work,",
            "work-*",
            "*-tests",
            "w*rk",
            "***",
            "!features/",
            "!a,!b",
            "a b",
            "a//b",
            "a/../b",
            "work/01JB2X3Y4Z5A6B7C8D9E0F1G2H",
            "a/b/c/d/e/f/g/h/i",
            "!",
        ] {
            assert!(ChannelSelector::parse(bad).is_err(), "`{bad}` should be refused");
        }
        // 8 fixed segments plus a trailing `**` is fine.
        assert!(ChannelSelector::parse("a/b/c/d/e/f/g/h/").is_ok());
        let sixteen = vec!["a"; 16].join(",");
        assert!(ChannelSelector::parse(&sixteen).is_ok());
        let seventeen = vec!["a"; 17].join(",");
        assert!(ChannelSelector::parse(&seventeen).is_err());
    }

    #[test]
    fn reaching_a_subtree() {
        for (s, reaches) in [
            ("features/", true),
            ("features", true),
            ("features/x", true),
            ("**", true),
            ("*", true),
            ("*/tests", true),
            ("**/tests", true),
            ("work/", false),
            ("work/*/tests", false),
            ("featuresx/", false),
            ("**,!features/", false),
            ("**,!*/", false),
            ("**,!features/x/", true),
            ("*/tests,!features/", false),
        ] {
            assert_eq!(sel(s).may_reach("features"), reaches, "{s}");
        }
        assert!(sel("features/,work/").names_subtree("features"));
        assert!(!sel("features/x/").names_subtree("features"));
        assert!(!sel("**").names_subtree("features"));
    }

    #[test]
    fn serde_is_the_text_form() {
        let s = sel("work/*/tests,!work/x/tests");
        let j = serde_json::to_string(&s).unwrap();
        assert_eq!(j, "\"work/*/tests,!work/x/tests\"");
        let back: ChannelSelector = serde_json::from_str(&j).unwrap();
        assert_eq!(back, s);
        assert!(serde_json::from_str::<ChannelSelector>("\"work-*\"").is_err());
    }
}
