//! The model catalog, `models.toml` (HANDOFF 16.2): the keyword vocabulary,
//! its aliases, and what each model can do.
//!
//! The board never sees any of this. Keywords reach tasks as plain labels,
//! `needs.<keyword>=yes`, and only the fleet and the CLI read them.

use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// The label prefix a task uses to ask for a keyword.
pub const NEEDS_PREFIX: &str = "needs.";
/// The value `--needs` writes.
pub const NEEDS_VALUE: &str = "yes";
/// Tier keywords are made from each model's tier, never declared.
pub const TIER_PREFIX: &str = "tier-";
/// A keyword is a label key after `needs.`, so it must fit in 64 bytes with it.
pub const MAX_KEYWORD_LEN: usize = 48;

#[derive(Debug, Clone, PartialEq)]
pub struct Model {
    pub name: String,
    /// Opaque: the fleet runs it after `cork tasks run ... --`.
    pub command: Vec<String>,
    /// 1 is the weakest. A model also has every tier below its own.
    pub tier: u32,
    /// Relative cost per task-hour. Models without one sort after those with one.
    pub cost: Option<f64>,
    /// Declared keywords, aliases already resolved. Tiers not included.
    pub keywords: BTreeSet<String>,
    /// Dollars per million tokens, for estimating cost when the agent does
    /// not report it (18.2).
    pub price: Option<Price>,
}

/// `[model.<name>.price]`: dollars per million tokens. The real bill is the
/// provider's; this is an estimate.
#[derive(Debug, Clone, Copy, PartialEq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Price {
    #[serde(default)]
    pub input_per_mtok: f64,
    #[serde(default)]
    pub output_per_mtok: f64,
    #[serde(default)]
    pub cache_read_per_mtok: f64,
    #[serde(default)]
    pub cache_write_per_mtok: f64,
}

impl Price {
    /// The estimated cost of these token counts.
    pub fn cost(&self, input: u64, output: u64, cache_read: u64, cache_write: u64) -> f64 {
        (input as f64 * self.input_per_mtok
            + output as f64 * self.output_per_mtok
            + cache_read as f64 * self.cache_read_per_mtok
            + cache_write as f64 * self.cache_write_per_mtok)
            / 1_000_000.0
    }
}

impl Model {
    /// Everything this model covers: its keywords and `tier-1..=tier`.
    pub fn covers(&self) -> BTreeSet<String> {
        let mut out = self.keywords.clone();
        out.extend(tier_keywords(self.tier));
        out
    }
}

pub fn tier_keyword(t: u32) -> String {
    format!("{TIER_PREFIX}{t}")
}

pub fn tier_keywords(tier: u32) -> impl Iterator<Item = String> {
    (1..=tier).map(tier_keyword)
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Catalog {
    /// Keyword to its description.
    pub keywords: BTreeMap<String, String>,
    /// Other spelling to keyword.
    pub aliases: BTreeMap<String, String>,
    pub models: BTreeMap<String, Model>,
}

/// A word that is not in the vocabulary, with the closest one that is.
#[derive(Debug, Clone, PartialEq)]
pub struct UnknownKeyword {
    pub word: String,
    pub suggestion: Option<String>,
}

impl std::fmt::Display for UnknownKeyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown keyword `{}`", self.word)?;
        if let Some(s) = &self.suggestion {
            write!(f, "; did you mean `{s}`?")?;
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCatalog {
    #[serde(default)]
    keywords: BTreeMap<String, String>,
    #[serde(default)]
    aliases: BTreeMap<String, String>,
    #[serde(default)]
    model: BTreeMap<String, RawModel>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawModel {
    command: Vec<String>,
    #[serde(default = "one")]
    tier: u32,
    cost: Option<f64>,
    #[serde(default)]
    keywords: Vec<String>,
    price: Option<Price>,
}

fn one() -> u32 {
    1
}

/// Names of fleets, pools and models end up in profile names, so they keep
/// to lowercase letters, digits, `-` and `_`.
pub fn check_name(kind: &str, s: &str) -> Result<(), String> {
    if s.is_empty()
        || s.len() > 32
        || !s
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
    {
        return Err(format!(
            "{kind} name `{s}` must be 1-32 chars of [a-z0-9_-]"
        ));
    }
    Ok(())
}

fn check_keyword_name(k: &str) -> Result<(), String> {
    if k.is_empty()
        || k.len() > MAX_KEYWORD_LEN
        || !k
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
    {
        return Err(format!(
            "keyword `{k}` must be 1-{MAX_KEYWORD_LEN} chars of [a-z0-9_-]"
        ));
    }
    if k.starts_with(TIER_PREFIX) {
        return Err(format!(
            "keyword `{k}`: `{TIER_PREFIX}N` keywords come from each model's tier and cannot be declared"
        ));
    }
    Ok(())
}

impl Catalog {
    /// Read a catalog: JSON if the file ends in `.json`, TOML otherwise.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("could not read {}: {e}", path.display()))?;
        let r = if is_json(path) {
            Self::parse_json(&text)
        } else {
            Self::parse_toml(&text)
        };
        r.map_err(|e| format!("{}: {e}", path.display()))
    }

    pub fn parse_toml(s: &str) -> Result<Self, String> {
        let raw: RawCatalog = toml::from_str(s).map_err(|e| e.to_string())?;
        Self::from_raw(raw)
    }

    pub fn parse_json(s: &str) -> Result<Self, String> {
        let raw: RawCatalog = serde_json::from_str(s).map_err(|e| e.to_string())?;
        Self::from_raw(raw)
    }

    fn from_raw(raw: RawCatalog) -> Result<Self, String> {
        let mut c = Catalog {
            keywords: BTreeMap::new(),
            aliases: BTreeMap::new(),
            models: BTreeMap::new(),
        };
        for (k, d) in raw.keywords {
            check_keyword_name(&k)?;
            c.keywords.insert(k, d);
        }
        for (a, k) in raw.aliases {
            let a = a.to_ascii_lowercase();
            if c.keywords.contains_key(&a) {
                return Err(format!("alias `{a}` is also a keyword"));
            }
            if !c.keywords.contains_key(&k) {
                return Err(format!(
                    "alias `{a}` points to `{k}`, which is not in [keywords]"
                ));
            }
            c.aliases.insert(a, k);
        }
        let mut models = BTreeMap::new();
        for (name, m) in raw.model {
            check_name("model", &name)?;
            if m.command.is_empty() || m.command[0].is_empty() {
                return Err(format!("model `{name}` needs a command"));
            }
            if m.tier == 0 {
                return Err(format!("model `{name}`: tier starts at 1"));
            }
            if let Some(cost) = m.cost
                && !(cost.is_finite() && cost >= 0.0)
            {
                return Err(format!("model `{name}`: cost must be a number >= 0"));
            }
            if let Some(p) = &m.price
                && [
                    p.input_per_mtok,
                    p.output_per_mtok,
                    p.cache_read_per_mtok,
                    p.cache_write_per_mtok,
                ]
                .iter()
                .any(|v| !(v.is_finite() && *v >= 0.0))
            {
                return Err(format!("model `{name}`: prices must be numbers >= 0"));
            }
            let mut keywords = BTreeSet::new();
            for k in &m.keywords {
                // A model names keywords, not tiers: its tier says that.
                match c.resolve_word(k) {
                    Ok(k) if !k.starts_with(TIER_PREFIX) => {
                        keywords.insert(k);
                    }
                    Ok(k) => {
                        return Err(format!("model `{name}` lists `{k}`; set `tier` instead"));
                    }
                    Err(e) => return Err(format!("model `{name}`: {e}")),
                }
            }
            models.insert(
                name.clone(),
                Model {
                    name,
                    command: m.command,
                    tier: m.tier,
                    cost: m.cost,
                    keywords,
                    price: m.price,
                },
            );
        }
        c.models = models;
        Ok(c)
    }

    /// The highest tier of any model. `tier-N` above it is unknown.
    pub fn max_tier(&self) -> u32 {
        self.models.values().map(|m| m.tier).max().unwrap_or(0)
    }

    /// Resolve one word to its keyword: a keyword, an alias, or a tier.
    pub fn resolve(&self, word: &str) -> Result<String, UnknownKeyword> {
        self.resolve_word(word)
    }

    fn resolve_word(&self, word: &str) -> Result<String, UnknownKeyword> {
        let w = word.trim().to_ascii_lowercase();
        if self.keywords.contains_key(&w) {
            return Ok(w);
        }
        if let Some(k) = self.aliases.get(&w) {
            return Ok(k.clone());
        }
        if let Some(n) = w.strip_prefix(TIER_PREFIX)
            && let Ok(n) = n.parse::<u32>()
            && n >= 1
            && n <= self.max_tier().max(1)
            && w == tier_keyword(n)
        {
            return Ok(w);
        }
        Err(UnknownKeyword {
            word: word.trim().to_string(),
            suggestion: self.closest(&w),
        })
    }

    /// Every word `resolve` accepts.
    pub fn vocabulary(&self) -> Vec<String> {
        let mut v: Vec<String> = self.keywords.keys().cloned().collect();
        v.extend(self.aliases.keys().cloned());
        v.extend(tier_keywords(self.max_tier().max(1)));
        v
    }

    fn closest(&self, w: &str) -> Option<String> {
        let mut best: Option<(usize, String)> = None;
        for cand in self.vocabulary() {
            let d = edit_distance(w, &cand);
            if best.as_ref().is_none_or(|(bd, _)| d < *bd) {
                best = Some((d, cand));
            }
        }
        let (d, cand) = best?;
        let limit = (w.chars().count() / 3).max(2);
        if d > limit {
            return None;
        }
        // Suggest the keyword an alias stands for.
        Some(self.aliases.get(&cand).cloned().unwrap_or(cand))
    }

    /// Resolve a comma-separated `--needs` list. All unknown words are
    /// reported together.
    pub fn resolve_list(&self, list: &[String]) -> Result<BTreeSet<String>, Vec<UnknownKeyword>> {
        let mut ok = BTreeSet::new();
        let mut bad = vec![];
        for item in list {
            for w in item.split(',').map(str::trim).filter(|w| !w.is_empty()) {
                match self.resolve(w) {
                    Ok(k) => {
                        ok.insert(k);
                    }
                    Err(e) => bad.push(e),
                }
            }
        }
        if bad.is_empty() { Ok(ok) } else { Err(bad) }
    }

    /// The keywords a task asks for, read from its labels. Words that are
    /// not in the vocabulary (a raw label, or one written by the API) are
    /// kept apart so the fleet can report them as unmatched.
    pub fn needs_of<'a, I>(&self, labels: I) -> Needs
    where
        I: IntoIterator<Item = (&'a String, &'a String)>,
    {
        let mut n = Needs::default();
        for (k, _) in labels {
            if let Some(w) = k.strip_prefix(NEEDS_PREFIX) {
                match self.resolve(w) {
                    Ok(k) => {
                        n.keywords.insert(k);
                    }
                    Err(_) => {
                        n.unknown.insert(w.to_string());
                    }
                }
            }
        }
        n
    }

    /// Which models cover each keyword, for `cork workers keywords`.
    pub fn coverage(&self) -> BTreeMap<String, Vec<String>> {
        let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for k in self.keywords.keys() {
            out.insert(k.clone(), vec![]);
        }
        for t in tier_keywords(self.max_tier()) {
            out.insert(t, vec![]);
        }
        for m in self.models.values() {
            for k in m.covers() {
                out.entry(k).or_default().push(m.name.clone());
            }
        }
        out
    }
}

/// What a task asks for.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Needs {
    pub keywords: BTreeSet<String>,
    /// `needs.*` labels whose word is not in the vocabulary.
    pub unknown: BTreeSet<String>,
}

/// The labels `--needs` writes for a set of keywords.
pub fn needs_labels(keywords: &BTreeSet<String>) -> Vec<(String, String)> {
    keywords
        .iter()
        .map(|k| (format!("{NEEDS_PREFIX}{k}"), NEEDS_VALUE.to_string()))
        .collect()
}

pub fn is_json(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("json"))
}

/// Levenshtein distance, for "did you mean".
pub fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut cur = vec![i + 1; b.len() + 1];
        for (j, cb) in b.iter().enumerate() {
            let sub = prev[j] + usize::from(ca != cb);
            cur[j + 1] = sub.min(prev[j + 1] + 1).min(cur[j] + 1);
        }
        prev = cur;
    }
    prev[b.len()]
}

#[cfg(test)]
pub(crate) const EXAMPLE: &str = r#"
[keywords]
rust          = "Writes and reviews Rust"
typescript    = "Writes and reviews TypeScript"
docs          = "Prose"
large-context = "Large codebase"
vision        = "Screenshots"
architecture  = "Design across modules"
review        = "Reviews someone else's change"

[aliases]
ts = "typescript"
documentation = "docs"

[model.opus]
command = ["claude", "-p", "x", "--model", "opus"]
tier = 3
cost = 15
keywords = ["rust", "typescript", "docs", "large-context", "vision", "architecture"]

[model.sonnet]
command = ["claude", "-p", "x", "--model", "sonnet"]
tier = 2
cost = 3
keywords = ["rust", "ts", "docs", "large-context"]

[model.sonnet.price]
input_per_mtok = 3.0
output_per_mtok = 15.0
cache_read_per_mtok = 0.3

[model.haiku]
command = ["claude", "-p", "x", "--model", "haiku"]
tier = 1
cost = 1
keywords = ["typescript", "documentation"]

[model.reviewer]
command = ["./scripts/review-agent.sh"]
tier = 2
keywords = ["review"]
"#;

#[cfg(test)]
mod tests {
    use super::*;

    fn cat() -> Catalog {
        Catalog::parse_toml(EXAMPLE).unwrap()
    }

    #[test]
    fn parses_the_spec_example() {
        let c = cat();
        assert_eq!(c.models.len(), 4);
        assert_eq!(c.max_tier(), 3);
        let s = &c.models["sonnet"];
        assert!(s.keywords.contains("typescript"), "alias resolved");
        assert!(s.covers().contains("tier-1") && s.covers().contains("tier-2"));
        assert!(!s.covers().contains("tier-3"));
        assert!(c.models["haiku"].keywords.contains("docs"));
        assert_eq!(c.models["reviewer"].cost, None);
        let p = c.models["sonnet"].price.unwrap();
        assert_eq!(p.input_per_mtok, 3.0);
        assert_eq!(p.cache_write_per_mtok, 0.0);
        assert!((p.cost(1_000_000, 100_000, 0, 0) - 4.5).abs() < 1e-9);
        assert_eq!(c.models["haiku"].price, None);
    }

    #[test]
    fn json_in_the_same_shape() {
        let j = r#"{"keywords":{"rust":"Rust"},"model":{"a":{"command":["x"],"tier":2,"keywords":["rust"]}}}"#;
        let c = Catalog::parse_json(j).unwrap();
        assert_eq!(c.models["a"].tier, 2);
    }

    #[test]
    fn resolve_aliases_tiers_and_typos() {
        let c = cat();
        assert_eq!(c.resolve("ts").unwrap(), "typescript");
        assert_eq!(c.resolve("TypeScript").unwrap(), "typescript");
        assert_eq!(c.resolve("tier-3").unwrap(), "tier-3");
        let e = c.resolve("typscript").unwrap_err();
        assert_eq!(e.suggestion.as_deref(), Some("typescript"));
        assert_eq!(
            e.to_string(),
            "unknown keyword `typscript`; did you mean `typescript`?"
        );
        assert!(c.resolve("tier-4").is_err());
        assert!(c.resolve("tier-0").is_err());
        assert!(c.resolve("tier-02").is_err());
        assert_eq!(c.resolve("zzzzzzzzz").unwrap_err().suggestion, None);
        // An alias near-miss suggests the keyword it stands for.
        assert_eq!(
            c.resolve("documentaton").unwrap_err().suggestion.as_deref(),
            Some("docs")
        );
    }

    #[test]
    fn resolve_list_reports_every_unknown() {
        let c = cat();
        let ok = c.resolve_list(&["rust, ts".into(), "docs".into()]).unwrap();
        assert_eq!(ok.len(), 3);
        let bad = c.resolve_list(&["rustt,ts,vison".into()]).unwrap_err();
        assert_eq!(bad.len(), 2);
    }

    #[test]
    fn rejects_bad_catalogs() {
        let bad = [
            (
                "[model.a]\ncommand=[\"x\"]\nkeywords=[\"nope\"]",
                "unknown keyword",
            ),
            ("[keywords]\ntier-1=\"x\"", "cannot be declared"),
            ("[aliases]\nx=\"y\"", "not in [keywords]"),
            ("[model.a]\ncommand=[]", "needs a command"),
            ("[model.a]\ncommand=[\"x\"]\ntier=0", "tier starts at 1"),
            ("[model.A]\ncommand=[\"x\"]", "must be 1-32"),
            ("[model.a]\ncommand=[\"x\"]\nkeyword=[]", "unknown field"),
            (
                "[model.a]\ncommand=[\"x\"]\ntier=2\nkeywords=[\"tier-1\"]",
                "set `tier` instead",
            ),
        ];
        for (src, want) in bad {
            let e = Catalog::parse_toml(src).unwrap_err();
            assert!(e.contains(want), "{src}: {e}");
        }
    }

    #[test]
    fn needs_from_labels() {
        let c = cat();
        let mut l = BTreeMap::new();
        l.insert("needs.rust".to_string(), "yes".to_string());
        l.insert("needs.ts".to_string(), "yes".to_string());
        l.insert("needs.typscript".to_string(), "yes".to_string());
        l.insert("parent".to_string(), "F".to_string());
        let n = c.needs_of(&l);
        assert_eq!(
            n.keywords.into_iter().collect::<Vec<_>>(),
            vec!["rust", "typescript"]
        );
        assert_eq!(n.unknown.into_iter().collect::<Vec<_>>(), vec!["typscript"]);
    }

    #[test]
    fn coverage_lists_models_per_keyword() {
        let cov = cat().coverage();
        assert_eq!(cov["rust"], vec!["opus", "sonnet"]);
        assert_eq!(cov["tier-3"], vec!["opus"]);
        assert_eq!(cov["review"], vec!["reviewer"]);
    }

    #[test]
    fn distance() {
        assert_eq!(edit_distance("kitten", "sitting"), 3);
        assert_eq!(edit_distance("", "ab"), 2);
    }
}
