//! Matching tasks to pools (HANDOFF 16.6). One function decides, and both
//! the dispatcher and `cork workers match` call it, so the dry run always
//! gives the answer the fleet acts on.

use crate::catalog::{Catalog, Needs, tier_keyword};
use crate::config::Pool;
use cork_core::Task;
use std::cmp::Ordering;

/// What routing needs to know about a task.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskFacts {
    pub channel: String,
    pub owner: String,
    pub assignee: Option<String>,
    pub needs: Needs,
}

impl TaskFacts {
    pub fn of(task: &Task, catalog: &Catalog) -> Self {
        Self {
            channel: task.channel.clone(),
            owner: task.owner.clone(),
            assignee: task.assignee.clone(),
            needs: catalog.needs_of(&task.labels),
        }
    }

    /// A task that does not exist yet, for `match --needs`. Its channel is
    /// empty unless `match --channel` names one; its owner is unknown, so
    /// `from` is not checked.
    pub fn planned(needs: Needs) -> Self {
        Self {
            channel: String::new(),
            owner: String::new(),
            assignee: None,
            needs,
        }
    }
}

/// Why a pool can or cannot take a task.
#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    Covers,
    /// Keywords the task needs that the pool lacks.
    Missing(Vec<String>),
    /// `needs.*` labels outside the vocabulary: no pool covers them.
    Unknown(Vec<String>),
    /// An `exclusive` pool takes only tasks that ask for its top tier.
    Exclusive(String),
    /// The pool does not select the task's channel.
    Channel(String),
    /// The task belongs to someone the pool does not take work from.
    Owner(String),
    /// `--for` tasks go to that agent, not to a fleet.
    Assigned(String),
}

impl Verdict {
    pub fn covers(&self) -> bool {
        matches!(self, Verdict::Covers)
    }
}

impl std::fmt::Display for Verdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Verdict::Covers => write!(f, "covers"),
            Verdict::Missing(m) => write!(f, "missing {}", m.join(", ")),
            Verdict::Unknown(u) => write!(f, "unknown keyword {}", u.join(", ")),
            Verdict::Exclusive(t) => write!(f, "exclusive: takes only tasks that need {t}"),
            Verdict::Channel(c) => write!(f, "does not select {c}"),
            Verdict::Owner(o) => write!(f, "does not take {o}'s tasks"),
            Verdict::Assigned(a) => write!(f, "the task is for {a}"),
        }
    }
}

/// Whether `pool` may take the task. `person` is who the fleet runs as;
/// a pool without `from` takes only that person's tasks, as `tasks run` does.
/// Pass `check_place = false` to judge keywords only (`match --needs`).
pub fn verdict(pool: &Pool, t: &TaskFacts, person: &str, check_place: bool) -> Verdict {
    if check_place {
        if !pool.select.matches(&t.channel) {
            return Verdict::Channel(t.channel.clone());
        }
        let from_ok = match pool.from.as_deref() {
            _ if t.owner.is_empty() => true,
            Some("*") => true,
            Some(p) => p.eq_ignore_ascii_case(&t.owner),
            None => person.eq_ignore_ascii_case(&t.owner),
        };
        if !from_ok {
            return Verdict::Owner(t.owner.clone());
        }
        if let Some(a) = &t.assignee {
            return Verdict::Assigned(a.clone());
        }
    }
    if !t.needs.unknown.is_empty() {
        return Verdict::Unknown(t.needs.unknown.iter().cloned().collect());
    }
    let missing: Vec<String> = t
        .needs
        .keywords
        .iter()
        .filter(|k| !pool.covers.contains(*k))
        .cloned()
        .collect();
    if !missing.is_empty() {
        return Verdict::Missing(missing);
    }
    if pool.exclusive {
        let top = tier_keyword(pool.model.tier);
        if !t.needs.keywords.contains(&top) {
            return Verdict::Exclusive(top);
        }
    }
    Verdict::Covers
}

/// How a pool's `select` judges a channel, for `workers match` (19.8):
/// the pattern that matched, the exclusion that took it away, or what the
/// pool selects instead.
pub fn channel_note(pool: &Pool, channel: &str) -> String {
    if let Some(p) = pool.select.matched_by(channel) {
        return format!("select {p} matched");
    }
    if let Some(x) = pool.select.excluded_by(channel) {
        return format!("excluded by !{x}");
    }
    let only: Vec<String> = pool
        .select
        .includes()
        .iter()
        .map(|p| p.to_string())
        .collect();
    format!("{} only", only.join(", "))
}

/// Two pools that both select a channel on the board now (HANDOFF 19.7).
#[derive(Debug, Clone, PartialEq)]
pub struct Overlap {
    pub a: String,
    pub b: String,
    /// The first shared channel, in path order.
    pub channel: String,
    /// How many other shared channels there are.
    pub more: usize,
    /// Which of the two takes a task with no needs there, if either does.
    pub winner: Option<String>,
}

impl std::fmt::Display for Overlap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} and {} overlap on {}", self.a, self.b, self.channel)?;
        if self.more > 0 {
            write!(f, " (and {} more)", self.more)?;
        }
        match &self.winner {
            Some(w) => write!(f, "; {w} wins (cheaper)"),
            None => write!(f, "; neither takes a task with no needs there"),
        }
    }
}

/// Every pair of pools whose selectors share a channel from `channels`.
/// Overlap is allowed; this only reports it, so it is easy to notice.
pub fn overlaps<'a, I>(pools: I, channels: &[String], person: &str) -> Vec<Overlap>
where
    I: IntoIterator<Item = &'a Pool>,
{
    let pools: Vec<&Pool> = pools.into_iter().collect();
    let mut sorted: Vec<&String> = channels.iter().collect();
    sorted.sort();
    sorted.dedup();
    let mut out = vec![];
    for (i, a) in pools.iter().enumerate() {
        for b in &pools[i + 1..] {
            let shared: Vec<&&String> = sorted
                .iter()
                .filter(|c| a.select.matches(c) && b.select.matches(c))
                .collect();
            let Some(first) = shared.first() else {
                continue;
            };
            let t = TaskFacts {
                channel: first.to_string(),
                owner: person.to_string(),
                assignee: None,
                needs: Needs::default(),
            };
            let winner = rank([*a, *b], &t, person, true)
                .first()
                .map(|p| p.name.clone());
            out.push(Overlap {
                a: a.name.clone(),
                b: b.name.clone(),
                channel: first.to_string(),
                more: shared.len() - 1,
                winner,
            });
        }
    }
    out
}

/// Cheapest first: by cost (a model with no cost after any with one), then
/// tier, then pool name. This is the order the dispatcher tries pools in.
pub fn cheaper(a: &Pool, b: &Pool) -> Ordering {
    let cost = |p: &Pool| p.model.cost.unwrap_or(f64::INFINITY);
    cost(a)
        .total_cmp(&cost(b))
        .then(a.model.tier.cmp(&b.model.tier))
        .then(a.name.cmp(&b.name))
}

/// The pools that may take the task, cheapest first.
pub fn rank<'a, I>(pools: I, t: &TaskFacts, person: &str, check_place: bool) -> Vec<&'a Pool>
where
    I: IntoIterator<Item = &'a Pool>,
{
    let mut v: Vec<&Pool> = pools
        .into_iter()
        .filter(|p| verdict(p, t, person, check_place).covers())
        .collect();
    v.sort_by(|a, b| cheaper(a, b));
    v
}

/// The pool the dispatcher gives the task to: the cheapest that covers it
/// and has a free worker. `free` says which pools have one.
pub fn choose<'a, I>(
    pools: I,
    t: &TaskFacts,
    person: &str,
    free: impl Fn(&Pool) -> bool,
) -> Option<&'a Pool>
where
    I: IntoIterator<Item = &'a Pool>,
{
    rank(pools, t, person, true).into_iter().find(|p| free(p))
}

/// Whether a model, on its own, covers what a task needs (for `match`).
pub fn model_verdict(model: &crate::Model, needs: &Needs) -> Verdict {
    if !needs.unknown.is_empty() {
        return Verdict::Unknown(needs.unknown.iter().cloned().collect());
    }
    let covers = model.covers();
    let missing: Vec<String> = needs
        .keywords
        .iter()
        .filter(|k| !covers.contains(*k))
        .cloned()
        .collect();
    if missing.is_empty() {
        Verdict::Covers
    } else {
        Verdict::Missing(missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::EXAMPLE;
    use crate::config::{EXAMPLE_WORKERS, Fleet};
    use std::collections::BTreeSet;

    fn fleet() -> Fleet {
        Fleet::parse(EXAMPLE_WORKERS, EXAMPLE).unwrap()
    }

    fn facts(f: &Fleet, needs: &[&str]) -> TaskFacts {
        let list: Vec<String> = needs.iter().map(|s| s.to_string()).collect();
        TaskFacts {
            channel: "work/app".into(),
            owner: "chris".into(),
            assignee: None,
            needs: Needs {
                keywords: f.catalog.resolve_list(&list).unwrap(),
                unknown: BTreeSet::new(),
            },
        }
    }

    fn names(v: Vec<&Pool>) -> Vec<String> {
        v.into_iter().map(|p| p.name.clone()).collect()
    }

    #[test]
    fn cheapest_covering_pool_first() {
        let f = fleet();
        let t = facts(&f, &["docs"]);
        assert_eq!(
            names(rank(f.pools.values(), &t, "chris", true)),
            vec!["fast", "strong"]
        );
        let t = facts(&f, &["rust"]);
        assert_eq!(
            names(rank(f.pools.values(), &t, "chris", true)),
            vec!["strong"]
        );
        // No needs: anyone, the costless reviewer last.
        let t = facts(&f, &[]);
        assert_eq!(
            names(rank(f.pools.values(), &t, "chris", true)),
            vec!["fast", "strong", "review"]
        );
    }

    #[test]
    fn verdicts_explain_why_not() {
        let f = fleet();
        let fast = &f.pools["fast"];
        assert_eq!(
            verdict(fast, &facts(&f, &["rust", "docs"]), "chris", true),
            Verdict::Missing(vec!["rust".into()])
        );
        assert_eq!(
            verdict(fast, &facts(&f, &["tier-2"]), "chris", true).to_string(),
            "missing tier-2"
        );
        let mut t = facts(&f, &[]);
        t.channel = "features".into();
        assert_eq!(
            verdict(fast, &t, "chris", true),
            Verdict::Channel("features".into())
        );
        assert!(verdict(fast, &t, "chris", false).covers());
        let t = facts(&f, &[]);
        assert_eq!(
            verdict(fast, &t, "sam", true),
            Verdict::Owner("chris".into())
        );
        let mut t = facts(&f, &[]);
        t.assignee = Some("bob".into());
        assert_eq!(
            verdict(fast, &t, "chris", true),
            Verdict::Assigned("bob".into())
        );
        let mut t = facts(&f, &["docs"]);
        t.needs.unknown.insert("typscript".into());
        assert_eq!(
            verdict(fast, &t, "chris", true),
            Verdict::Unknown(vec!["typscript".into()])
        );
    }

    #[test]
    fn exclusive_pools_take_only_top_tier_work() {
        let mut f = fleet();
        f.pools.get_mut("strong").unwrap().exclusive = true;
        let strong = &f.pools["strong"];
        assert!(matches!(
            verdict(strong, &facts(&f, &["rust"]), "chris", true),
            Verdict::Exclusive(_)
        ));
        assert!(verdict(strong, &facts(&f, &["rust", "tier-3"]), "chris", true).covers());
    }

    #[test]
    fn notes_and_overlaps() {
        let w = r#"
[fleet]
name = "dev"
models = "m"
[pool.tests]
model = "haiku"
select = "work/*/tests"
[pool.general]
model = "sonnet"
select = ["**", "!features/"]
[pool.lead]
model = "opus"
select = "features/"
"#;
        let f = Fleet::parse(w, EXAMPLE).unwrap();
        let p = |n: &str| &f.pools[n];
        assert_eq!(
            channel_note(p("tests"), "work/api/tests"),
            "select work/*/tests matched"
        );
        assert_eq!(
            channel_note(p("general"), "work/api/tests"),
            "select ** matched"
        );
        assert_eq!(channel_note(p("lead"), "work/api/tests"), "features/ only");
        assert_eq!(
            channel_note(p("general"), "features/x"),
            "excluded by !features/"
        );
        let chans: Vec<String> = ["work/api/tests", "work/cli/tests", "features/x", "docs"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let o = overlaps(f.pools.values(), &chans, "chris");
        assert_eq!(o.len(), 1, "{o:?}");
        assert_eq!(
            o[0].to_string(),
            "general and tests overlap on work/api/tests (and 1 more); tests wins (cheaper)"
        );
        // A planned task has no owner, so `from` is not checked.
        let mut t = TaskFacts::planned(Needs::default());
        t.channel = "work/api/tests".into();
        assert!(verdict(p("tests"), &t, "", true).covers());
    }

    #[test]
    fn choose_skips_busy_pools() {
        let f = fleet();
        let t = facts(&f, &["docs"]);
        let p = choose(f.pools.values(), &t, "chris", |p| p.name != "fast").unwrap();
        assert_eq!(
            p.name, "strong",
            "a stronger pool takes easier work when the cheap one is busy"
        );
        assert!(choose(f.pools.values(), &t, "chris", |_| false).is_none());
    }
}
