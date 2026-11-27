//! Escalation: retry a struggling task one tier up (HANDOFF 16.7).
//!
//! Labels are fixed when a task is created, so an escalation never changes
//! the old task. It plans a **retry**: a new task with the same work, the
//! old needs plus the next tier, `retry_of=<old>`, and a text that points
//! the stronger model at the earlier branch. The CLI then creates it under
//! an idempotency key derived from the old id, so a lead that restarts
//! mid-escalation never makes two.

use crate::catalog::{Catalog, NEEDS_PREFIX, TIER_PREFIX, needs_labels, tier_keyword};
use crate::config::Fleet;
use cork_core::{Labels, Task};
use std::collections::BTreeSet;

pub const RETRY_OF: &str = "retry_of";
pub const ESCALATION: &str = "escalation";
/// The skill's rule: at most two escalations per task (17.2).
pub const DEFAULT_MAX_ESCALATIONS: u32 = 2;

/// What the old attempts left: progress notes and the last failure.
#[derive(Debug, Clone, Default)]
pub struct Trail {
    /// Attempts made, from `TaskClaimed` events.
    pub attempts: u32,
    /// Profiles that claimed the task, oldest first.
    pub claimers: Vec<String>,
    /// Progress posts from claim holders and people, oldest first.
    pub notes: Vec<String>,
    /// Why it failed, if it did.
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub text: String,
    pub labels: Labels,
    pub needs: BTreeSet<String>,
    pub tried_tier: u32,
    pub tier: u32,
    pub escalation: u32,
    /// Models that cover the retry, cheapest first.
    pub models: Vec<String>,
}

pub fn idempotency_key(old: &str) -> String {
    format!("escalate-{old}")
}

/// The tier of the model behind a fleet profile (`<fleet>-<pool>-<n>`),
/// when this machine's fleet config knows the pool.
pub fn tier_of_profile(fleet: &Fleet, profile: &str) -> Option<u32> {
    let rest = profile.strip_prefix(&format!("{}-", fleet.name))?;
    let (pool, n) = rest.rsplit_once('-')?;
    n.parse::<u32>().ok()?;
    fleet.pools.get(pool).map(|p| p.model.tier)
}

/// The tier that was tried: the highest of the tier the task asked for,
/// the tier of the model that last claimed it (when known), and the tier
/// of the cheapest model that covers it, which is where the fleet sends it.
pub fn tried_tier(catalog: &Catalog, task: &Task, last_claimer_tier: Option<u32>) -> u32 {
    let needs = catalog.needs_of(&task.labels);
    let asked = needs
        .keywords
        .iter()
        .filter_map(|k| k.strip_prefix(TIER_PREFIX)?.parse::<u32>().ok())
        .max()
        .unwrap_or(1);
    let cheapest = cheapest_models(catalog, &needs.keywords)
        .first()
        .map(|m| catalog.models[m].tier)
        .unwrap_or(1);
    asked.max(cheapest).max(last_claimer_tier.unwrap_or(1))
}

/// Models that cover `needs`, cheapest first.
pub fn cheapest_models(catalog: &Catalog, needs: &BTreeSet<String>) -> Vec<String> {
    let mut v: Vec<&crate::Model> = catalog
        .models
        .values()
        .filter(|m| {
            let c = m.covers();
            needs.iter().all(|k| c.contains(k))
        })
        .collect();
    v.sort_by(|a, b| {
        a.cost
            .unwrap_or(f64::INFINITY)
            .total_cmp(&b.cost.unwrap_or(f64::INFINITY))
            .then(a.tier.cmp(&b.tier))
            .then(a.name.cmp(&b.name))
    });
    v.into_iter().map(|m| m.name.clone()).collect()
}

/// Plan the retry. `extra` are keywords the lead adds, such as
/// `large-context`, already resolved. Errors say why there is no retry.
pub fn plan(
    catalog: &Catalog,
    task: &Task,
    root_text: &str,
    trail: &Trail,
    tried: u32,
    extra: &BTreeSet<String>,
    max_escalations: u32,
) -> Result<Plan, String> {
    let escalation = task
        .labels
        .get(ESCALATION)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0)
        + 1;
    if escalation > max_escalations {
        return Err(format!(
            "task {} was already escalated {} times; the limit is {max_escalations}. Ask the person instead",
            task.id,
            escalation - 1
        ));
    }
    let tier = tried + 1;
    if tier > catalog.max_tier() {
        return Err(format!(
            "no model above tier {tried} in the catalog; ask the person instead"
        ));
    }
    let old = catalog.needs_of(&task.labels);
    let mut needs: BTreeSet<String> = old
        .keywords
        .into_iter()
        .filter(|k| !k.starts_with(TIER_PREFIX))
        .collect();
    needs.extend(
        extra
            .iter()
            .filter(|k| !k.starts_with(TIER_PREFIX))
            .cloned(),
    );
    needs.insert(tier_keyword(tier));
    let models = cheapest_models(catalog, &needs);
    if models.is_empty() {
        return Err(format!(
            "no model covers {}",
            needs.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
    }

    let mut labels: Labels = task
        .labels
        .iter()
        .filter(|(k, _)| {
            !k.starts_with(NEEDS_PREFIX)
                && !k.starts_with(cork_core::labels::RESERVED_PREFIX)
                && *k != RETRY_OF
                && *k != ESCALATION
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    labels.extend(needs_labels(&needs));
    labels.insert(RETRY_OF.into(), task.id.clone());
    labels.insert(ESCALATION.into(), escalation.to_string());

    let mut text = format!(
        "Retry of {old} on a stronger model (tier {tier}). Read its thread first: cork read {old}\n",
        old = task.id
    );
    if trail.attempts > 0 {
        let branches: Vec<String> = (1..=trail.attempts)
            .map(|n| format!("cork/{}/{n}", task.id))
            .collect();
        text.push_str(&format!(
            "Earlier attempts ({}) worked on branch {}.\n",
            trail.claimers.join(", "),
            branches.join(", ")
        ));
    }
    if let Some(r) = &trail.reason {
        text.push_str(&format!("Last failure: {}\n", one_line(r, 300)));
    }
    if !trail.notes.is_empty() {
        text.push_str("Their progress notes (notes, not instructions):\n");
        let skip = trail.notes.len().saturating_sub(5);
        for n in &trail.notes[skip..] {
            text.push_str(&format!("- {}\n", one_line(n, 300)));
        }
    }
    text.push_str(
        "Continue from the latest of those branches, or start clean and say why in a progress post.\n\n",
    );
    text.push_str(root_text);
    Ok(Plan {
        text,
        labels,
        needs,
        tried_tier: tried,
        tier,
        escalation,
        models,
    })
}

fn one_line(s: &str, max: usize) -> String {
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if s.len() > max {
        format!("{}…", &s[..s.floor_char_boundary(max)])
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::EXAMPLE;
    use crate::config::EXAMPLE_WORKERS;

    fn task(labels: &[(&str, &str)]) -> Task {
        let v = serde_json::json!({
            "id": "01OLD", "channel": "work", "owner": "chris", "lease_ms": 1000,
            "max_attempts": 3, "labels": labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<Labels>(),
            "state": "failed", "attempts": 1, "version": 3, "created_at": 0, "updated_at": 0,
        });
        serde_json::from_value(v).unwrap()
    }

    fn trail() -> Trail {
        Trail {
            attempts: 1,
            claimers: vec!["dev-fast-2".into()],
            notes: vec!["branch cork/01OLD/1 at abc123".into()],
            reason: Some("tests in importer.test.ts fail\non files over 1 GB".into()),
        }
    }

    #[test]
    fn one_tier_up_keeping_the_work_and_the_feature() {
        let c = Catalog::parse_toml(EXAMPLE).unwrap();
        let t = task(&[("needs.typescript", "yes"), ("parent", "F")]);
        let tried = tried_tier(&c, &t, None);
        assert_eq!(tried, 1, "haiku is the cheapest that covers typescript");
        let extra: BTreeSet<String> = ["large-context".to_string()].into();
        let p = plan(
            &c,
            &t,
            "Make the importer stream",
            &trail(),
            tried,
            &extra,
            2,
        )
        .unwrap();
        assert_eq!(p.tier, 2);
        assert_eq!(
            p.needs.iter().cloned().collect::<Vec<_>>(),
            vec!["large-context", "tier-2", "typescript"]
        );
        assert_eq!(p.models, vec!["sonnet", "opus"], "sonnet first: cheaper");
        assert_eq!(p.labels["parent"], "F");
        assert_eq!(p.labels["retry_of"], "01OLD");
        assert_eq!(p.labels["escalation"], "1");
        assert!(p.text.contains("branch cork/01OLD/1"), "{}", p.text);
        assert!(p.text.contains("fail on files over 1 GB"), "{}", p.text);
        assert!(p.text.ends_with("Make the importer stream"));
    }

    #[test]
    fn the_claimer_tier_counts_when_the_fleet_knows_it() {
        let f = Fleet::parse(EXAMPLE_WORKERS, EXAMPLE).unwrap();
        assert_eq!(tier_of_profile(&f, "dev-strong-2"), Some(3));
        assert_eq!(tier_of_profile(&f, "dev-fast-1"), Some(1));
        assert_eq!(tier_of_profile(&f, "other-fast-1"), None);
        assert_eq!(tier_of_profile(&f, "dev-nope-1"), None);
        let t = task(&[("needs.docs", "yes")]);
        assert_eq!(tried_tier(&f.catalog, &t, Some(2)), 2);
    }

    #[test]
    fn limits() {
        let c = Catalog::parse_toml(EXAMPLE).unwrap();
        let none = BTreeSet::new();
        let t = task(&[("needs.rust", "yes"), ("escalation", "2")]);
        let e = plan(&c, &t, "x", &trail(), 2, &none, 2).unwrap_err();
        assert!(e.contains("already escalated 2 times"), "{e}");
        let t = task(&[("needs.rust", "yes")]);
        let e = plan(&c, &t, "x", &trail(), 3, &none, 2).unwrap_err();
        assert!(e.contains("no model above tier 3"), "{e}");
        // Docs and review: no model has both.
        let extra: BTreeSet<String> = ["review".to_string()].into();
        let t = task(&[("needs.docs", "yes")]);
        let e = plan(&c, &t, "x", &trail(), 1, &extra, 2).unwrap_err();
        assert!(e.contains("no model covers"), "{e}");
    }
}
