//! HANDOFF 16.8, item 7: for 1,000 random catalogs and tasks, the pool the
//! dispatcher picks is the first one `workers match` ranks, and both agree
//! with a plain reading of the rules written out again here.

use cork_fleet::routing::{TaskFacts, choose, rank, verdict};
use cork_fleet::{Fleet, Needs};
use std::collections::BTreeSet;

/// A small deterministic generator, so a failure can be replayed.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}

const WORDS: [&str; 6] = ["rust", "ts", "docs", "vision", "review", "arch"];

fn random_fleet(r: &mut Rng) -> Fleet {
    let mut models = String::from("[keywords]\n");
    for w in WORDS {
        models.push_str(&format!("{w} = \"{w}\"\n"));
    }
    let n_models = 1 + r.below(4);
    for m in 0..n_models {
        let kws: Vec<String> = WORDS
            .iter()
            .filter(|_| r.chance(50))
            .map(|w| format!("\"{w}\""))
            .collect();
        models.push_str(&format!(
            "[model.m{m}]\ncommand = [\"true\"]\ntier = {}\n{}keywords = [{}]\n",
            1 + r.below(3),
            if r.chance(80) {
                format!("cost = {}\n", r.below(20))
            } else {
                String::new()
            },
            kws.join(",")
        ));
    }
    let mut workers = String::from("[fleet]\nname = \"f\"\nmodels = \"m\"\nmax_workers = 100\n");
    for p in 0..(1 + r.below(5)) {
        workers.push_str(&format!(
            "[pool.p{p}]\nmodel = \"m{}\"\nselect = \"{}\"\nexclusive = {}\n",
            r.below(n_models),
            if r.chance(70) { "work/" } else { "other" },
            r.chance(15)
        ));
    }
    Fleet::parse(&workers, &models).unwrap()
}

fn random_task(r: &mut Rng, f: &Fleet) -> TaskFacts {
    let mut keywords: BTreeSet<String> = WORDS
        .iter()
        .filter(|_| r.chance(25))
        .map(|w| w.to_string())
        .collect();
    if r.chance(30) {
        keywords.insert(format!("tier-{}", 1 + r.below(f.catalog.max_tier() as u64)));
    }
    let mut unknown = BTreeSet::new();
    if r.chance(5) {
        unknown.insert("typscript".to_string());
    }
    TaskFacts {
        channel: if r.chance(85) {
            "work/a".into()
        } else {
            "other".into()
        },
        owner: if r.chance(90) {
            "chris".into()
        } else {
            "sam".into()
        },
        assignee: None,
        needs: Needs { keywords, unknown },
    }
}

#[test]
fn match_and_dispatch_agree_on_1000_random_cases() {
    let mut r = Rng(0x9e37_79b9_7f4a_7c15);
    for case in 0..1000 {
        let f = random_fleet(&mut r);
        let t = random_task(&mut r, &f);
        let ranked = rank(f.pools.values(), &t, "chris", true);
        let all_free = choose(f.pools.values(), &t, "chris", |_| true);
        assert_eq!(
            ranked.first().map(|p| &p.name),
            all_free.map(|p| &p.name),
            "case {case}"
        );
        // The rules, read plainly.
        for p in f.pools.values() {
            let ok = p.select.matches(&t.channel)
                && t.owner == "chris"
                && t.needs.unknown.is_empty()
                && t.needs.keywords.iter().all(|k| p.covers.contains(k))
                && (!p.exclusive || t.needs.keywords.contains(&format!("tier-{}", p.model.tier)));
            assert_eq!(
                verdict(p, &t, "chris", true).covers(),
                ok,
                "case {case}, pool {}",
                p.name
            );
            assert_eq!(ranked.iter().any(|q| q.name == p.name), ok, "case {case}");
        }
        // Nothing cheaper than the choice covers the task.
        if let Some(c) = all_free {
            let cost = |p: &cork_fleet::Pool| p.model.cost.unwrap_or(f64::INFINITY);
            for q in &ranked {
                assert!(cost(q) >= cost(c), "case {case}");
            }
        }
        // With some pools busy, the choice is the first free one in the ranking.
        let busy: BTreeSet<String> = f.pools.keys().filter(|_| r.chance(40)).cloned().collect();
        let pick = choose(f.pools.values(), &t, "chris", |p| !busy.contains(&p.name));
        assert_eq!(
            pick.map(|p| &p.name),
            ranked
                .iter()
                .find(|p| !busy.contains(&p.name))
                .map(|p| &p.name),
            "case {case}"
        );
    }
}
