//! `cork workers keywords` and `cork workers match`, and finding the model
//! catalog for `tasks create --needs` (HANDOFF 16.3, 16.6). These read only
//! the files and the board, so they work without a running fleet.

use cork_api::TaskOut;
use cork_client::{Client, ClientError};
use cork_fleet::routing::{TaskFacts, Verdict, model_verdict, rank, verdict};
use cork_fleet::{Catalog, Fleet, Needs};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

fn usage(msg: impl Into<String>) -> ClientError {
    ClientError::new("usage", msg)
}

/// Where the fleet config is: `--config`, `CORK_WORKERS`, or
/// `<CORK_HOME>/workers.toml` (or `.json`) if it exists.
pub fn find_config(home: &Path, flag: Option<&Path>) -> Option<PathBuf> {
    if let Some(p) = flag {
        return Some(p.to_path_buf());
    }
    if let Some(p) = std::env::var_os("CORK_WORKERS").filter(|p| !p.is_empty()) {
        return Some(PathBuf::from(p));
    }
    ["workers.toml", "workers.json"]
        .iter()
        .map(|f| home.join(f))
        .find(|p| p.exists())
}

/// The catalog and, when there is one, the fleet config that names it.
pub struct Loaded {
    pub catalog: Catalog,
    pub fleet: Option<Fleet>,
}

/// Find the catalog: `--models`/`CORK_MODELS`, else the one the fleet
/// config names, else `<CORK_HOME>/models.toml` (or `.json`).
pub fn load(
    home: &Path,
    models_flag: Option<&Path>,
    config_flag: Option<&Path>,
) -> Result<Loaded, ClientError> {
    let models = models_flag.map(Path::to_path_buf).or_else(|| {
        std::env::var_os("CORK_MODELS")
            .filter(|p| !p.is_empty())
            .map(PathBuf::from)
    });
    let fleet = match find_config(home, config_flag) {
        Some(p) => Some(Fleet::load(&p).map_err(usage)?),
        None => None,
    };
    let catalog = match (&models, &fleet) {
        (Some(m), _) => Catalog::load(m).map_err(usage)?,
        (None, Some(f)) => f.catalog.clone(),
        (None, None) => {
            let p = ["models.toml", "models.json"]
                .iter()
                .map(|f| home.join(f))
                .find(|p| p.exists())
                .ok_or_else(|| {
                    usage(format!(
                        "no model catalog: pass --models FILE, set CORK_MODELS, or write {}",
                        home.join("models.toml").display()
                    ))
                })?;
            Catalog::load(&p).map_err(usage)?
        }
    };
    Ok(Loaded { catalog, fleet })
}

/// Check `--needs` words against the vocabulary (16.6). Unknown words are
/// an error that names the closest keyword; aliases become their keyword.
pub fn needs_labels(
    catalog: &Catalog,
    needs: &[String],
) -> Result<Vec<(String, String)>, ClientError> {
    match catalog.resolve_list(needs) {
        Ok(k) => Ok(cork_fleet::catalog::needs_labels(&k)),
        Err(bad) => Err(usage(
            bad.iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("\n"),
        )),
    }
}

pub fn keywords(l: &Loaded, json_out: bool) {
    let c = &l.catalog;
    let cov = c.coverage();
    if json_out {
        let kws: Vec<Value> = cov
            .iter()
            .map(|(k, models)| {
                json!({
                    "keyword": k,
                    "description": c.keywords.get(k),
                    "aliases": c.aliases.iter().filter(|(_, t)| *t == k).map(|(a, _)| a).collect::<Vec<_>>(),
                    "models": models,
                })
            })
            .collect();
        crate::print_json(&json!({ "keywords": kws }));
        return;
    }
    for (k, models) in &cov {
        let aliases: Vec<&String> = c
            .aliases
            .iter()
            .filter(|(_, t)| *t == k)
            .map(|(a, _)| a)
            .collect();
        let aka = if aliases.is_empty() {
            String::new()
        } else {
            format!(
                " (also {})",
                aliases
                    .iter()
                    .map(|a| a.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let desc = c
            .keywords
            .get(k)
            .map(|d| format!("  {d}"))
            .unwrap_or_default();
        let who = if models.is_empty() {
            "no model".to_string()
        } else {
            models.join(", ")
        };
        println!("{k}{aka}\t{who}{desc}");
    }
}

/// `cork workers match <task-id | --needs k1,k2>`.
pub async fn matches(
    l: &Loaded,
    client: Option<&Client>,
    task_id: Option<&str>,
    needs: &[String],
    json_out: bool,
) -> Result<i32, ClientError> {
    let c = &l.catalog;
    let (facts, person, place) = match (task_id, needs.is_empty()) {
        (Some(id), true) => {
            let client = client.expect("a task id needs the board");
            let v = client.task(id).await?;
            let t: TaskOut = serde_json::from_value(v)
                .map_err(|e| ClientError::new("internal", e.to_string()))?;
            let person = client.whoami().await?.person;
            (TaskFacts::of(&t.task, c), person, true)
        }
        (None, false) => {
            let mut n = Needs::default();
            match c.resolve_list(needs) {
                Ok(k) => n.keywords = k,
                Err(bad) => {
                    n.unknown = bad.iter().map(|e| e.word.clone()).collect();
                    for e in &bad {
                        eprintln!("cork: {e}");
                    }
                }
            }
            (TaskFacts::planned(n), String::new(), false)
        }
        _ => return Err(usage("workers match takes a task id or --needs k1,k2")),
    };

    let mut models: Vec<&cork_fleet::Model> = c.models.values().collect();
    models.sort_by(|a, b| {
        a.cost
            .unwrap_or(f64::INFINITY)
            .total_cmp(&b.cost.unwrap_or(f64::INFINITY))
            .then(a.tier.cmp(&b.tier))
            .then(a.name.cmp(&b.name))
    });
    let model_rows: Vec<(&cork_fleet::Model, Verdict)> = models
        .iter()
        .map(|m| (*m, model_verdict(m, &facts.needs)))
        .collect();
    let cheapest = model_rows
        .iter()
        .find(|(m, v)| v.covers() && m.cost.is_some_and(|c| c > 0.0))
        .map(|(m, _)| *m);
    let pools: Vec<&cork_fleet::Pool> = l
        .fleet
        .as_ref()
        .map(|f| f.pools.values().collect())
        .unwrap_or_default();
    let ranked = rank(pools.iter().copied(), &facts, &person, place);
    let choice = ranked.first();

    if json_out {
        let mrows: Vec<Value> = model_rows
            .iter()
            .map(|(m, v)| {
                json!({"model": m.name, "tier": m.tier, "cost": m.cost, "covers": v.covers(), "why": v.to_string()})
            })
            .collect();
        let prows: Vec<Value> = pools
            .iter()
            .map(|p| {
                let v = verdict(p, &facts, &person, place);
                json!({"pool": p.name, "model": p.model.name, "covers": v.covers(), "why": v.to_string()})
            })
            .collect();
        crate::print_json(&json!({
            "needs": facts.needs.keywords,
            "unknown": facts.needs.unknown,
            "models": mrows,
            "fleet": l.fleet.as_ref().map(|f| f.name.clone()),
            "pools": prows,
            "ranked": ranked.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
            "choice": choice.map(|p| json!({"pool": p.name, "model": p.model.name})),
        }));
    } else {
        for (m, v) in &model_rows {
            let mut line = format!("{}: {v}", m.name);
            if v.covers()
                && let (Some(base), Some(cost)) = (cheapest, m.cost)
                && base.name != m.name
                && let Some(bc) = base.cost
            {
                let x = cost / bc;
                let x = if x.fract() == 0.0 {
                    format!("{x:.0}")
                } else {
                    format!("{x:.1}")
                };
                line.push_str(&format!(" (costs {x}x {})", base.name));
            }
            println!("{line}");
        }
        if let Some(f) = &l.fleet {
            for p in &pools {
                println!(
                    "pool {} ({}): {}",
                    p.name,
                    p.model.name,
                    verdict(p, &facts, &person, place)
                );
            }
            match choice {
                Some(p) => println!(
                    "fleet {} gives it to pool {} ({}), the cheapest that covers it",
                    f.name, p.name, p.model.name
                ),
                None => println!("no pool in fleet {} covers it", f.name),
            }
        }
    }
    let any = if l.fleet.is_some() {
        choice.is_some()
    } else {
        model_rows.iter().any(|(_, v)| v.covers())
    };
    // 3 means nothing can take it, like `nothing_ready`.
    Ok(if any { 0 } else { 3 })
}
