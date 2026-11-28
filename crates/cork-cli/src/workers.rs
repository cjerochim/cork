//! `cork workers keywords` and `cork workers match`, and finding the model
//! catalog for `tasks create --needs` (HANDOFF 16.3, 16.6). These read only
//! the files and the board, so they work without a running fleet.

use cork_api::TaskOut;
use cork_client::{Client, ClientError};
use cork_fleet::routing::{TaskFacts, Verdict, channel_note, model_verdict, rank, verdict};
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

/// `cork workers match <task-id | --needs k1,k2 [--channel c]>`.
pub async fn matches(
    l: &Loaded,
    client: Option<&Client>,
    task_id: Option<&str>,
    needs: &[String],
    channel: Option<&str>,
    json_out: bool,
) -> Result<i32, ClientError> {
    let c = &l.catalog;
    let planned = !needs.is_empty() || channel.is_some();
    let (mut facts, person, mut place) = match (task_id, planned) {
        (Some(id), false) => {
            let client = client.expect("a task id needs the board");
            let v = client.task(id).await?;
            let t: TaskOut = serde_json::from_value(v)
                .map_err(|e| ClientError::new("internal", e.to_string()))?;
            let person = client.whoami().await?.person;
            (TaskFacts::of(&t.task, c), person, true)
        }
        (None, true) => {
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
        _ => {
            return Err(usage(
                "workers match takes a task id, or --needs k1,k2 and/or --channel c",
            ));
        }
    };
    if let Some(ch) = channel {
        facts.channel = cork_core::normalize_channel(ch).map_err(|e| usage(e.message))?;
        place = true;
    }
    // How each pool's select judges the channel (19.8).
    let note = |p: &cork_fleet::Pool| -> String {
        if place {
            format!(" ({})", channel_note(p, &facts.channel))
        } else {
            String::new()
        }
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
                let select = place.then(|| channel_note(p, &facts.channel));
                json!({"pool": p.name, "model": p.model.name, "covers": v.covers(), "why": v.to_string(), "select": select})
            })
            .collect();
        crate::print_json(&json!({
            "channel": place.then_some(&facts.channel),
            "needs": facts.needs.keywords,
            "unknown": facts.needs.unknown,
            "models": mrows,
            "fleet": l.fleet.as_ref().map(|f| f.name.clone()),
            "pools": prows,
            "ranked": ranked.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
            "choice": choice.map(|p| json!({"pool": p.name, "model": p.model.name})),
        }));
    } else {
        if place {
            let needs: Vec<&String> = facts.needs.keywords.iter().collect();
            println!(
                "task in {}, needs {}",
                facts.channel,
                if needs.is_empty() {
                    "nothing".to_string()
                } else {
                    needs
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            );
        }
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
                    "pool {} ({}): {}{}",
                    p.name,
                    p.model.name,
                    verdict(p, &facts, &person, place),
                    note(p)
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

// ---------- the running fleet ----------

/// `cork workers up`: run the supervisor here, or in the background.
pub async fn up(home: &Path, config: Option<&Path>, detach: bool) -> Result<i32, ClientError> {
    let path = find_config(home, config).ok_or_else(|| {
        usage(format!(
            "no fleet config: pass --config FILE, set CORK_WORKERS, or write {}",
            home.join("workers.toml").display()
        ))
    })?;
    let path =
        std::fs::canonicalize(&path).map_err(|e| usage(format!("{}: {e}", path.display())))?;
    let fleet = Fleet::load(&path).map_err(usage)?;
    let exe = std::env::current_exe().map_err(|e| ClientError::new("internal", e.to_string()))?;
    let sock = cork_fleet::supervisor::socket_path(home, &fleet.name);
    if detach {
        let dir = home.join("workers");
        std::fs::create_dir_all(&dir).map_err(|e| ClientError::new("internal", e.to_string()))?;
        let log_path = dir.join(format!("{}.log", fleet.name));
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| ClientError::new("internal", e.to_string()))?;
        let mut cmd = std::process::Command::new(&exe);
        cmd.args(["workers", "up", "--config"])
            .arg(&path)
            .env("CORK_HOME", home)
            .stdin(std::process::Stdio::null())
            .stdout(
                log.try_clone()
                    .map_err(|e| ClientError::new("internal", e.to_string()))?,
            )
            .stderr(log);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }
        let child = cmd
            .spawn()
            .map_err(|e| ClientError::new("internal", e.to_string()))?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if cork_fleet::supervisor::call(&sock, &json!({"op": "status"}))
                .await
                .is_ok()
            {
                println!(
                    "fleet {} running as pid {}; log {}",
                    fleet.name,
                    child.id(),
                    log_path.display()
                );
                return Ok(0);
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        return Err(ClientError::new(
            "internal",
            format!(
                "the fleet did not start within 10 s; see {}",
                log_path.display()
            ),
        ));
    }
    let conn = cork_client::home::Connect::from_env(Some(fleet.name.clone()));
    cork_fleet::supervisor::run(cork_fleet::supervisor::Options {
        fleet,
        cork_exe: exe,
        conn,
        home: home.to_path_buf(),
    })
    .await
    .map_err(|e| ClientError::new("internal", e))
}

/// Which fleet's socket to talk to: `--fleet`, else the config's name, else
/// the only socket there is.
fn pick_socket(
    home: &Path,
    fleet: Option<&str>,
    config: Option<&Path>,
) -> Result<PathBuf, ClientError> {
    if let Some(f) = fleet {
        return Ok(cork_fleet::supervisor::socket_path(home, f));
    }
    if let Some(p) = find_config(home, config) {
        let f = Fleet::load(&p).map_err(usage)?;
        return Ok(cork_fleet::supervisor::socket_path(home, &f.name));
    }
    let dir = home.join("workers");
    let socks: Vec<PathBuf> = std::fs::read_dir(&dir)
        .map(|r| {
            r.flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|e| e == "sock"))
                .collect()
        })
        .unwrap_or_default();
    match socks.as_slice() {
        [one] => Ok(one.clone()),
        [] => Err(usage(
            "no fleet is running here; start one with `cork workers up`",
        )),
        _ => Err(usage("more than one fleet is running; pass --fleet NAME")),
    }
}

pub async fn control(
    home: &Path,
    fleet: Option<&str>,
    config: Option<&Path>,
    req: Value,
    json_out: bool,
) -> Result<i32, ClientError> {
    let sock = pick_socket(home, fleet, config)?;
    let v = cork_fleet::supervisor::call(&sock, &req)
        .await
        .map_err(|e| ClientError::new("transport", e))?;
    if json_out {
        crate::print_json(&v);
    } else if v["ok"] != true {
        // Printed by the caller's error path.
    } else if req["op"] == "status" {
        print_status(&v);
    } else if req["op"] == "metrics" {
        print!("{}", v["text"].as_str().unwrap_or(""));
    } else {
        println!("{}", v["message"].as_str().unwrap_or("ok"));
    }
    if v["ok"] == true {
        Ok(0)
    } else {
        Err(usage(v["error"].as_str().unwrap_or("the fleet refused")))
    }
}

fn print_status(v: &Value) {
    let down = match v["shutting_down"].as_str() {
        Some(s) => format!(", shutting down ({s})"),
        None => String::new(),
    };
    println!(
        "fleet {} as {}, up {} s{down}",
        v["fleet"].as_str().unwrap_or(""),
        v["person"].as_str().unwrap_or(""),
        v["up_secs"]
    );
    for p in v["pools"].as_array().into_iter().flatten() {
        let mut flags = vec![];
        if p["paused"] == true {
            flags.push(format!(
                "PAUSED: {}",
                p["last_error"].as_str().unwrap_or("")
            ));
        }
        if p["removed"] == true {
            flags.push("removed".into());
        }
        if p["autoscale"] == true {
            flags.push("autoscale".into());
        }
        let flags = if flags.is_empty() {
            String::new()
        } else {
            format!("  [{}]", flags.join("; "))
        };
        println!(
            "pool {} ({}, tier {}) on {}: {}/{} workers (min {}, max {}), {} busy, {} idle, {} draining, {} restarts{flags}",
            p["pool"].as_str().unwrap_or(""),
            p["model"].as_str().unwrap_or(""),
            p["tier"],
            p["select"].as_str().unwrap_or(""),
            p["live"],
            p["target"],
            p["min"],
            p["max"],
            p["busy"],
            p["idle"],
            p["draining"],
            p["restarts"],
        );
        for w in p["workers"].as_array().into_iter().flatten() {
            let what = match w["task"].as_str() {
                Some(t) => format!("on {t}"),
                None if w["backing_off"] == true => "backing off".into(),
                None => "idle".into(),
            };
            let d = if w["draining"] == true {
                " (draining)"
            } else {
                ""
            };
            println!("  {}\t{what}{d}", w["profile"].as_str().unwrap_or(""));
        }
    }
    for o in v["overlaps"].as_array().into_iter().flatten() {
        println!("{}", o.as_str().unwrap_or(""));
    }
    let un = v["unmatched"].as_array().cloned().unwrap_or_default();
    if !un.is_empty() {
        println!("unmatched: {} ready tasks no pool covers", un.len());
        for u in un {
            let needs: Vec<String> = u["needs"]
                .as_array()
                .into_iter()
                .flatten()
                .chain(u["unknown"].as_array().into_iter().flatten())
                .filter_map(|x| x.as_str().map(String::from))
                .collect();
            println!(
                "  {} in {} needs {}",
                u["task"].as_str().unwrap_or(""),
                u["channel"].as_str().unwrap_or(""),
                needs.join(", ")
            );
        }
    }
}
