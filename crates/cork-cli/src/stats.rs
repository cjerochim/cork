//! `cork stats` (HANDOFF 18.3): replay the log through `/events`, or read
//! an export, and print flow, usage and cost per successful task. It reads,
//! never writes.

use cork_api::EventOut;
use cork_client::{Client, ClientError};
use cork_stats::{By, Options, Price};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub struct StatsOpts {
    pub since: Option<Duration>,
    pub channel: Option<String>,
    pub labels: Vec<(String, String)>,
    pub feature: Option<String>,
    pub by: By,
    pub json: bool,
    pub csv: bool,
    pub reasons: bool,
    pub file: Option<PathBuf>,
    pub models: Option<PathBuf>,
    pub config: Option<PathBuf>,
}

fn usage(msg: impl Into<String>) -> ClientError {
    ClientError::new("usage", msg)
}

/// Every event, oldest first, optionally in a channel selector.
pub async fn fetch(
    client: &Client,
    channel: Option<&str>,
) -> Result<(Vec<EventOut>, i64), ClientError> {
    let mut out = vec![];
    let mut after = 0u64;
    let mut now = 0;
    loop {
        let mut q = vec![("after", after.to_string()), ("limit", "1000".to_string())];
        if let Some(c) = channel {
            q.push(("channel", c.to_string()));
        }
        let page = client.events(&q, Duration::ZERO).await?;
        now = now.max(page.now);
        let n = page.events.len();
        out.extend(page.events);
        if n == 0 || page.next_after <= after {
            break;
        }
        after = page.next_after;
    }
    Ok((out, now))
}

/// Read an export (JSONL of event envelopes).
pub fn read_export(path: &Path) -> Result<Vec<EventOut>, ClientError> {
    let text =
        std::fs::read_to_string(path).map_err(|e| usage(format!("{}: {e}", path.display())))?;
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .enumerate()
        .map(|(i, l)| {
            serde_json::from_str(l)
                .map_err(|e| usage(format!("{} line {}: {e}", path.display(), i + 1)))
        })
        .collect()
}

/// Prices, pools and fleet names from `models.toml` and `workers.toml`, if
/// they can be found. Stats work without them; they only name models for
/// attempts with no usage reply and price tokens with no cost.
fn fleet_info(home: &Path, o: &StatsOpts, opts: &mut Options) -> Result<(), ClientError> {
    let catalog = match crate::workers::load(home, o.models.as_deref(), o.config.as_deref()) {
        Ok(l) => {
            if let Some(f) = &l.fleet {
                opts.fleets.push(f.name.clone());
                for (name, p) in &f.pools {
                    opts.pool_models.insert(name.clone(), p.model.name.clone());
                }
            }
            l.catalog
        }
        // Asked for by name: a broken file is an error.
        Err(e) if o.models.is_some() || o.config.is_some() => return Err(e),
        Err(_) => return Ok(()),
    };
    for (name, m) in &catalog.models {
        opts.models.insert(name.clone());
        if let Some(p) = m.price {
            opts.prices.insert(
                name.clone(),
                Price {
                    input: p.input_per_mtok,
                    output: p.output_per_mtok,
                    cache_read: p.cache_read_per_mtok,
                    cache_write: p.cache_write_per_mtok,
                },
            );
        }
    }
    Ok(())
}

pub async fn stats(home: &Path, client: Option<&Client>, o: StatsOpts) -> Result<i32, ClientError> {
    let (mut events, now) = match (&o.file, client) {
        (Some(f), _) => {
            let ev = read_export(f)?;
            let now = ev.iter().map(|e| e.at).max().unwrap_or(0);
            (ev, now)
        }
        (None, Some(c)) => fetch(c, o.channel.as_deref()).await?,
        (None, None) => return Err(usage("stats needs a daemon or --file")),
    };
    if o.file.is_some()
        && let Some(sel) = &o.channel
    {
        let sel =
            cork_core::channel::ChannelSelector::parse(sel).map_err(|e| usage(e.to_string()))?;
        events.retain(|e| e.channel.as_deref().is_some_and(|c| sel.matches(c)));
    }
    let mut opts = Options {
        by: Some(o.by.clone()),
        since: o.since.map(|d| now - d.as_millis() as i64),
        labels: o.labels.clone(),
        feature: o.feature.clone(),
        reasons: o.reasons,
        ..Default::default()
    };
    fleet_info(home, &o, &mut opts)?;
    let r = cork_stats::report(&events, &opts);
    if o.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&r)
                .map_err(|e| ClientError::new("internal", e.to_string()))?
        );
    } else if o.csv {
        print!("{}", cork_stats::render_csv(&r));
    } else {
        print!("{}", cork_stats::render_text(&r));
    }
    Ok(0)
}
