//! The fleet on one machine, `workers.toml` (HANDOFF 16.2): which pools run
//! here, on which model, how many, and over which channels.

use crate::catalog::{Catalog, Model, check_name, is_json, tier_keywords};
use cork_core::channel::ChannelSelector;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const DEFAULT_MAX_WORKERS: usize = 16;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFleetFile {
    fleet: RawFleet,
    #[serde(default)]
    pool: BTreeMap<String, RawPool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFleet {
    name: String,
    models: String,
    max_workers: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPool {
    model: String,
    select: String,
    count: Option<usize>,
    min: Option<usize>,
    max: Option<usize>,
    lease: Option<String>,
    max_runtime: Option<String>,
    command: Option<Vec<String>>,
    keywords: Option<Vec<String>>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    cwd: Option<String>,
    from: Option<String>,
    #[serde(default)]
    exclusive: bool,
    autoscale: Option<RawAutoscale>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAutoscale {
    target_ready_per_worker: f64,
    idle_after: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Autoscale {
    pub target_ready_per_worker: f64,
    pub idle_after: Duration,
}

/// One pool, resolved against the catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct Pool {
    pub name: String,
    pub model: Model,
    pub select: ChannelSelector,
    pub count: usize,
    pub min: usize,
    pub max: usize,
    pub lease: Option<Duration>,
    pub max_runtime: Option<Duration>,
    /// The command to run, `{model}` and `{pool}` filled in.
    pub command: Vec<String>,
    /// What this pool covers: its (maybe narrowed) keywords and its tiers.
    pub covers: BTreeSet<String>,
    pub env: BTreeMap<String, String>,
    pub cwd: Option<PathBuf>,
    /// Whose tasks to take: `None` for the fleet's person, `*` for anyone.
    pub from: Option<String>,
    pub exclusive: bool,
    pub autoscale: Option<Autoscale>,
}

impl Pool {
    /// What a running worker of this pool looks like; a change means
    /// `reload` must drain the old workers and start new ones (16.5).
    pub fn worker_shape(
        &self,
    ) -> (
        String,
        &[String],
        &BTreeMap<String, String>,
        &Option<PathBuf>,
    ) {
        (self.model.name.clone(), &self.command, &self.env, &self.cwd)
    }

    pub fn same_workers(&self, other: &Pool) -> bool {
        self.worker_shape() == other.worker_shape()
            && self.lease == other.lease
            && self.max_runtime == other.max_runtime
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Fleet {
    pub name: String,
    pub max_workers: usize,
    pub pools: BTreeMap<String, Pool>,
    pub catalog: Catalog,
    pub models_path: PathBuf,
    pub config_path: PathBuf,
}

impl Fleet {
    /// Read `workers.toml` (or `.json`) and the catalog it names, and check
    /// both. Unknown keywords, models and names are refused (16.6).
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("could not read {}: {e}", path.display()))?;
        let raw: RawFleetFile = if is_json(path) {
            serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?
        } else {
            toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?
        };
        let dir = path.parent().unwrap_or(Path::new("."));
        let models_path = dir.join(&raw.fleet.models);
        let catalog = Catalog::load(&models_path)?;
        Self::build(raw, catalog, dir, models_path, path.to_path_buf())
            .map_err(|e| format!("{}: {e}", path.display()))
    }

    /// Build from strings, for tests: `models` is TOML.
    pub fn parse(workers_toml: &str, models_toml: &str) -> Result<Self, String> {
        let raw: RawFleetFile = toml::from_str(workers_toml).map_err(|e| e.to_string())?;
        let catalog = Catalog::parse_toml(models_toml)?;
        Self::build(
            raw,
            catalog,
            Path::new("."),
            PathBuf::from("models.toml"),
            PathBuf::from("workers.toml"),
        )
    }

    fn build(
        raw: RawFleetFile,
        catalog: Catalog,
        dir: &Path,
        models_path: PathBuf,
        config_path: PathBuf,
    ) -> Result<Self, String> {
        check_name("fleet", &raw.fleet.name)?;
        let max_workers = raw.fleet.max_workers.unwrap_or(DEFAULT_MAX_WORKERS);
        if max_workers == 0 {
            return Err("fleet.max_workers must be at least 1".into());
        }
        let mut pools = BTreeMap::new();
        let mut total = 0;
        for (name, p) in raw.pool {
            check_name("pool", &name)?;
            let pool = build_pool(&name, p, &catalog, dir, max_workers)?;
            total += pool.count;
            pools.insert(name, pool);
        }
        if total > max_workers {
            return Err(format!(
                "pool counts add up to {total}, over fleet.max_workers {max_workers}"
            ));
        }
        Ok(Self {
            name: raw.fleet.name,
            max_workers,
            pools,
            catalog,
            models_path,
            config_path,
        })
    }
}

fn build_pool(
    name: &str,
    p: RawPool,
    catalog: &Catalog,
    dir: &Path,
    max_workers: usize,
) -> Result<Pool, String> {
    let model = catalog
        .models
        .get(&p.model)
        .ok_or_else(|| format!("pool `{name}`: no model `{}` in the catalog", p.model))?
        .clone();
    let select = ChannelSelector::parse(&p.select)
        .map_err(|e| format!("pool `{name}`: select: {}", e.message))?;
    let count = p.count.unwrap_or(1);
    let min = p.min.unwrap_or(0);
    let max = p.max.unwrap_or(max_workers.max(count));
    if min > max || count < min || count > max {
        return Err(format!(
            "pool `{name}`: needs min <= count <= max, got {min} <= {count} <= {max}"
        ));
    }
    let keywords: BTreeSet<String> = match &p.keywords {
        None => model.keywords.clone(),
        Some(list) => {
            let mut out = BTreeSet::new();
            for w in list {
                let k = catalog
                    .resolve(w)
                    .map_err(|e| format!("pool `{name}`: {e}"))?;
                if !model.keywords.contains(&k) {
                    return Err(format!(
                        "pool `{name}`: keywords may only narrow model `{}`, which lacks `{k}`",
                        model.name
                    ));
                }
                out.insert(k);
            }
            out
        }
    };
    let mut covers = keywords;
    covers.extend(tier_keywords(model.tier));
    let command: Vec<String> = p
        .command
        .clone()
        .unwrap_or_else(|| model.command.clone())
        .into_iter()
        .map(|a| a.replace("{model}", &model.name).replace("{pool}", name))
        .collect();
    if command.is_empty() || command[0].is_empty() {
        return Err(format!("pool `{name}`: empty command"));
    }
    let dur = |what: &str, s: &Option<String>| -> Result<Option<Duration>, String> {
        s.as_deref()
            .map(|s| parse_duration(s).map_err(|e| format!("pool `{name}`: {what}: {e}")))
            .transpose()
    };
    let autoscale = match p.autoscale {
        None => None,
        Some(a) => {
            if !(a.target_ready_per_worker.is_finite() && a.target_ready_per_worker > 0.0) {
                return Err(format!(
                    "pool `{name}`: autoscale.target_ready_per_worker must be > 0"
                ));
            }
            Some(Autoscale {
                target_ready_per_worker: a.target_ready_per_worker,
                idle_after: parse_duration(&a.idle_after)
                    .map_err(|e| format!("pool `{name}`: autoscale.idle_after: {e}"))?,
            })
        }
    };
    if let Some(f) = &p.from
        && f != "*"
    {
        cork_core::actor::validate_name("from", f).map_err(|e| e.message)?;
    }
    Ok(Pool {
        name: name.to_string(),
        select,
        count,
        min,
        max,
        lease: dur("lease", &p.lease)?,
        max_runtime: dur("max_runtime", &p.max_runtime)?,
        command,
        covers,
        env: p.env,
        cwd: p.cwd.map(|c| dir.join(c)),
        from: p.from,
        exclusive: p.exclusive,
        autoscale,
        model,
    })
}

/// `500ms`, `30s`, `5m`, `2h`; a bare number is seconds.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let split = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let (n, unit) = s.split_at(split);
    let bad = || format!("bad duration `{s}`: use 500ms, 30s, 5m or 2h");
    let n: f64 = n.parse().map_err(|_| bad())?;
    let ms = match unit {
        "ms" => n,
        "" | "s" => n * 1_000.0,
        "m" => n * 60_000.0,
        "h" => n * 3_600_000.0,
        _ => return Err(bad()),
    };
    Ok(Duration::from_millis(ms as u64))
}

#[cfg(test)]
pub(crate) const EXAMPLE_WORKERS: &str = r#"
[fleet]
name = "dev"
models = "models.toml"
max_workers = 12

[pool.strong]
model = "opus"
select = "work/"
count = 2
min = 0
max = 6
lease = "10m"
max_runtime = "2h"

[pool.fast]
model = "haiku"
select = "work/"
count = 5
max = 10

[pool.review]
model = "reviewer"
select = "work/"
count = 1
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::EXAMPLE;

    #[test]
    fn parses_the_spec_example() {
        let f = Fleet::parse(EXAMPLE_WORKERS, EXAMPLE).unwrap();
        assert_eq!(f.name, "dev");
        assert_eq!(f.pools.len(), 3);
        let s = &f.pools["strong"];
        assert_eq!(s.max, 6);
        assert_eq!(s.lease, Some(Duration::from_secs(600)));
        assert!(s.covers.contains("tier-3") && s.covers.contains("rust"));
        assert_eq!(f.pools["review"].max, 12, "max defaults to the fleet cap");
    }

    #[test]
    fn fills_model_and_pool_into_the_command() {
        let w = r#"
[fleet]
name = "dev"
models = "m"
[pool.lead]
model = "opus"
select = "features/"
command = ["claude", "--model", "{model}", "--pool={pool}"]
"#;
        let f = Fleet::parse(w, EXAMPLE).unwrap();
        assert_eq!(
            f.pools["lead"].command,
            vec!["claude", "--model", "opus", "--pool=lead"]
        );
    }

    #[test]
    fn narrowing_keywords() {
        let w = r#"
[fleet]
name = "dev"
models = "m"
[pool.r]
model = "opus"
select = "work/"
keywords = ["rust"]
"#;
        let f = Fleet::parse(w, EXAMPLE).unwrap();
        let c: Vec<_> = f.pools["r"].covers.iter().cloned().collect();
        assert_eq!(c, vec!["rust", "tier-1", "tier-2", "tier-3"]);
    }

    #[test]
    fn refuses_bad_fleets() {
        let head = "[fleet]\nname = \"dev\"\nmodels = \"m\"\nmax_workers = 4\n";
        let bad = [
            (
                "[pool.a]\nmodel = \"gpt\"\nselect = \"w\"",
                "no model `gpt`",
            ),
            (
                "[pool.a]\nmodel = \"haiku\"\nselect = \"w\"\nkeywords = [\"rust\"]",
                "lacks `rust`",
            ),
            (
                "[pool.a]\nmodel = \"haiku\"\nselect = \"w\"\nkeywords = [\"rustt\"]",
                "did you mean `rust`",
            ),
            (
                "[pool.a]\nmodel = \"haiku\"\nselect = \"w\"\ncount = 5",
                "over fleet.max_workers",
            ),
            (
                "[pool.a]\nmodel = \"haiku\"\nselect = \"w\"\ncount = 3\nmax = 2",
                "min <= count <= max",
            ),
            (
                "[pool.a]\nmodel = \"haiku\"\nselect = \"w\"\nlease = \"5 minutes\"",
                "bad duration",
            ),
            (
                "[pool.Big]\nmodel = \"haiku\"\nselect = \"w\"",
                "must be 1-32",
            ),
            (
                "[pool.a]\nmodel = \"haiku\"\nselect = \"w\"\nsize = 1",
                "unknown field",
            ),
        ];
        for (p, want) in bad {
            let e = Fleet::parse(&format!("{head}{p}"), EXAMPLE).unwrap_err();
            assert!(e.contains(want), "{p}: {e}");
        }
    }

    #[test]
    fn loads_files_and_json() {
        let d = std::env::temp_dir().join(format!("cork-fleet-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("models.toml"), EXAMPLE).unwrap();
        std::fs::write(d.join("workers.toml"), EXAMPLE_WORKERS).unwrap();
        let f = Fleet::load(&d.join("workers.toml")).unwrap();
        assert_eq!(f.pools.len(), 3);
        let j = r#"{"fleet":{"name":"j","models":"models.toml"},"pool":{"a":{"model":"haiku","select":"work/","cwd":"sub"}}}"#;
        std::fs::write(d.join("workers.json"), j).unwrap();
        let f = Fleet::load(&d.join("workers.json")).unwrap();
        assert_eq!(f.pools["a"].cwd.as_deref(), Some(d.join("sub").as_path()));
        let _ = std::fs::remove_dir_all(&d);
    }
}
