//! `cork-stress`: run the stress scenarios and print the results.
//!
//! ```text
//! cargo run --release -p cork-stress -- --sync full
//! cargo run --release -p cork-stress -- chaos --sync normal --scale 0.1
//! ```

use clap::Parser;
use cork_stress::Sync;
use cork_stress::scenarios::{Params, Scenario, run};

#[derive(Parser)]
#[command(about = "Stress tests for Cork, with log invariants")]
struct Args {
    /// Which scenario to run. Leave out to run all five.
    scenario: Option<Scenario>,
    /// SQLite durability. Run once with each to compare.
    #[arg(long, value_enum, default_value = "full")]
    sync: Sync,
    /// Shrink counts and durations (1.0 is the full size from the spec).
    #[arg(long, default_value_t = 1.0)]
    scale: f64,
    /// Length of the soak run in seconds (the spec asks for 1,800).
    #[arg(long, default_value_t = 1_800)]
    soak_secs: u64,
    /// Fail when a latency or memory target is missed, not only invariants.
    #[arg(long)]
    strict: bool,
}

#[tokio::main]
async fn main() {
    let a = Args::parse();
    let p = Params {
        sync: a.sync,
        scale: a.scale,
        soak_secs: a.soak_secs,
        strict: a.strict,
    };
    let list = match a.scenario {
        Some(s) => vec![s],
        None => vec![
            Scenario::Throughput,
            Scenario::Contention,
            Scenario::Chaos,
            Scenario::Waiters,
            Scenario::Soak,
        ],
    };
    let mut ok = true;
    for s in list {
        let r = run(s, &p).await;
        r.print();
        ok &= r.passed();
    }
    std::process::exit(if ok { 0 } else { 1 });
}
