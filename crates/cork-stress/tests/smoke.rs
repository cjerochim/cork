//! Every scenario at a small scale, so CI keeps the stress tool working.
//! Real numbers come from `cargo run --release -p cork-stress`.

use cork_stress::Sync;
use cork_stress::scenarios::{Params, Scenario, run};

fn small() -> Params {
    Params {
        sync: Sync::Normal,
        scale: 0.02,
        soak_secs: 8,
        strict: false,
    }
}

async fn go(s: Scenario) {
    let r = run(s, &small()).await;
    r.print();
    assert!(r.passed(), "{:?}", r.failures);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throughput() {
    go(Scenario::Throughput).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn contention() {
    go(Scenario::Contention).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos() {
    go(Scenario::Chaos).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiters() {
    go(Scenario::Waiters).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soak() {
    go(Scenario::Soak).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lead() {
    // 20 features at this scale (the full run uses 100).
    let r = run(
        Scenario::Lead,
        &Params {
            scale: 0.2,
            ..small()
        },
    )
    .await;
    r.print();
    assert!(r.passed(), "{:?}", r.failures);
}
