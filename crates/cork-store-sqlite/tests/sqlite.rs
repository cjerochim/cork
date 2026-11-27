//! SQLite-specific behavior beyond the shared contract.

use cork_store::testkit::put;
use cork_store::*;
use cork_store_sqlite::{Durability, SqliteConfig, SqliteStore};

fn ev(n: u64) -> NewEvent {
    NewEvent::new(
        "A",
        serde_json::json!({ "n": n }),
        "t",
        n as i64,
        EventMeta::default(),
    )
}

fn commit(n: u64) -> Commit {
    Commit {
        appends: vec![StreamAppend {
            stream_id: "s:a".into(),
            expected: ExpectedVersion::Any,
            events: vec![ev(n)],
        }],
        projections: vec![],
    }
}

#[tokio::test]
async fn data_survives_reopen_and_positions_continue() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("b.db");
    {
        let s = SqliteStore::open(SqliteConfig::new(&path)).await.unwrap();
        for i in 1..=3 {
            put(&s, commit(i)).await.unwrap();
        }
    }
    let s = SqliteStore::open(SqliteConfig::new(&path)).await.unwrap();
    assert_eq!(s.head().await.unwrap(), 3);
    let out = put(&s, commit(4)).await.unwrap();
    assert_eq!(out[0].position, 4);
    assert_eq!(out[0].version, 4);
    assert_eq!(s.max_at().await.unwrap(), Some(4));
}

#[tokio::test]
async fn checkpoint_truncates_the_wal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("b.db");
    let mut cfg = SqliteConfig::new(&path);
    cfg.durability = Durability::Normal;
    let s = SqliteStore::open(cfg).await.unwrap();
    for i in 1..=200 {
        put(&s, commit(i)).await.unwrap();
    }
    let wal = dir.path().join("b.db-wal");
    assert!(std::fs::metadata(&wal).unwrap().len() > 0);
    s.checkpoint().await.unwrap();
    assert_eq!(std::fs::metadata(&wal).unwrap().len(), 0);
    assert_eq!(s.head().await.unwrap(), 200);
}

#[tokio::test]
async fn a_panicking_write_is_internal_and_the_writer_keeps_going() {
    let dir = tempfile::tempdir().unwrap();
    let s = SqliteStore::open(SqliteConfig::new(dir.path().join("b.db")))
        .await
        .unwrap();
    let r: cork_core::Result<(Vec<StoredEvent>, ())> = s.write(|_| panic!("boom")).await;
    assert_eq!(r.unwrap_err().code, cork_core::ErrorCode::Internal);
    put(&s, commit(1)).await.unwrap();
    assert_eq!(s.head().await.unwrap(), 1);
}
