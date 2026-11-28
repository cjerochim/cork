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

#[tokio::test]
async fn rebuild_from_the_log_matches_the_live_projections() {
    use cork_core::{Actor, CreateTask, Labels};
    use cork_store::testkit::{create_task, decide};
    let dir = tempfile::tempdir().unwrap();
    let s = SqliteStore::open(SqliteConfig::new(dir.path().join("b.db")))
        .await
        .unwrap();
    let mut l = Labels::new();
    l.insert("step".into(), "fix".into());
    let input = CreateTask {
        lease_ms: Some(1_000),
        max_attempts: Some(3),
        assignee: None,
    };
    let a = create_task(&s, "work", l.clone(), input.clone(), 1).await;
    let b = create_task(&s, "work/x", Labels::new(), input, 2).await;
    let w = Actor::new("chris", Some("w".into())).unwrap();
    decide(&s, &a.id, 3, move |t| t.claim(3, &w, "c".into(), None))
        .await
        .unwrap();
    decide(&s, &a.id, 4, |t| t.complete(4, "c", Some("ok".into())))
        .await
        .unwrap();
    decide(&s, &b.id, 5, |t| t.cancel(5, &Actor::person("chris"), None))
        .await
        .unwrap();
    let before = s.projection_dump().await.unwrap();
    assert!(!before.is_empty());
    s.rebuild_projections().await.unwrap();
    let after = s.projection_dump().await.unwrap();
    assert_eq!(before, after);
}

#[tokio::test]
async fn a_database_from_before_patterns_learns_its_channels_on_open() {
    use cork_core::{ChannelSelector, CreateTask, Labels};
    use cork_store::testkit::create_task;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("b.db");
    let t = {
        let s = SqliteStore::open(SqliteConfig::new(&path)).await.unwrap();
        create_task(
            &s,
            "work/api/tests",
            Labels::new(),
            CreateTask::default(),
            1,
        )
        .await
    };
    // Make it look like a database written before `known_channels` existed.
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute_batch(
            "DELETE FROM known_channels; DELETE FROM meta WHERE key = 'known_channels';",
        )
        .unwrap();
    }
    let s = SqliteStore::open(SqliteConfig::new(&path)).await.unwrap();
    let got = s
        .list_tasks(&TaskQuery {
            channel: Some(ChannelSelector::parse("work/*/tests").unwrap()),
            ..TaskQuery::ready(2)
        })
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].task.id, t.id);
}
