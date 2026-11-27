//! The storage contract suite, run against SQLite.

use cork_store_sqlite::{SqliteConfig, SqliteStore};

async fn fresh_store() -> SqliteStore {
    // The directory must outlive the store, so it is kept until the process ends.
    let dir = tempfile::tempdir().unwrap().keep();
    SqliteStore::open(SqliteConfig::new(dir.join("board.db")))
        .await
        .unwrap()
}

cork_store::contract_tests!(fresh_store());
