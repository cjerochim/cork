//! The storage contract suite again, with every wildcard pattern forced onto
//! the fallback scan instead of the list of known channels (HANDOFF 19.5).
//! Its own test binary, so the setting can't leak into other tests.

use cork_store_sqlite::{SqliteConfig, SqliteStore, set_pattern_list_max};

async fn fresh_store() -> SqliteStore {
    set_pattern_list_max(0);
    let dir = tempfile::tempdir().unwrap().keep();
    SqliteStore::open(SqliteConfig::new(dir.join("board.db")))
        .await
        .unwrap()
}

cork_store::contract_tests!(fresh_store());
