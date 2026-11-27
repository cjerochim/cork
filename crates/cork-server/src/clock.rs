//! The daemon clock. Leases are decided from it, inside the write.
//!
//! It is monotonic: wall time read once at start, plus time elapsed since on a
//! monotonic timer. It starts no lower than the largest `at` already in the
//! log, so a wall clock that stepped back can never move time backwards
//! (HANDOFF 3).

use cork_core::Millis;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Millis;
}

pub struct SystemClock {
    base: Millis,
    start: Instant,
}

impl SystemClock {
    /// `floor` is the largest `at` in the log, if any.
    pub fn anchored(floor: Option<Millis>) -> Self {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as Millis)
            .unwrap_or(0);
        Self {
            base: wall.max(floor.map_or(0, |f| f + 1)),
            start: Instant::now(),
        }
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Millis {
        self.base + self.start.elapsed().as_millis() as Millis
    }
}

/// A clock tests move by hand.
pub struct ManualClock(AtomicI64);

impl ManualClock {
    pub fn new(at: Millis) -> Self {
        Self(AtomicI64::new(at))
    }
    pub fn set(&self, at: Millis) {
        self.0.store(at, Ordering::SeqCst);
    }
    pub fn advance(&self, ms: Millis) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Millis {
        self.0.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_above_the_log() {
        let far = 10_000_000_000_000; // year 2286
        let c = SystemClock::anchored(Some(far));
        assert!(c.now() > far);
        let a = c.now();
        assert!(c.now() >= a);
    }
}
