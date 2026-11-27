//! Latency percentiles.

use std::time::Duration;

#[derive(Debug, Default, Clone)]
pub struct Samples(Vec<Duration>);

impl Samples {
    pub fn push(&mut self, d: Duration) {
        self.0.push(d);
    }
    pub fn extend(&mut self, other: Samples) {
        self.0.extend(other.0);
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn pct(&self, p: f64) -> Duration {
        if self.0.is_empty() {
            return Duration::ZERO;
        }
        let mut v = self.0.clone();
        v.sort();
        let i = ((v.len() as f64 - 1.0) * p).round() as usize;
        v[i.min(v.len() - 1)]
    }
    pub fn max(&self) -> Duration {
        self.0.iter().copied().max().unwrap_or_default()
    }
    pub fn summary(&self) -> String {
        format!(
            "n={} p50={:.1}ms p99={:.1}ms max={:.1}ms",
            self.len(),
            ms(self.pct(0.5)),
            ms(self.pct(0.99)),
            ms(self.max())
        )
    }
}

pub fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}
