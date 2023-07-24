use std::time::{Duration, Instant};

/// Statistics regarding an connection returned by the pool
#[derive(Clone, Copy, Debug)]
#[must_use]
pub struct Metrics {
    /// The instant when this connection was created
    pub created: Instant,
    /// The instant when this connection was last used
    pub reused: Option<Instant>,
    /// The number of times the connections was reused
    pub reuse_count: usize,
}

impl Metrics {
    /// Access the age of this connection
    pub fn age(&self) -> Duration {
        self.created.elapsed()
    }
    /// Get the time elapsed when this connection was last used
    pub fn last_used(&self) -> Duration {
        self.reused.unwrap_or(self.created).elapsed()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            created: Instant::now(),
            reused: None,
            reuse_count: 0,
        }
    }
}
