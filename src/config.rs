use std::time::Duration;

#[derive(Debug, Clone)]
pub struct StrunkConfig {
    pub database_url: String,
    pub poll_interval: Duration,
    pub relay_batch_size: i64,
    pub reaper_retention_delivered: Duration,
    pub reaper_retention_dead: Duration,
    pub reaper_batch_size: i64,
    pub reaper_interval: Duration,
}

impl Default for StrunkConfig {
    fn default() -> Self {
        Self {
            database_url: String::new(),
            poll_interval: Duration::from_millis(100),
            relay_batch_size: 100,
            reaper_retention_delivered: Duration::from_secs(7 * 24 * 3600),
            reaper_retention_dead: Duration::from_secs(30 * 24 * 3600),
            reaper_batch_size: 10_000,
            reaper_interval: Duration::from_secs(300),
        }
    }
}
