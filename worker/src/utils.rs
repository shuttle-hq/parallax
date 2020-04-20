use std::time::{Duration, Instant};

pub struct Timer {
    start: Instant,
    expires_at: Instant,
}

impl Timer {
    pub fn new(time_out_millis: u64) -> Self {
        let now = Instant::now();
        Timer {
            start: now,
            expires_at: now + Duration::from_millis(time_out_millis),
        }
    }
    pub fn is_done(&self) -> bool {
        self.expires_at > Instant::now()
    }
}
