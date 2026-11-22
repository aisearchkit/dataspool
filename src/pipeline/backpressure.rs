// SPDX-License-Identifier: AGPL-3.0-or-later
//! Adaptive write-side flow control driven by Elasticsearch responses and latency.

use std::time::Duration;

/// Signals observed from a destination bulk request.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClusterPressure {
    pub took: Duration,
    pub rejected: u64,
    pub status_code: u16,
    pub queue_utilization: f64,
}

/// Runtime limits selected for the next request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowWindow {
    pub batch_size: usize,
    pub max_in_flight: usize,
    pub pause: Duration,
}

#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    pub min_batch_size: usize,
    pub max_batch_size: usize,
    pub initial_batch_size: usize,
    pub min_in_flight: usize,
    pub max_in_flight: usize,
    pub target_latency: Duration,
    pub high_queue_utilization: f64,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            min_batch_size: 100,
            max_batch_size: 5_000,
            initial_batch_size: 1_000,
            min_in_flight: 1,
            max_in_flight: 8,
            target_latency: Duration::from_secs(2),
            high_queue_utilization: 0.80,
        }
    }
}

/// AIMD controller: multiplicatively decreases on pressure and cautiously
/// increases when the cluster remains healthy.
pub struct AdaptiveBackpressure {
    config: BackpressureConfig,
    window: FlowWindow,
    healthy_samples: u32,
}

impl AdaptiveBackpressure {
    pub fn new(config: BackpressureConfig) -> Self {
        let min_batch = config.min_batch_size.max(1);
        let max_batch = config.max_batch_size.max(min_batch);
        let batch = config.initial_batch_size.clamp(min_batch, max_batch);
        let min_flight = config.min_in_flight.max(1);
        Self {
            config,
            window: FlowWindow {
                batch_size: batch,
                max_in_flight: min_flight,
                pause: Duration::ZERO,
            },
            healthy_samples: 0,
        }
    }

    pub fn window(&self) -> FlowWindow {
        self.window
    }

    pub fn observe(&mut self, sample: ClusterPressure) -> FlowWindow {
        let overloaded = sample.rejected > 0
            || matches!(sample.status_code, 429 | 502 | 503 | 504)
            || sample.took > self.config.target_latency
            || sample.queue_utilization >= self.config.high_queue_utilization;

        if overloaded {
            self.healthy_samples = 0;
            self.window.batch_size =
                (self.window.batch_size / 2).max(self.config.min_batch_size.max(1));
            self.window.max_in_flight =
                (self.window.max_in_flight / 2).max(self.config.min_in_flight.max(1));
            self.window.pause = if sample.rejected > 0 || sample.status_code == 429 {
                Duration::from_millis(500)
            } else {
                Duration::from_millis(100)
            };
        } else {
            self.healthy_samples = self.healthy_samples.saturating_add(1);
            self.window.pause = Duration::ZERO;
            if self.healthy_samples >= 5 {
                self.healthy_samples = 0;
                self.window.batch_size = (self.window.batch_size
                    + (self.window.batch_size / 10).max(1))
                .min(self.config.max_batch_size);
                self.window.max_in_flight =
                    (self.window.max_in_flight + 1).min(self.config.max_in_flight);
            }
        }
        self.window
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backs_off_on_es_rejections_and_recovers_slowly() {
        let mut ctl = AdaptiveBackpressure::new(BackpressureConfig::default());
        let initial = ctl.window();
        let reduced = ctl.observe(ClusterPressure {
            status_code: 429,
            rejected: 10,
            ..Default::default()
        });
        assert!(reduced.batch_size < initial.batch_size);
        assert!(reduced.max_in_flight <= initial.max_in_flight);
        assert!(!reduced.pause.is_zero());

        for _ in 0..5 {
            ctl.observe(ClusterPressure {
                took: Duration::from_millis(50),
                status_code: 200,
                ..Default::default()
            });
        }
        assert!(ctl.window().batch_size >= reduced.batch_size);
    }

    #[test]
    fn respects_minimum_batch() {
        let mut ctl = AdaptiveBackpressure::new(BackpressureConfig {
            min_batch_size: 64,
            initial_batch_size: 64,
            ..Default::default()
        });
        for _ in 0..20 {
            ctl.observe(ClusterPressure {
                status_code: 429,
                rejected: 1,
                ..Default::default()
            });
        }
        assert_eq!(ctl.window().batch_size, 64);
    }
}
