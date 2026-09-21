//! Small dependency-free metrics registry for the sync/auth/billing surface.
//!
//! The registry intentionally exposes only counters and bounded labels. A
//! deployment can scrape /internal/metrics and translate the output into
//! alerts without putting provider payloads or account identifiers in metric
//! labels.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const HISTOGRAM_BUCKETS_MS: &[u64] =
    &[1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000];

#[derive(Default)]
struct Histogram {
    buckets: BTreeMap<u64, u64>,
    count: u64,
    sum_ms: u64,
}

#[derive(Clone, Default)]
pub struct Metrics {
    counters: Arc<Mutex<BTreeMap<String, u64>>>,
    histograms: Arc<Mutex<BTreeMap<String, Histogram>>>,
}

impl Metrics {
    pub fn inc(&self, name: &str) {
        self.inc_labeled(name, &[]);
    }

    pub fn inc_labeled(&self, name: &str, labels: &[(&str, &str)]) {
        self.add_labeled(name, labels, 1);
    }

    pub fn add(&self, name: &str, value: u64) {
        self.add_labeled(name, &[], value);
    }

    pub fn add_labeled(&self, name: &str, labels: &[(&str, &str)], value: u64) {
        if value == 0 {
            return;
        }
        let key = metric_key(name, labels);
        let Ok(mut counters) = self.counters.lock() else {
            return;
        };
        let counter = counters.entry(key).or_default();
        *counter = counter.saturating_add(value);
    }

    pub fn observe_ms(&self, name: &str, duration: Duration) {
        self.observe_ms_labeled(name, &[], duration);
    }

    pub fn observe_ms_labeled(&self, name: &str, labels: &[(&str, &str)], duration: Duration) {
        let key = metric_key(name, labels);
        let milliseconds = duration.as_millis().min(u128::from(u64::MAX)) as u64;
        let Ok(mut histograms) = self.histograms.lock() else {
            return;
        };
        let histogram = histograms.entry(key).or_default();
        histogram.count = histogram.count.saturating_add(1);
        histogram.sum_ms = histogram.sum_ms.saturating_add(milliseconds);
        for bucket in HISTOGRAM_BUCKETS_MS {
            if milliseconds <= *bucket {
                let count = histogram.buckets.entry(*bucket).or_default();
                *count = count.saturating_add(1);
            }
        }
    }

    pub fn render_prometheus(&self) -> String {
        let Ok(counters) = self.counters.lock() else {
            return String::new();
        };
        let mut output = String::new();
        for (key, value) in counters.iter() {
            output.push_str(key);
            output.push(' ');
            output.push_str(&value.to_string());
            output.push('\n');
        }
        let Ok(histograms) = self.histograms.lock() else {
            return output;
        };
        for (key, histogram) in histograms.iter() {
            for bucket in HISTOGRAM_BUCKETS_MS {
                let bucket_key = metric_key_with_extra_label(key, "le", &bucket.to_string());
                output.push_str(&bucket_key);
                output.push(' ');
                output.push_str(
                    &histogram
                        .buckets
                        .get(bucket)
                        .copied()
                        .unwrap_or_default()
                        .to_string(),
                );
                output.push('\n');
            }
            let infinite_key = metric_key_with_extra_label(key, "le", "+Inf");
            output.push_str(&infinite_key);
            output.push(' ');
            output.push_str(&histogram.count.to_string());
            output.push('\n');
            output.push_str(&format_metric_suffix(key, "_sum"));
            output.push(' ');
            output.push_str(&histogram.sum_ms.to_string());
            output.push('\n');
            output.push_str(&format_metric_suffix(key, "_count"));
            output.push(' ');
            output.push_str(&histogram.count.to_string());
            output.push('\n');
        }
        output
    }
}

fn metric_key_with_extra_label(base: &str, label: &str, value: &str) -> String {
    if let Some((name, labels)) = base.split_once('{') {
        format!(
            "{name}_bucket{{{},{label}=\"{}\"",
            labels.trim_end_matches('}'),
            escape_label(value)
        ) + "}"
    } else {
        format!("{base}_bucket{{{label}=\"{}\"}}", escape_label(value))
    }
}

fn format_metric_suffix(base: &str, suffix: &str) -> String {
    if let Some((name, labels)) = base.split_once('{') {
        format!("{name}{suffix}{{{}", labels)
    } else {
        format!("{base}{suffix}")
    }
}

fn metric_key(name: &str, labels: &[(&str, &str)]) -> String {
    let name = sanitize_metric_name(name);
    if labels.is_empty() {
        return name;
    }
    let mut labels = labels
        .iter()
        .map(|(key, value)| (sanitize_metric_name(key), escape_label(value)))
        .collect::<Vec<_>>();
    labels.sort_by(|left, right| left.0.cmp(&right.0));
    let rendered = labels
        .into_iter()
        .map(|(key, value)| format!(r#"{key}="{value}""#))
        .collect::<Vec<_>>()
        .join(",");
    format!("{name}{{{rendered}}}")
}

fn sanitize_metric_name(value: &str) -> String {
    let mut result = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' || character == ':' {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if result.is_empty() || result.starts_with(|character: char| character.is_ascii_digit()) {
        result.insert(0, '_');
    }
    result
}

fn escape_label(value: &str) -> String {
    let mut escaped = String::new();
    for character in value.chars().take(256) {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            other if !other.is_control() => escaped.push(other),
            _ => {}
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_sorted_escaped_bounded_labels() {
        let metrics = Metrics::default();
        metrics.inc_labeled(
            "sync responses",
            &[("status", "5\"00"), ("route", "/sync\nreconcile")],
        );
        metrics.inc_labeled(
            "sync responses",
            &[("route", "/sync\nreconcile"), ("status", "5\"00")],
        );
        assert_eq!(
            metrics.render_prometheus(),
            "sync_responses{route=\"/sync\\nreconcile\",status=\"5\\\"00\"} 2\n"
        );
    }

    #[test]
    fn renders_counter_additions_and_latency_histograms() {
        let metrics = Metrics::default();
        metrics.add("payload_bytes_total", 7);
        metrics.observe_ms_labeled(
            "request_duration_ms",
            &[("route", "sync")],
            Duration::from_millis(12),
        );
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("payload_bytes_total 7\n"));
        assert!(rendered.contains("request_duration_ms_bucket{route=\"sync\",le=\"25\"} 1\n"));
        assert!(rendered.contains("request_duration_ms_bucket{route=\"sync\",le=\"+Inf\"} 1\n"));
        assert!(rendered.contains("request_duration_ms_sum{route=\"sync\"} 12\n"));
        assert!(rendered.contains("request_duration_ms_count{route=\"sync\"} 1\n"));
    }
}
