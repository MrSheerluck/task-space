//! Small dependency-free metrics registry for the sync/auth/billing surface.
//!
//! The registry intentionally exposes only counters and bounded labels. A
//! deployment can scrape /internal/metrics and translate the output into
//! alerts without putting provider payloads or account identifiers in metric
//! labels.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct Metrics {
    counters: Arc<Mutex<BTreeMap<String, u64>>>,
}

impl Metrics {
    pub fn inc(&self, name: &str) {
        self.inc_labeled(name, &[]);
    }

    pub fn inc_labeled(&self, name: &str, labels: &[(&str, &str)]) {
        let key = metric_key(name, labels);
        let Ok(mut counters) = self.counters.lock() else {
            return;
        };
        let counter = counters.entry(key).or_default();
        *counter = counter.saturating_add(1);
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
        output
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
}
