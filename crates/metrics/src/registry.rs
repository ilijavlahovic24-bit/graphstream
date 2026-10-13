use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// A monotonic counter. Backed by an atomic so updates are lock-free.
#[derive(Debug, Default)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    pub fn new() -> Self { Self::default() }

    pub fn inc(&self, by: u64) {
        self.value.fetch_add(by, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// A gauge: a value that can go up and down. Stored as f64 bits in an
/// atomic so no lock is needed for writes.
#[derive(Debug, Default)]
pub struct Gauge {
    bits: AtomicU64,
}

impl Gauge {
    pub fn new() -> Self { Self::default() }

    pub fn set(&self, v: f64) {
        self.bits.store(v.to_bits(), Ordering::Relaxed);
    }

    pub fn get(&self) -> f64 {
        f64::from_bits(self.bits.load(Ordering::Relaxed))
    }
}

/// Simple fixed-bucket histogram. Bucket bounds are supplied at
/// construction; observations are counted into the matching bucket and
/// also accumulated into `sum` and `count`.
#[derive(Debug)]
pub struct Histogram {
    buckets: Vec<f64>,
    counts: Vec<AtomicU64>,
    sum_bits: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    pub fn new(buckets: Vec<f64>) -> Self {
        let n = buckets.len();
        let mut counts = Vec::with_capacity(n);
        for _ in 0..n {
            counts.push(AtomicU64::new(0));
        }
        Self {
            buckets,
            counts,
            sum_bits: AtomicU64::new(0f64.to_bits()),
            count: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, v: f64) {
        for (i, bound) in self.buckets.iter().enumerate() {
            if v <= *bound {
                self.counts[i].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        let prev = f64::from_bits(self.sum_bits.load(Ordering::Relaxed));
        // Best-effort non-atomic add; races may lose a sample under
        // contention, acceptable for observability.
        self.sum_bits.store((prev + v).to_bits(), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            buckets: self.buckets.clone(),
            counts: self.counts.iter().map(|c| c.load(Ordering::Relaxed)).collect(),
            sum: f64::from_bits(self.sum_bits.load(Ordering::Relaxed)),
            count: self.count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
    pub buckets: Vec<f64>,
    pub counts: Vec<u64>,
    pub sum: f64,
    pub count: u64,
}

/// Prometheus-style metric registry.
///
/// Every metric has a name and a set of labels. Labels are stored as a
/// sorted `Vec<(String, String)>` so rendering is deterministic.
#[derive(Debug, Default)]
pub struct Registry {
    counters: Mutex<BTreeMap<(String, Vec<(String, String)>), Counter>>,
    gauges:   Mutex<BTreeMap<(String, Vec<(String, String)>), Gauge>>,
    histograms: Mutex<BTreeMap<(String, Vec<(String, String)>), Histogram>>,
    help: Mutex<BTreeMap<String, String>>,
}

impl Registry {
    pub fn new() -> Self { Self::default() }

    pub fn set_help(&self, name: &str, help: &str) {
        self.help.lock().unwrap().insert(name.into(), help.into());
    }

    fn sorted_labels(labels: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    pub fn counter(&self, name: &str, labels: &[(&str, &str)]) -> Counter {
        let key = (name.to_string(), Self::sorted_labels(labels));
        let mut m = self.counters.lock().unwrap();
        if let Some(c) = m.get(&key) {
            return Counter { value: AtomicU64::new(c.get()) };
        }
        let c = Counter::new();
        m.insert(key, Counter::new());
        c
    }

    /// Get (creating if needed) the counter stored at this key, and
    /// increment it by `by`. Returns the new value.
    pub fn inc_counter(&self, name: &str, labels: &[(&str, &str)], by: u64) -> u64 {
        let key = (name.to_string(), Self::sorted_labels(labels));
        let mut m = self.counters.lock().unwrap();
        let c = m.entry(key).or_insert_with(Counter::new);
        c.inc(by);
        c.get()
    }

    pub fn set_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        let key = (name.to_string(), Self::sorted_labels(labels));
        let mut m = self.gauges.lock().unwrap();
        let g = m.entry(key).or_insert_with(Gauge::new);
        g.set(value);
    }

    pub fn observe_histogram(&self, name: &str, labels: &[(&str, &str)], value: f64, buckets: &[f64]) {
        let key = (name.to_string(), Self::sorted_labels(labels));
        let mut m = self.histograms.lock().unwrap();
        let h = m.entry(key).or_insert_with(|| Histogram::new(buckets.to_vec()));
        h.observe(value);
    }

    /// Render the whole registry in Prometheus exposition format.
    pub fn render(&self) -> String {
        let mut out = String::new();
        let help = self.help.lock().unwrap();

        // Counters
        {
            let counters = self.counters.lock().unwrap();
            let mut by_name: BTreeMap<&str, Vec<(&Vec<(String, String)>, u64)>> = BTreeMap::new();
            for ((name, labels), c) in counters.iter() {
                by_name.entry(name).or_default().push((labels, c.get()));
            }
            for (name, entries) in by_name {
                write_help_type(&mut out, name, help.get(name).map(String::as_str), "counter");
                for (labels, value) in entries {
                    let _ = writeln!(&mut out, "{}{} {}", name, fmt_labels(labels), value);
                }
            }
        }

        // Gauges
        {
            let gauges = self.gauges.lock().unwrap();
            let mut by_name: BTreeMap<&str, Vec<(&Vec<(String, String)>, f64)>> = BTreeMap::new();
            for ((name, labels), g) in gauges.iter() {
                by_name.entry(name).or_default().push((labels, g.get()));
            }
            for (name, entries) in by_name {
                write_help_type(&mut out, name, help.get(name).map(String::as_str), "gauge");
                for (labels, value) in entries {
                    let _ = writeln!(&mut out, "{}{} {}", name, fmt_labels(labels), value);
                }
            }
        }

        // Histograms
        {
            let histograms = self.histograms.lock().unwrap();
            let mut by_name: BTreeMap<&str, Vec<(&Vec<(String, String)>, HistogramSnapshot)>> = BTreeMap::new();
            for ((name, labels), h) in histograms.iter() {
                by_name.entry(name).or_default().push((labels, h.snapshot()));
            }
            for (name, entries) in by_name {
                write_help_type(&mut out, name, help.get(name).map(String::as_str), "histogram");
                for (labels, snap) in entries {
                    let mut cumulative = 0u64;
                    for (i, bound) in snap.buckets.iter().enumerate() {
                        cumulative += snap.counts[i];
                        let mut l = labels.clone();
                        l.push(("le".into(), bound.to_string()));
                        let _ = writeln!(
                            &mut out,
                            "{}_bucket{} {}",
                            name, fmt_labels(&l), cumulative
                        );
                    }
                    let mut l = labels.clone();
                    l.push(("le".into(), "+Inf".into()));
                    let _ = writeln!(
                        &mut out,
                        "{}_bucket{} {}",
                        name, fmt_labels(&l), snap.count
                    );
                    let _ = writeln!(&mut out, "{}_sum{} {}", name, fmt_labels(labels), snap.sum);
                    let _ = writeln!(&mut out, "{}_count{} {}", name, fmt_labels(labels), snap.count);
                }
            }
        }

        out
    }
}

fn write_help_type(out: &mut String, name: &str, help: Option<&str>, kind: &str) {
    if let Some(h) = help {
        let _ = writeln!(out, "# HELP {name} {h}");
    }
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn fmt_labels(labels: &[(String, String)]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut s = String::from("{");
    for (i, (k, v)) in labels.iter().enumerate() {
        if i > 0 { s.push(','); }
        s.push_str(k);
        s.push_str("=\"");
        for c in v.chars() {
            match c {
                '"' => s.push_str("\\\""),
                '\\' => s.push_str("\\\\"),
                '\n' => s.push_str("\\n"),
                _ => s.push(c),
            }
        }
        s.push('"');
    }
    s.push('}');
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_renders() {
        let r = Registry::new();
        r.set_help("test_total", "a test counter");
        r.inc_counter("test_total", &[("a", "1")], 5);
        r.inc_counter("test_total", &[("a", "1")], 3);
        let out = r.render();
        assert!(out.contains("# HELP test_total a test counter"));
        assert!(out.contains("# TYPE test_total counter"));
        assert!(out.contains("test_total{a=\"1\"} 8"));
    }

    #[test]
    fn gauge_renders() {
        let r = Registry::new();
        r.set_gauge("test_gauge", &[("shard", "0")], 42.5);
        let out = r.render();
        assert!(out.contains("test_gauge{shard=\"0\"} 42.5"));
    }

    #[test]
    fn histogram_renders_buckets() {
        let r = Registry::new();
        let buckets = vec![0.1, 0.5, 1.0, 5.0];
        r.observe_histogram("latency_seconds", &[], 0.3, &buckets);
        r.observe_histogram("latency_seconds", &[], 0.7, &buckets);
        r.observe_histogram("latency_seconds", &[], 2.0, &buckets);
        let out = r.render();
        assert!(out.contains("latency_seconds_bucket{le=\"0.1\"} 0"));
        assert!(out.contains("latency_seconds_bucket{le=\"0.5\"} 1"));
        assert!(out.contains("latency_seconds_bucket{le=\"1\"} 2"));
        assert!(out.contains("latency_seconds_bucket{le=\"+Inf\"} 3"));
        assert!(out.contains("latency_seconds_count 3"));
    }

    #[test]
    fn labels_are_sorted() {
        let r = Registry::new();
        r.inc_counter("c_total", &[("z", "1"), ("a", "2")], 1);
        let out = r.render();
        assert!(out.contains("c_total{a=\"2\",z=\"1\"} 1"));
    }
}