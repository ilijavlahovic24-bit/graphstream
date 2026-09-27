mod common;

use std::sync::Arc;
use engine::ingestion;
use query::QueryEngine;

const CYBER:   &str = include_str!("../tql/cybersecurity.tql");
const FINANCE: &str = include_str!("../tql/finance.tql");
const PHYSICS: &str = include_str!("../tql/physics.tql");

#[test]
fn cyber_lateral_movement() {
    let data = common::build_cybersecurity();
    assert!(data.graph.node_count() >= 1000, "need >=1000 hosts");
    assert!(data.graph.edge_count() >= 5000, "need >=5000 edges");

    let engine = QueryEngine::new(Arc::new(data.graph));
    let res = engine.query(CYBER).expect("cyber query should run");

    // Svaki planted path mora biti u rezultatu.
    // Rezultat ima kolone [a, b, c], 3 reda minimum (3 planted).
    assert!(res.rows.len() >= 3, "expected >=3 lateral paths, got {}", res.rows.len());

    // Nijedan red ne sme da ima c bez critical=true
    for row in &res.rows {
        if let query::ResultCell::Node { properties, .. } = &row.cells[2] {
            assert_eq!(properties.get("critical").and_then(|v| v.as_bool()), Some(true));
        }
    }
}

#[test]
fn finance_coordinated_trading() {
    let data = common::build_finance();
    assert!(data.graph.node_count() >= 500, "need >=500 trades");

    let engine = QueryEngine::new(Arc::new(data.graph));
    let res = engine.query(FINANCE).expect("finance query should run");

    // Sa WINDOW 1s + HAVING COUNT(*) >= 5, očekujemo bar jedan red
    // iz svakog spoofing paterna (2 planted).
    assert!(!res.rows.is_empty(), "expected coordinated-trading matches");
}

#[test]
fn physics_decay_chain() {
    let data = common::build_physics();
    let chains = data.chains.clone();

    let engine = QueryEngine::new(Arc::new(data.graph));
    let res = engine.query(PHYSICS).expect("physics query should run");

    // Očekujemo bar 3 reda (3 prava decay lanca) — lažni ne prolazi WITHIN.
    assert!(res.rows.len() >= 3,
            "expected >=3 decay chains, got {}", res.rows.len());

    // Proveravamo energy conservation: SUM(child.energy) mora biti poznata vrednost.
    let total: f64 = res.rows.iter()
        .filter_map(|r| match &r.cells[1] {
            query::ResultCell::Value(v) => v.as_f64(),
            _ => None,
        })
        .sum();

    let expected: f64 = chains.iter().map(|(_, _, _, ce)| ce).sum();
    assert!((total - expected).abs() < 1e-6,
            "energy conservation failed: got {total}, expected {expected}");
}