use chrono::{Duration, NaiveDate, NaiveDateTime};
use std::collections::HashMap;
use temporal_graph::{NodeId, TemporalGraph};
use serde_json::json;

pub fn t(secs: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
        .and_hms_opt(0, 0, 0).unwrap()
        + Duration::seconds(secs)
}

pub fn t_ns(ns: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
        .and_hms_opt(0, 0, 0).unwrap()
        + Duration::nanoseconds(ns)
}

// ---------- 7.1 Cybersecurity ----------

pub struct CyberData {
    pub graph: TemporalGraph,
    pub planted_paths: Vec<(NodeId, NodeId, NodeId)>,
}

pub fn build_cybersecurity() -> CyberData {
    let mut g = TemporalGraph::new(256);

    // 1000 Host čvorova. Poslednji u lancu ima critical=true.
    let mut hosts: Vec<NodeId> = Vec::with_capacity(1000);
    for i in 0..1000 {
        let critical = i % 100 == 0 && i > 0;   // svaki 100. je critical
        let mut p = HashMap::new();
        p.insert("critical".into(), json!(critical));
        hosts.push(g.add_node("Host", p));
    }

    let mut planted = Vec::new();

    // 3 ugrađena lateral movement paterna: a -> b -> c (c.critical)
    for k in 0..3 {
        let a = hosts[k * 3];
        let b = hosts[k * 3 + 1];
        let c = hosts[k * 3 + 2];

        // Postavi c.critical = true
        if let Some(node) = g.node(c) {
            let mut p = node.properties.clone();
            p.insert("critical".into(), json!(true));
            g.insert_node(temporal_graph::Node::new(c, "Host").with_properties(p));
        }

        let base = k as i64 * 3600;
        // r1: a->b, t=base..base+10min
        g.add_edge(a, b, "Connection", t(base), t(base + 600), HashMap::new()).unwrap();
        // r2: b->c, t=base+5min..base+25min (BEFORE + WITHIN <30min)
        g.add_edge(b, c, "Connection", t(base + 300), t(base + 1500), HashMap::new()).unwrap();
        planted.push((a, b, c));
    }

    // 10 lažnih paterna koji ne zadovoljavaju temporalne uslove:
    // r2 počinje PRE r1 (krši BEFORE)
    for k in 0..10 {
        let a = hosts[100 + k * 3];
        let b = hosts[100 + k * 3 + 1];
        let c = hosts[100 + k * 3 + 2];
        if let Some(node) = g.node(c) {
            let mut p = node.properties.clone();
            p.insert("critical".into(), json!(true));
            g.insert_node(temporal_graph::Node::new(c, "Host").with_properties(p));
        }
        let base = 10_000 + k as i64 * 100;
        // r2 PRE r1
        g.add_edge(a, b, "Connection", t(base + 500), t(base + 900), HashMap::new()).unwrap();
        g.add_edge(b, c, "Connection", t(base), t(base + 100), HashMap::new()).unwrap();
    }

    // Popuni do 5000 ivica šumom
    let mut next = 200usize;
    let mut guard = 0usize;
    while g.edge_count() < 5000 && guard < 50_000 {
        let a = hosts[next % hosts.len()];
        let b = hosts[(next * 7 + 3) % hosts.len()];
        if a != b {
            g.add_edge(a, b, "Connection", t(next as i64), t(next as i64 + 60), HashMap::new()).ok();
        }
        next += 1;
    }

    CyberData { graph: g, planted_paths: planted }
}

// ---------- 7.2 Finance ----------

pub struct FinanceData {
    pub graph: TemporalGraph,
    pub planted_pairs: Vec<(NodeId, NodeId)>,
}

pub fn build_finance() -> FinanceData {
    let mut g = TemporalGraph::new(128);

    // 500 Trade čvorova na različitim berzama
    let exchanges = ["NYSE", "NASDAQ", "LSE", "TSE", "HKEX"];
    let mut trades: Vec<NodeId> = Vec::with_capacity(500);
    for i in 0..500 {
        let mut p = HashMap::new();
        p.insert("exchange".into(), json!(exchanges[i % exchanges.len()]));
        trades.push(g.add_node("Trade", p));
    }

    let mut planted = Vec::new();

    // 2 spoofing paterna: >=5 CORRELATED ivica unutar 1 sekunde,
    // correlation > 0.8, različite berze.
    for k in 0..2 {
        let base_ns = k as i64 * 10_000_000_000; // 10s razmak
        for j in 0..8 {
            let a = trades[k * 10 + j];
            let b = trades[k * 10 + j + 1];
            let mut p = HashMap::new();
            p.insert("correlation".into(), json!(0.9));
            g.add_edge(a, b, "CORRELATED",
                       t_ns(base_ns + (j as i64) * 100_000_000),     // 100ms razmak
                       t_ns(base_ns + (j as i64) * 100_000_000 + 50_000_000),
                       p).unwrap();
            planted.push((a, b));
        }
    }

    // Šum: nasumične nekorelirane ivice
    let mut next = 100usize;
    while g.edge_count() < 3000 && next + 2 < trades.len() {
        let a = trades[next % trades.len()];
        let b = trades[(next * 13 + 5) % trades.len()];
        if a != b {
            let mut p = HashMap::new();
            p.insert("correlation".into(), json!(0.3));   // ispod 0.8
            g.add_edge(a, b, "CORRELATED",
                       t_ns(next as i64 * 1_000_000_000),
                       t_ns(next as i64 * 1_000_000_000 + 100_000_000),
                       p).ok();
        }
        next += 1;
    }

    FinanceData { graph: g, planted_pairs: planted }
}

// ---------- 7.3 Physics ----------

pub struct PhysicsData {
    pub graph: TemporalGraph,
    pub chains: Vec<(NodeId, NodeId, f64, f64)>,
}

pub fn build_physics() -> PhysicsData {
    let mut g = TemporalGraph::new(64);

    // 3 različita tipa decay lanaca: alpha, beta, gamma
    // Svaki ima N parent->child ivica sa istim timestampom (simultano
    // na ns rezoluciji, pa WITHIN < 1.5e-12 SECONDS prolazi kao 0 < 1.5e-12).
    let mut chains = Vec::new();

    for chain_type in 0..3 {
        let base_ns = chain_type as i64 * 1_000_000;   // 1ms razmak između lanaca
        let parent_energy = 100.0 + chain_type as f64 * 50.0;
        let child_energy  = 40.0 + chain_type as f64 * 20.0;

        let mut pp = HashMap::new();
        pp.insert("energy".into(), json!(parent_energy));
        let parent = g.add_node("Particle", pp);

        let mut cp = HashMap::new();
        cp.insert("energy".into(), json!(child_energy));
        let child = g.add_node("Particle", cp);

        let mut ep = HashMap::new();
        ep.insert("energy".into(), json!(child_energy));
        // Isti ns timestamp za parent i child — diff = 0
        g.add_edge(parent, child, "DECAYS_TO",
                   t_ns(base_ns), t_ns(base_ns + 1000),
                   ep).unwrap();

        chains.push((parent, child, parent_energy, child_energy));
    }

    // Lažni lanac: parent i child 100ns razmaknuti (WITHIN pada)
    let mut pp = HashMap::new();
    pp.insert("energy".into(), json!(999.0));
    let parent = g.add_node("Particle", pp);
    let mut cp = HashMap::new();
    cp.insert("energy".into(), json!(1.0));
    let child = g.add_node("Particle", cp);
    let mut ep = HashMap::new();
    ep.insert("energy".into(), json!(1.0));
    g.add_edge(parent, child, "DECAYS_TO",
               t_ns(0), t_ns(100_000), ep).unwrap();

    PhysicsData { graph: g, chains }
}