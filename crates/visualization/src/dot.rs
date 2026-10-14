use std::fmt::Write;

use crate::snapshot::Snapshot;

/// Render a snapshot as Graphviz DOT.
///
/// Use `dot -Tpng` or `dot -Tsvg` to turn the output into an image:
///
/// ```text
/// $ graphstream viz dot --at 2024-01-01T00:00:05 > g.dot
/// $ dot -Tpng g.dot -o g.png
/// ```
pub fn render(snapshot: &Snapshot) -> String {
    let mut out = String::new();
    writeln!(&mut out, "digraph GraphStream {{").unwrap();
    writeln!(
        &mut out,
        "  graph [label=\"t = {}\\n({} nodes, {} edges active)\", labelloc=t, fontsize=18];",
        snapshot.time_display,
        snapshot.nodes.len(),
        snapshot.edges.len()
    )
        .unwrap();
    writeln!(
        &mut out,
        "  node [shape=circle, style=filled, fillcolor=\"#e0f0ff\", fontsize=11];"
    )
        .unwrap();
    writeln!(&mut out, "  edge [fontsize=9, color=\"#555555\"];").unwrap();

    for n in &snapshot.nodes {
        let _ = writeln!(
            &mut out,
            "  n{} [label=\"{}\\n#{}\"];",
            n.id, n.label, n.id
        );
    }

    for e in &snapshot.edges {
        let _ = writeln!(
            &mut out,
            "  n{} -> n{} [label=\"{}\"];",
            e.source, e.target, e.label
        );
    }

    writeln!(&mut out, "}}").unwrap();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{Snapshot, SnapshotEdge, SnapshotNode};
    use std::collections::HashMap;

    fn sample() -> Snapshot {
        Snapshot {
            time_ms: 0,
            time_display: "1970-01-01 00:00:00.000".into(),
            nodes: vec![
                SnapshotNode { id: 0, label: "Host".into(), properties: HashMap::new() },
                SnapshotNode { id: 1, label: "Host".into(), properties: HashMap::new() },
            ],
            edges: vec![SnapshotEdge {
                id: 0,
                source: 0,
                target: 1,
                label: "Connection".into(),
                start_ms: 0,
                end_ms: 100,
                properties: HashMap::new(),
            }],
            total_nodes: 2,
            total_edges: 1,
            shards: None,
        }
    }

    #[test]
    fn renders_digraph_and_nodes() {
        let out = render(&sample());
        assert!(out.starts_with("digraph GraphStream"));
        assert!(out.contains("n0 [label=\"Host"));
        assert!(out.contains("n0 -> n1"));
        assert!(out.ends_with("}\n"));
    }
}