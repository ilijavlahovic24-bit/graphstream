use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Duration, NaiveDate, NaiveDateTime};
use serde_json::Value;

use temporal_graph::{EdgeId, NodeId, TemporalEdge, TemporalGraph};
use tql_parser::ast::*;

use crate::binding::{Binding, BoundValue, IntervalKey};
use crate::error::TqlError;
use crate::result::{QueryResult, ResultCell, Row};

pub struct QueryEngine {
    graph: Arc<TemporalGraph>,
    epoch: NaiveDateTime,
}

impl QueryEngine {
    pub fn new(graph: Arc<TemporalGraph>) -> Self {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        Self { graph, epoch }
    }

    pub fn graph(&self) -> &TemporalGraph { &self.graph }

    // ---- entry point ----------------------------------------------------

    pub fn query(&self, tql: &str) -> Result<QueryResult, TqlError> {
        let ast = tql_parser::parse(tql)?;
        self.execute(&ast)
    }

    pub fn execute(&self, q: &Query) -> Result<QueryResult, TqlError> {
        let mut bindings = self.eval_match(&q.match_clause)?;

        if let Some(w) = &q.where_clause {
            bindings = self.eval_where(bindings, w)?;
        }
        if let Some(w) = &q.window_clause {
            bindings = self.eval_window(bindings, w)?;
        }
        if let Some(h) = &q.having_clause {
            if q.window_clause.is_none() {
                return Err(TqlError::AggregateWithoutWindow);
            }
            bindings = self.eval_having(bindings, h)?;
        }
        self.eval_return(bindings, &q.return_clause)
    }

    // ---- MATCH ----------------------------------------------------------

    fn eval_match(&self, m: &MatchClause) -> Result<Vec<Binding>, TqlError> {
        let mut all = vec![Binding::new()];
        for path in &m.patterns {
            let path_bindings = self.eval_path(path)?;
            let mut combined = Vec::new();
            for a in &all {
                for b in &path_bindings {
                    let mut merged = a.clone();
                    let mut ok = true;
                    for (k, v) in &b.vars {
                        if let Some(existing) = merged.vars.get(k) {
                            if existing != v { ok = false; break; }
                        }
                        merged.vars.insert(k.clone(), *v);
                    }
                    if ok {
                        if merged.time_key.is_none() { merged.time_key = b.time_key; }
                        combined.push(merged);
                    }
                }
            }
            all = combined;
            if all.is_empty() { break; }
        }
        Ok(all)
    }

    fn eval_path(&self, path: &PathPattern) -> Result<Vec<Binding>, TqlError> {
        let first = &path.nodes[0];
        let mut partials: Vec<Binding> = Vec::new();
        for n in self.graph.nodes() {
            if !label_matches(Some(&n.label), &first.label) { continue; }
            let mut b = Binding::new();
            b.vars.insert(first.binding.clone(), BoundValue::Node(n.id));
            partials.push(b);
        }

        for (i, ep) in path.edges.iter().enumerate() {
            let prev_binding = &path.nodes[i].binding;
            let next_node = &path.nodes[i + 1];

            let mut next_partials = Vec::new();
            for p in &partials {
                let Some(BoundValue::Node(prev_id)) = p.get(prev_binding) else { continue };
                for edge in self.graph.edges() {
                    if !label_matches(Some(&edge.label), &ep.label) { continue; }
                    let Some(next_id) = edge_target(edge, prev_id, ep.direction) else { continue };
                    let Some(next_n) = self.graph.node(next_id) else { continue };
                    if !label_matches(Some(&next_n.label), &next_node.label) { continue; }

                    for iv in edge.intervals.iter() {
                        let ikey = IntervalKey { start: iv.start_time, end: iv.end_time };
                        let mut b = p.clone();
                        b.vars.insert(ep.binding.clone(), BoundValue::Edge(edge.id, ikey));
                        b.vars.insert(next_node.binding.clone(), BoundValue::Node(next_id));
                        if b.time_key.is_none() { b.time_key = Some(iv.start_time); }
                        next_partials.push(b);
                    }
                }
            }
            partials = next_partials;
            if partials.is_empty() { break; }
        }
        Ok(partials)
    }

    // ---- WHERE ----------------------------------------------------------

    fn eval_where(&self, bindings: Vec<Binding>, w: &WhereClause) -> Result<Vec<Binding>, TqlError> {
        let mut out = Vec::with_capacity(bindings.len());
        'outer: for b in bindings {
            for c in &w.conditions {
                if !self.eval_condition(&b, c)? { continue 'outer; }
            }
            out.push(b);
        }
        Ok(out)
    }

    fn eval_condition(&self, b: &Binding, c: &Condition) -> Result<bool, TqlError> {
        match c {
            Condition::At(x) => self.eval_at(b, x),
            Condition::Between(x) => self.eval_between(b, x),
            Condition::During(x) => self.eval_during(b, x),
            Condition::Ordering(x) => self.eval_ordering(b, x),
            Condition::Within(x) => self.eval_within(b, x),
            Condition::Diff(x) => self.eval_diff(b, x),
            Condition::Property(x) => self.eval_property(b, x),
        }
    }

    fn eval_at(&self, b: &Binding, ac: &AtCondition) -> Result<bool, TqlError> {
        let subject = self.resolve_time(b, &ac.subject)?;
        let instant = self.resolve_time(b, &ac.instant)?.start();
        Ok(subject.overlaps(instant, instant))
    }

    fn eval_between(&self, b: &Binding, bc: &BetweenCondition) -> Result<bool, TqlError> {
        let subject = self.resolve_time(b, &bc.subject)?;
        let t1 = self.resolve_time(b, &bc.t1)?.start();
        let t2 = self.resolve_time(b, &bc.t2)?.start();
        Ok(subject.overlaps(t1, t2))
    }

    fn eval_during(&self, b: &Binding, dc: &DuringCondition) -> Result<bool, TqlError> {
        let subject = self.resolve_time(b, &dc.subject)?;
        let t1 = self.resolve_time(b, &dc.t1)?.start();
        let t2 = self.resolve_time(b, &dc.t2)?.start();
        Ok(subject.overlaps(t1, t2))
    }

    fn eval_ordering(&self, b: &Binding, oc: &OrderingCondition) -> Result<bool, TqlError> {
        let l = self.resolve_time(b, &oc.left)?.start();
        let r = self.resolve_time(b, &oc.right)?.start();
        Ok(match oc.order {
            Order::Before => l < r,
            Order::After  => l > r,
        })
    }

    fn eval_within(&self, b: &Binding, wc: &WithinCondition) -> Result<bool, TqlError> {
        let a = self.resolve_time(b, &wc.t1)?.start();
        let z = self.resolve_time(b, &wc.t2)?.start();
        let diff_secs = ((z - a).num_nanoseconds().unwrap_or(0).abs() as f64) / 1e9;
        let limit = self.duration_seconds(&wc.duration);
        Ok(cmp_f64(diff_secs, wc.op, limit))
    }

    fn eval_diff(&self, b: &Binding, dc: &DiffCondition) -> Result<bool, TqlError> {
        let t1 = self.resolve_time(b, &dc.t1)?.start();
        let t2 = self.resolve_time(b, &dc.t2)?.start();
        let v1 = self.property_value_at(b, &dc.property, t1)?;
        let v2 = self.property_value_at(b, &dc.property, t2)?;

        match (v1.as_f64(), v2.as_f64()) {
            (Some(a), Some(z)) => {
                let rhs = value_as_f64(&ast_value_to_json(&dc.value))?;   // ← konverzija
                Ok(cmp_f64(z - a, dc.op, rhs))
            }
            _ => {
                let equal = v1 == v2;
                Ok(match dc.op {
                    ComparisonOp::Eq => equal,
                    ComparisonOp::NotEq => !equal,
                    _ => return Err(TqlError::TypeMismatch {
                        context: "DIFF".into(),
                        detail: "non-numeric values only support = and !=".into(),
                    }),
                })
            }
        }
    }

    fn eval_property(&self, b: &Binding, pc: &PropertyCondition) -> Result<bool, TqlError> {
        match pc {
            PropertyCondition::Value { left, op, right } => {
                let lv = self.lookup_property(b, left)?;
                let rv = ast_value_to_json(right);       // ← konverzija
                Ok(compare_value(&lv, *op, &rv))         // ← sada &Value (serde_json)
            }
            PropertyCondition::Property { left, op, right } => {
                let lv = self.lookup_property(b, left)?;
                let rv = self.lookup_property(b, right)?;
                Ok(compare_value(&lv, *op, &rv))
            }
        }
    }

    // ---- time & property resolution -------------------------------------

    fn resolve_time(&self, b: &Binding, e: &TimeExpr) -> Result<ResolvedTime, TqlError> {
        match e {
            TimeExpr::Timestamp(ms) => Ok(ResolvedTime::Instant(self.epoch + Duration::milliseconds(*ms))),
            TimeExpr::Variable(name) => match b.get(name) {
                Some(BoundValue::Edge(_, iv)) => Ok(ResolvedTime::Interval(iv)),
                Some(BoundValue::Node(_)) => Err(TqlError::TypeMismatch {
                    context: "time expression".into(),
                    detail: format!("`{name}` is a node; nodes have no implicit time in v1"),
                }),
                None => Err(TqlError::UnknownVariable(name.clone())),
            },
            TimeExpr::Property(pref) => match b.get(&pref.binding) {
                Some(BoundValue::Edge(edge_id, iv)) if pref.property == "time" => {
                    Ok(ResolvedTime::Interval(iv))
                }
                Some(BoundValue::Edge(edge_id, _iv)) => {
                    let edge = self.graph.edge(edge_id).ok_or(TqlError::UnknownEdge(edge_id))?;
                    let v = edge.intervals.iter()
                        .flat_map(|x| x.properties.get(&pref.property))
                        .next()
                        .cloned()
                        .ok_or_else(|| TqlError::UnknownVariable(pref.property.clone()))?;
                    self.value_to_time(&v)
                }
                Some(BoundValue::Node(node_id)) => {
                    let node = self.graph.node(node_id).ok_or(TqlError::UnknownNode(node_id))?;
                    let v = node.properties.get(&pref.property)
                        .ok_or_else(|| TqlError::UnknownVariable(pref.property.clone()))?;
                    self.value_to_time(v)
                }
                None => Err(TqlError::UnknownVariable(pref.binding.clone())),
            },
        }
    }

    fn value_to_time(&self, v: &Value) -> Result<ResolvedTime, TqlError> {
        if let Some(n) = v.as_i64() {
            Ok(ResolvedTime::Instant(self.epoch + Duration::milliseconds(n)))
        } else {
            Err(TqlError::TypeMismatch {
                context: "time value".into(),
                detail: format!("expected integer timestamp, got {v}"),
            })
        }
    }

    fn lookup_property(&self, b: &Binding, pref: &PropertyRef) -> Result<Value, TqlError> {
        match b.get(&pref.binding) {
            Some(BoundValue::Node(id)) => {
                let n = self.graph.node(id).ok_or(TqlError::UnknownNode(id))?;
                Ok(n.properties.get(&pref.property).cloned().unwrap_or(Value::Null))
            }
            Some(BoundValue::Edge(id, iv)) => {
                let e = self.graph.edge(id).ok_or(TqlError::UnknownEdge(id))?;
                let v = e.intervals.iter()
                    .find(|x| x.start_time == iv.start && x.end_time == iv.end)
                    .and_then(|x| x.properties.get(&pref.property))
                    .cloned()
                    .unwrap_or(Value::Null);
                Ok(v)
            }
            None => Err(TqlError::UnknownVariable(pref.binding.clone())),
        }
    }

    fn property_value_at(&self, b: &Binding, pref: &PropertyRef, _t: NaiveDateTime) -> Result<Value, TqlError> {
        // v1: DIFF is evaluated on the already-bound interval (identity), not
        // re-queried at each t. Documented limitation.
        self.lookup_property(b, pref)
    }

    fn duration_seconds(&self, d: &tql_parser::ast::Duration) -> f64 {
        let scale = match d.unit {
            TimeUnit::Milliseconds => 1e-3,
            TimeUnit::Seconds      => 1.0,
            TimeUnit::Minutes      => 60.0,
            TimeUnit::Hours        => 3600.0,
            TimeUnit::Days         => 86400.0,
        };
        d.value * scale
    }

    // ---- WINDOW ---------------------------------------------------------

    fn eval_window(&self, mut bindings: Vec<Binding>, w: &WindowClause) -> Result<Vec<Binding>, TqlError> {
        let secs = self.duration_seconds(&w.duration);
        if secs <= 0.0 {
            return Err(TqlError::TypeMismatch {
                context: "WINDOW".into(),
                detail: "duration must be positive".into(),
            });
        }
        // v1: SLIDING step == duration (same as TUMBLING); step parameter is v2.
        let _ = w.kind;
        for b in &mut bindings {
            if let Some(t) = b.time_key {
                let secs_since_epoch = (t - self.epoch).num_nanoseconds().unwrap_or(0) as f64 / 1e9;
                b.window_bucket = Some((secs_since_epoch / secs).floor() as i64);
            }
        }
        Ok(bindings)
    }

    // ---- HAVING ---------------------------------------------------------

    fn eval_having(&self, bindings: Vec<Binding>, h: &HavingClause) -> Result<Vec<Binding>, TqlError> {
        let mut groups: HashMap<i64, Vec<usize>> = HashMap::new();
        for (i, b) in bindings.iter().enumerate() {
            if let Some(k) = b.window_bucket {
                groups.entry(k).or_default().push(i);
            }
        }
        let mut keep = vec![false; bindings.len()];
        for idxs in groups.values() {
            let rows: Vec<&Binding> = idxs.iter().map(|&i| &bindings[i]).collect();
            let v = self.aggregate(&h.aggregate, &rows)?;
            if cmp_f64(v, h.op, h.threshold) {
                for &i in idxs { keep[i] = true; }
            }
        }
        Ok(bindings.into_iter().enumerate()
            .filter(|(i, _)| keep[*i])
            .map(|(_, b)| b)
            .collect())
    }

    // ---- RETURN ---------------------------------------------------------

    fn eval_return(&self, bindings: Vec<Binding>, r: &ReturnClause) -> Result<QueryResult, TqlError> {
        let columns: Vec<String> = r.items.iter().map(|it| match it {
            ReturnItem::Binding(n) => n.clone(),
            ReturnItem::Property(p) => format!("{}.{}", p.binding, p.property),
            ReturnItem::Aggregate { expr, alias } => alias.clone().unwrap_or_else(|| agg_name(expr)),
        }).collect();

        let has_agg = r.items.iter().any(|i| matches!(i, ReturnItem::Aggregate { .. }));

        let rows = if has_agg {
            let rows_ref: Vec<&Binding> = bindings.iter().collect();
            let cells = r.items.iter().map(|it| match it {
                ReturnItem::Aggregate { expr, .. } => {
                    Ok(ResultCell::Value(Value::from(self.aggregate(expr, &rows_ref)?)))
                }
                _ => Err(TqlError::TypeMismatch {
                    context: "RETURN".into(),
                    detail: "non-aggregate column mixed with aggregate".into(),
                }),
            }).collect::<Result<Vec<_>, _>>()?;
            vec![Row { cells }]
        } else {
            bindings.iter().map(|b| {
                let cells = r.items.iter()
                    .map(|it| self.project(b, it))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Row { cells })
            }).collect::<Result<Vec<_>, TqlError>>()?
        };

        Ok(QueryResult { columns, rows })
    }

    fn project(&self, b: &Binding, item: &ReturnItem) -> Result<ResultCell, TqlError> {
        match item {
            ReturnItem::Binding(name) => match b.get(name) {
                Some(BoundValue::Node(id)) => {
                    let n = self.graph.node(id).ok_or(TqlError::UnknownNode(id))?;
                    Ok(ResultCell::Node { id, label: n.label.clone(), properties: n.properties.clone() })
                }
                Some(BoundValue::Edge(edge_id, iv)) => {
                    let e = self.graph.edge(edge_id).ok_or(TqlError::UnknownEdge(edge_id))?;
                    let props = e.intervals.iter()
                        .find(|x| x.start_time == iv.start && x.end_time == iv.end)
                        .map(|x| x.properties.clone())
                        .unwrap_or_default();
                    Ok(ResultCell::Edge {
                        id: edge_id, source: e.source, target: e.target, label: e.label.clone(),
                        start: iv.start, end: iv.end, properties: props,
                    })
                }
                None => Err(TqlError::UnknownVariable(name.clone())),
            },
            ReturnItem::Property(pref) => Ok(ResultCell::Value(self.lookup_property(b, pref)?)),
            ReturnItem::Aggregate { .. } => Err(TqlError::TypeMismatch {
                context: "RETURN".into(),
                detail: "aggregate in non-aggregate context".into(),
            }),
        }
    }

    // ---- aggregates -----------------------------------------------------

    fn aggregate(&self, expr: &AggregateExpr, rows: &[&Binding]) -> Result<f64, TqlError> {
        match expr {
            AggregateExpr::Count(CountArg::Star) => Ok(rows.len() as f64),
            AggregateExpr::Count(CountArg::Property(p)) => {
                let mut c = 0.0;
                for r in rows { if !self.lookup_property(r, p)?.is_null() { c += 1.0; } }
                Ok(c)
            }
            AggregateExpr::Sum(p) => {
                let mut s = 0.0;
                for r in rows { s += value_as_f64(&self.lookup_property(r, p)?)?; }
                Ok(s)
            }
            AggregateExpr::Avg(p) => {
                if rows.is_empty() { return Ok(0.0); }
                let mut s = 0.0;
                for r in rows { s += value_as_f64(&self.lookup_property(r, p)?)?; }
                Ok(s / rows.len() as f64)
            }
            AggregateExpr::Min(p) => {
                let mut m = f64::INFINITY;
                for r in rows { m = m.min(value_as_f64(&self.lookup_property(r, p)?)?); }
                Ok(m)
            }
            AggregateExpr::Max(p) => {
                let mut m = f64::NEG_INFINITY;
                for r in rows { m = m.max(value_as_f64(&self.lookup_property(r, p)?)?); }
                Ok(m)
            }
        }
    }
}

// ---- helpers ----------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum ResolvedTime {
    Instant(NaiveDateTime),
    Interval(IntervalKey),
}

impl ResolvedTime {
    fn start(&self) -> NaiveDateTime {
        match self { ResolvedTime::Instant(t) => *t, ResolvedTime::Interval(iv) => iv.start }
    }
    fn end(&self) -> NaiveDateTime {
        match self { ResolvedTime::Instant(t) => *t, ResolvedTime::Interval(iv) => iv.end }
    }
    fn overlaps(&self, t1: NaiveDateTime, t2: NaiveDateTime) -> bool {
        self.start() <= t2 && self.end() >= t1
    }
}

fn label_matches(actual: Option<&String>, required: &Option<String>) -> bool {
    match required {
        None => true,
        Some(req) => actual.map(|a| a == req).unwrap_or(false),
    }
}

fn edge_target(e: &TemporalEdge, from: NodeId, dir: EdgeDirection) -> Option<NodeId> {
    match dir {
        EdgeDirection::Right => if e.source == from { Some(e.target) } else { None },
        EdgeDirection::Left  => if e.target == from { Some(e.source) } else { None },
        EdgeDirection::Undirected => {
            if e.source == from { Some(e.target) }
            else if e.target == from { Some(e.source) }
            else { None }
        }
    }
}

fn cmp_f64(a: f64, op: ComparisonOp, b: f64) -> bool {
    match op {
        ComparisonOp::Eq => a == b,
        ComparisonOp::NotEq => a != b,
        ComparisonOp::Lt => a < b,
        ComparisonOp::Le => a <= b,
        ComparisonOp::Gt => a > b,
        ComparisonOp::Ge => a >= b,
    }
}

fn value_as_f64(v: &Value) -> Result<f64, TqlError> {
    if let Some(n) = v.as_f64() { Ok(n) }
    else if let Some(n) = v.as_i64() { Ok(n as f64) }
    else { Err(TqlError::TypeMismatch { context: "numeric value".into(), detail: format!("got {v}") }) }
}

fn compare_value(left: &Value, op: ComparisonOp, right: &Value) -> bool {
    // Numeric compare when both are numbers; else equality/inequality.
    if let (Some(a), Some(b)) = (left.as_f64().or_else(|| left.as_i64().map(|n| n as f64)),
                                 right.as_f64().or_else(|| right.as_i64().map(|n| n as f64))) {
        return cmp_f64(a, op, b);
    }
    match op {
        ComparisonOp::Eq => left == right,
        ComparisonOp::NotEq => left != right,
        _ => false,
    }
}

fn agg_name(e: &AggregateExpr) -> String {
    match e {
        AggregateExpr::Count(_) => "count".into(),
        AggregateExpr::Sum(_)   => "sum".into(),
        AggregateExpr::Avg(_)   => "avg".into(),
        AggregateExpr::Min(_)   => "min".into(),
        AggregateExpr::Max(_)   => "max".into(),
    }
}

fn ast_value_to_json(v: &tql_parser::ast::Value) -> serde_json::Value {
    use tql_parser::ast::Value as AstValue;
    match v {
        AstValue::Int(n)   => serde_json::Value::from(*n),
        AstValue::Float(x) => serde_json::Value::from(*x),
        AstValue::String(s) => serde_json::Value::from(s.clone()),
        AstValue::Bool(b)  => serde_json::Value::from(*b),
        AstValue::Null     => serde_json::Value::Null,
    }
}