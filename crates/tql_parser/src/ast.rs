#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub match_clause: MatchClause,
    pub where_clause: Option<WhereClause>,
    pub window_clause: Option<WindowClause>,
    pub having_clause: Option<HavingClause>,
    pub return_clause: ReturnClause,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchClause {
    pub patterns: Vec<PathPattern>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PathPattern {
    pub nodes: Vec<NodePattern>,
    pub edges: Vec<EdgePattern>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub binding: String,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDirection { Right, Left, Undirected }

#[derive(Debug, Clone, PartialEq)]
pub struct EdgePattern {
    pub binding: String,
    pub label: Option<String>,
    pub direction: EdgeDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    At(AtCondition),
    Between(BetweenCondition),
    During(DuringCondition),
    Ordering(OrderingCondition),
    Within(WithinCondition),
    Diff(DiffCondition),
    Property(PropertyCondition),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AtCondition {
    pub subject: TimeExpr,
    pub instant: TimeExpr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BetweenCondition {
    pub subject: TimeExpr,
    pub t1: TimeExpr,
    pub t2: TimeExpr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DuringCondition {
    pub subject: TimeExpr,
    pub t1: TimeExpr,
    pub t2: TimeExpr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderingCondition {
    pub left: TimeExpr,
    pub order: Order,
    pub right: TimeExpr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order { Before, After }

#[derive(Debug, Clone, PartialEq)]
pub struct WithinCondition {
    pub t1: TimeExpr,
    pub t2: TimeExpr,
    pub op: ComparisonOp,
    pub duration: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiffCondition {
    pub property: PropertyRef,
    pub t1: TimeExpr,
    pub t2: TimeExpr,
    pub op: ComparisonOp,
    pub value: Value,
}



#[derive(Debug, Clone, PartialEq)]
pub enum PropertyCondition {
    Value { left: PropertyRef, op: ComparisonOp, right: Value },
    Property { left: PropertyRef, op: ComparisonOp, right: PropertyRef },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp { Eq, NotEq, Lt, Le, Gt, Ge }

#[derive(Debug, Clone, PartialEq)]
pub enum TimeExpr {
    Property(PropertyRef),
    Timestamp(i64),
    Variable(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyRef {
    pub binding: String,
    pub property: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Duration {
    pub value: f64,
    pub unit: TimeUnit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit { Milliseconds, Seconds, Minutes, Hours, Days }

#[derive(Debug, Clone, PartialEq)]
pub struct WindowClause {
    pub duration: Duration,
    pub kind: WindowKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowKind { Sliding, Tumbling }

#[derive(Debug, Clone, PartialEq)]
pub struct HavingClause {
    pub aggregate: AggregateExpr,
    pub op: ComparisonOp,
    pub threshold: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateExpr {
    Count(CountArg),
    Sum(PropertyRef),
    Avg(PropertyRef),
    Min(PropertyRef),
    Max(PropertyRef),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CountArg {
    Star,
    Property(PropertyRef),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    pub items: Vec<ReturnItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReturnItem {
    Binding(String),
    Property(PropertyRef),
    Aggregate { expr: AggregateExpr, alias: Option<String> },
}