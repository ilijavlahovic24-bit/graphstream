use crate::ast::*;
use crate::error::ParseError;
use crate::lexer::Lexer;
use crate::token::{Token, TokenKind};

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn parse(src: &str) -> Result<Query, ParseError> {
        let tokens = Lexer::new(src).tokenize()?;
        let mut p = Parser { tokens, pos: 0 };
        let q = p.parse_query()?;
        p.expect(&TokenKind::Eof)?;
        Ok(q)
    }

    // ---- token helpers --------------------------------------------------

    fn peek(&self) -> &Token { &self.tokens[self.pos] }
    fn peek_kind(&self) -> &TokenKind { &self.tokens[self.pos].kind }

    fn advance(&mut self) -> Token {
        let t = self.tokens[self.pos].clone();
        if !matches!(t.kind, TokenKind::Eof) { self.pos += 1; }
        t
    }

    fn check(&self, k: &TokenKind) -> bool { self.peek_kind() == k }

    fn eat(&mut self, k: &TokenKind) -> bool {
        if self.check(k) { self.advance(); true } else { false }
    }

    fn expect(&mut self, k: &TokenKind) -> Result<Token, ParseError> {
        if self.check(k) { Ok(self.advance()) }
        else {
            let t = self.peek();
            Err(ParseError {
                line: t.line,
                column: t.column,
                message: format!("expected {}", k.describe()),
                found: Some(t.kind.describe()),
                expected: Some(k.describe()),
            })
        }
    }

    fn error<T>(&self, msg: impl Into<String>) -> Result<T, ParseError> {
        let t = self.peek();
        Err(ParseError {
            line: t.line,
            column: t.column,
            message: msg.into(),
            found: Some(t.kind.describe()),
            expected: None,
        })
    }

    fn parse_identifier(&mut self) -> Result<String, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::Ident(s) => { self.advance(); Ok(s) }
            // The AT production requires the literal `t`. Allow it here.
            TokenKind::At => { self.advance(); Ok("AT".into()) }
            other => Err(ParseError {
                line: self.peek().line,
                column: self.peek().column,
                message: "expected identifier".into(),
                found: Some(other.describe()),
                expected: Some("identifier".into()),
            }),
        }
    }

    // ---- top-level ------------------------------------------------------

    fn parse_query(&mut self) -> Result<Query, ParseError> {
        let match_clause = self.parse_match()?;

        let where_clause = if self.check(&TokenKind::Where) {
            Some(self.parse_where()?)
        } else { None };

        let (window_clause, having_clause) = if self.check(&TokenKind::Window) {
            let w = self.parse_window()?;
            let h = if self.check(&TokenKind::Having) {
                Some(self.parse_having()?)
            } else { None };
            (Some(w), h)
        } else {
            if self.check(&TokenKind::Having) {
                return self.error("HAVING requires a preceding WINDOW clause");
            }
            (None, None)
        };

        let return_clause = self.parse_return()?;

        Ok(Query { match_clause, where_clause, window_clause, having_clause, return_clause })
    }

    // ---- MATCH ----------------------------------------------------------

    fn parse_match(&mut self) -> Result<MatchClause, ParseError> {
        self.expect(&TokenKind::Match)?;
        let mut patterns = vec![self.parse_path_pattern()?];
        while self.eat(&TokenKind::Comma) {
            patterns.push(self.parse_path_pattern()?);
        }
        Ok(MatchClause { patterns })
    }

    fn parse_path_pattern(&mut self) -> Result<PathPattern, ParseError> {
        let mut nodes = vec![self.parse_node_pattern()?];
        let mut edges = Vec::new();
        while self.check(&TokenKind::Minus) || self.check(&TokenKind::ArrowLeft) {
            edges.push(self.parse_edge_pattern()?);
            nodes.push(self.parse_node_pattern()?);
        }
        Ok(PathPattern { nodes, edges })
    }

    fn parse_node_pattern(&mut self) -> Result<NodePattern, ParseError> {
        self.expect(&TokenKind::LParen)?;
        let binding = self.parse_identifier()?;
        let label = if self.eat(&TokenKind::Colon) { Some(self.parse_identifier()?) } else { None };
        self.expect(&TokenKind::RParen)?;
        Ok(NodePattern { binding, label })
    }

    fn parse_edge_pattern(&mut self) -> Result<EdgePattern, ParseError> {
        // Left-directed edges start with `<-`; right/undirected start with `-[`.
        enum Start { Left, RightOrUndirected }
        let start = if self.eat(&TokenKind::ArrowLeft) {
            Start::Left
        } else {
            self.expect(&TokenKind::Minus)?;
            Start::RightOrUndirected
        };

        self.expect(&TokenKind::LBracket)?;
        let binding = self.parse_identifier()?;
        let label = if self.eat(&TokenKind::Colon) { Some(self.parse_identifier()?) } else { None };
        self.expect(&TokenKind::RBracket)?;

        let direction = match start {
            Start::Left => {
                self.expect(&TokenKind::Minus)?;
                EdgeDirection::Left
            }
            Start::RightOrUndirected => {
                if self.eat(&TokenKind::ArrowRight) {
                    EdgeDirection::Right
                } else {
                    self.expect(&TokenKind::Minus)?;
                    EdgeDirection::Undirected
                }
            }
        };
        Ok(EdgePattern { binding, label, direction })
    }

    // ---- WHERE ----------------------------------------------------------

    fn parse_where(&mut self) -> Result<WhereClause, ParseError> {
        self.expect(&TokenKind::Where)?;
        let mut conditions = vec![self.parse_condition()?];
        while self.check(&TokenKind::And) {
            self.advance();
            conditions.push(self.parse_condition()?);
        }
        Ok(WhereClause { conditions })
    }

    fn parse_condition(&mut self) -> Result<Condition, ParseError> {
        // Prefix operators
        if self.check(&TokenKind::Within) { return Ok(Condition::Within(self.parse_within()?)); }
        if self.check(&TokenKind::Diff)   { return Ok(Condition::Diff(self.parse_diff()?)); }

        // Infix: parse the left side first
        let left = self.parse_time_expr()?;

        if self.eat(&TokenKind::Before) {
            let right = self.parse_time_expr()?;
            return Ok(Condition::Ordering(OrderingCondition { left, order: Order::Before, right }));
        }
        if self.eat(&TokenKind::After) {
            let right = self.parse_time_expr()?;
            return Ok(Condition::Ordering(OrderingCondition { left, order: Order::After, right }));
        }
        if self.eat(&TokenKind::Between) {
            let t1 = self.parse_time_expr()?;
            self.expect(&TokenKind::Comma)?;
            let t2 = self.parse_time_expr()?;
            return Ok(Condition::Between(BetweenCondition { subject: left, t1, t2 }));
        }
        if self.eat(&TokenKind::During) {
            self.expect(&TokenKind::LBracket)?;
            let t1 = self.parse_time_expr()?;
            self.expect(&TokenKind::Comma)?;
            let t2 = self.parse_time_expr()?;
            self.expect(&TokenKind::RBracket)?;
            return Ok(Condition::During(DuringCondition { subject: left, t1, t2 }));
        }
        if self.eat(&TokenKind::At) {
            // Grammar requires the literal `t` and `=`.
            let t_ident = self.parse_identifier()?;
            if t_ident != "t" {
                return self.error("expected literal `t` after AT");
            }
            self.expect(&TokenKind::Eq)?;
            let instant = self.parse_time_expr()?;
            return Ok(Condition::At(AtCondition { subject: left, instant }));
        }

        // Otherwise it's a property comparison.
        let TimeExpr::Property(prop) = left else {
            return self.error("expected temporal operator (BEFORE/AFTER/BETWEEN/DURING/AT) after time expression");
        };
        let op = self.parse_comparison_op()?;

        // Right side may be a property reference or a value.
        if matches!(self.peek_kind(), TokenKind::Ident(_)) {
            // Look one token ahead: `ident . ident` → property ref
            let save = self.pos;
            let first = self.parse_identifier()?;
            if self.eat(&TokenKind::Dot) {
                let second = self.parse_identifier()?;
                return Ok(Condition::Property(PropertyCondition::Property {
                    left: prop,
                    op,
                    right: PropertyRef { binding: first, property: second },
                }));
            }
            // Roll back: treat as a bare identifier value? Not supported.
            self.pos = save;
        }
        let right = self.parse_value()?;
        Ok(Condition::Property(PropertyCondition::Value { left: prop, op, right }))
    }

    fn parse_within(&mut self) -> Result<WithinCondition, ParseError> {
        self.expect(&TokenKind::Within)?;
        self.expect(&TokenKind::LParen)?;
        let t1 = self.parse_time_expr()?;
        self.expect(&TokenKind::Comma)?;
        let t2 = self.parse_time_expr()?;
        self.expect(&TokenKind::RParen)?;
        let op = self.parse_comparison_op()?;
        let duration = self.parse_duration()?;
        Ok(WithinCondition { t1, t2, op, duration })
    }

    fn parse_diff(&mut self) -> Result<DiffCondition, ParseError> {
        self.expect(&TokenKind::Diff)?;
        self.expect(&TokenKind::LParen)?;
        let property = self.parse_property_ref()?;
        self.expect(&TokenKind::Comma)?;
        let t1 = self.parse_time_expr()?;
        self.expect(&TokenKind::Comma)?;
        let t2 = self.parse_time_expr()?;
        self.expect(&TokenKind::RParen)?;
        let op = self.parse_comparison_op()?;
        let value = self.parse_value()?;
        Ok(DiffCondition { property, t1, t2, op, value })
    }



    // ---- WINDOW / HAVING ------------------------------------------------

    fn parse_window(&mut self) -> Result<WindowClause, ParseError> {
        self.expect(&TokenKind::Window)?;
        let duration = self.parse_duration()?;
        let kind = if self.eat(&TokenKind::Sliding) { WindowKind::Sliding }
        else if self.eat(&TokenKind::Tumbling) { WindowKind::Tumbling }
        else { WindowKind::Tumbling }; // default per grammar
        Ok(WindowClause { duration, kind })
    }

    fn parse_having(&mut self) -> Result<HavingClause, ParseError> {
        self.expect(&TokenKind::Having)?;
        let aggregate = self.parse_aggregate()?;
        let op = self.parse_comparison_op()?;
        let threshold = match self.peek_kind().clone() {
            TokenKind::Int(n) => { self.advance(); n as f64 }
            TokenKind::Float(x) => { self.advance(); x }
            other => return Err(ParseError {
                line: self.peek().line, column: self.peek().column,
                message: "expected numeric threshold".into(),
                found: Some(other.describe()), expected: Some("number".into()),
            }),
        };
        Ok(HavingClause { aggregate, op, threshold })
    }

    fn parse_aggregate(&mut self) -> Result<AggregateExpr, ParseError> {
        if self.eat(&TokenKind::Count) {
            self.expect(&TokenKind::LParen)?;
            let arg = if self.eat(&TokenKind::Star) { CountArg::Star }
            else { CountArg::Property(self.parse_property_ref()?) };
            self.expect(&TokenKind::RParen)?;
            return Ok(AggregateExpr::Count(arg));
        }
        let ctor: fn(PropertyRef) -> AggregateExpr =
            if self.eat(&TokenKind::Sum) { AggregateExpr::Sum }
            else if self.eat(&TokenKind::Avg) { AggregateExpr::Avg }
            else if self.eat(&TokenKind::Min) { AggregateExpr::Min }
            else if self.eat(&TokenKind::Max) { AggregateExpr::Max }
            else { return self.error("expected aggregate function (COUNT/SUM/AVG/MIN/MAX)"); };
        self.expect(&TokenKind::LParen)?;
        let p = self.parse_property_ref()?;
        self.expect(&TokenKind::RParen)?;
        Ok(ctor(p))
    }

    // ---- RETURN ---------------------------------------------------------

    fn parse_return(&mut self) -> Result<ReturnClause, ParseError> {
        self.expect(&TokenKind::Return)?;
        let mut items = vec![self.parse_return_item()?];
        while self.eat(&TokenKind::Comma) {
            items.push(self.parse_return_item()?);
        }
        Ok(ReturnClause { items })
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem, ParseError> {
        // Aggregate?
        if matches!(self.peek_kind(),
            TokenKind::Count | TokenKind::Sum | TokenKind::Avg | TokenKind::Min | TokenKind::Max)
        {
            let expr = self.parse_aggregate()?;
            let alias = if self.eat(&TokenKind::As) { Some(self.parse_identifier()?) } else { None };
            return Ok(ReturnItem::Aggregate { expr, alias });
        }
        // Ident[.Ident] → binding or property
        let first = self.parse_identifier()?;
        if self.eat(&TokenKind::Dot) {
            let second = self.parse_identifier()?;
            Ok(ReturnItem::Property(PropertyRef { binding: first, property: second }))
        } else {
            Ok(ReturnItem::Binding(first))
        }
    }

    // ---- shared sub-parsers ---------------------------------------------

    fn parse_time_expr(&mut self) -> Result<TimeExpr, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::Int(n) => { self.advance(); Ok(TimeExpr::Timestamp(n)) }
            TokenKind::Ident(_) => {
                let first = self.parse_identifier()?;
                if self.eat(&TokenKind::Dot) {
                    let prop = self.parse_identifier()?;
                    Ok(TimeExpr::Property(PropertyRef { binding: first, property: prop }))
                } else {
                    Ok(TimeExpr::Variable(first))
                }
            }
            other => Err(ParseError {
                line: self.peek().line, column: self.peek().column,
                message: "expected time expression".into(),
                found: Some(other.describe()),
                expected: Some("time expression (property path, variable, or timestamp)".into()),
            }),
        }
    }

    fn parse_property_ref(&mut self) -> Result<PropertyRef, ParseError> {
        let b = self.parse_identifier()?;
        self.expect(&TokenKind::Dot)?;
        let p = self.parse_identifier()?;
        Ok(PropertyRef { binding: b, property: p })
    }

    fn parse_comparison_op(&mut self) -> Result<ComparisonOp, ParseError> {
        let op = match self.peek_kind() {
            TokenKind::Eq    => ComparisonOp::Eq,
            TokenKind::NotEq => ComparisonOp::NotEq,
            TokenKind::Lt    => ComparisonOp::Lt,
            TokenKind::Le    => ComparisonOp::Le,
            TokenKind::Gt    => ComparisonOp::Gt,
            TokenKind::Ge    => ComparisonOp::Ge,
            _ => return self.error("expected comparison operator"),
        };
        self.advance();
        Ok(op)
    }

    fn parse_value(&mut self) -> Result<Value, ParseError> {
        let v = match self.peek_kind().clone() {
            TokenKind::Int(n)   => { self.advance(); Value::Int(n) }
            TokenKind::Float(x) => { self.advance(); Value::Float(x) }
            TokenKind::Str(s)   => { self.advance(); Value::String(s) }
            TokenKind::True     => { self.advance(); Value::Bool(true) }
            TokenKind::False    => { self.advance(); Value::Bool(false) }
            TokenKind::Null     => { self.advance(); Value::Null }
            other => return Err(ParseError {
                line: self.peek().line, column: self.peek().column,
                message: "expected value".into(),
                found: Some(other.describe()),
                expected: Some("number, string, boolean, or null".into()),
            }),
        };
        Ok(v)
    }

    fn parse_duration(&mut self) -> Result<Duration, ParseError> {
        let value = match self.peek_kind().clone() {
            TokenKind::Int(n)   => { self.advance(); n as f64 }
            TokenKind::Float(x) => { self.advance(); x }
            other => return Err(ParseError {
                line: self.peek().line, column: self.peek().column,
                message: "expected duration value".into(),
                found: Some(other.describe()), expected: Some("number".into()),
            }),
        };
        let unit = match self.peek_kind() {
            TokenKind::Milliseconds => TimeUnit::Milliseconds,
            TokenKind::Seconds      => TimeUnit::Seconds,
            TokenKind::Minutes      => TimeUnit::Minutes,
            TokenKind::Hours        => TimeUnit::Hours,
            TokenKind::Days         => TimeUnit::Days,
            _ => return self.error("expected time unit (MILLISECONDS/SECONDS/MINUTES/HOURS/DAYS)"),
        };
        self.advance();
        Ok(Duration { value, unit })
    }
}