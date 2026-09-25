#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    // Keywords
    Match, Where, Window, Having, Return,
    And, Or,
    At, Between, During, Before, After, Within, Diff, Evolve,
    Sliding, Tumbling,
    Count, Sum, Avg, Min, Max, As,
    True, False, Null,
    // Time units
    Milliseconds, Seconds, Minutes, Hours, Days,
    // Identifiers and literals
    Ident(String),
    Int(i64),
    Float(f64),
    Str(String),
    // Punctuation
    LParen, RParen,
    LBracket, RBracket,
    Comma, Dot, Colon,
    // Operators
    Eq, NotEq, Lt, Le, Gt, Ge,
    ArrowRight, ArrowLeft,
    Minus, Star,
    // End of input
    Eof,
}

impl TokenKind {
    /// Human-readable name used in error messages.
    pub fn describe(&self) -> String {
        match self {
            TokenKind::Ident(s) => format!("identifier `{}`", s),
            TokenKind::Int(n)   => format!("integer `{}`", n),
            TokenKind::Float(x) => format!("float `{}`", x),
            TokenKind::Str(s)   => format!("string \"{}\"", s),
            TokenKind::Eof      => "end of input".into(),
            other               => format!("`{}`", other.literal()),
        }
    }

    /// Literal spelling for keywords/operators (for diagnostics).
    pub fn literal(&self) -> &'static str {
        match self {
            TokenKind::Match => "MATCH",
            TokenKind::Where => "WHERE",
            TokenKind::Window => "WINDOW",
            TokenKind::Having => "HAVING",
            TokenKind::Return => "RETURN",
            TokenKind::And => "AND",
            TokenKind::Or => "OR",
            TokenKind::At => "AT",
            TokenKind::Between => "BETWEEN",
            TokenKind::During => "DURING",
            TokenKind::Before => "BEFORE",
            TokenKind::After => "AFTER",
            TokenKind::Within => "WITHIN",
            TokenKind::Diff => "DIFF",
            TokenKind::Evolve => "EVOLVE",
            TokenKind::Sliding => "SLIDING",
            TokenKind::Tumbling => "TUMBLING",
            TokenKind::Count => "COUNT",
            TokenKind::Sum => "SUM",
            TokenKind::Avg => "AVG",
            TokenKind::Min => "MIN",
            TokenKind::Max => "MAX",
            TokenKind::As => "AS",
            TokenKind::True => "true",
            TokenKind::False => "false",
            TokenKind::Null => "null",
            TokenKind::Milliseconds => "MILLISECONDS",
            TokenKind::Seconds => "SECONDS",
            TokenKind::Minutes => "MINUTES",
            TokenKind::Hours => "HOURS",
            TokenKind::Days => "DAYS",
            TokenKind::LParen => "(",
            TokenKind::RParen => ")",
            TokenKind::LBracket => "[",
            TokenKind::RBracket => "]",
            TokenKind::Comma => ",",
            TokenKind::Dot => ".",
            TokenKind::Colon => ":",
            TokenKind::Eq => "=",
            TokenKind::NotEq => "!=",
            TokenKind::Lt => "<",
            TokenKind::Le => "<=",
            TokenKind::Gt => ">",
            TokenKind::Ge => ">=",
            TokenKind::ArrowRight => "->",
            TokenKind::ArrowLeft => "<-",
            TokenKind::Minus => "-",
            TokenKind::Star => "*",
            TokenKind::Ident(_) | TokenKind::Int(_) | TokenKind::Float(_) | TokenKind::Str(_) | TokenKind::Eof => "",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Token {
    pub kind: TokenKind,
    pub line: u32,
    pub column: u32,
}