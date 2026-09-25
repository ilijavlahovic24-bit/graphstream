use std::fmt;

#[derive(Debug, Clone)]
pub struct ParseError {
    pub line: u32,
    pub column: u32,
    pub message: String,
    pub found: Option<String>,
    pub expected: Option<String>,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "parse error at {}:{}: {}", self.line, self.column, self.message)?;
        if let Some(found) = &self.found {
            write!(f, " (found `{}`)", found)?;
        }
        if let Some(expected) = &self.expected {
            write!(f, " (expected {})", expected)?;
        }
        Ok(())
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug, Clone)]
pub enum LexError {
    UnexpectedChar { ch: char, line: u32, column: u32 },
    UnterminatedString { line: u32, column: u32 },
    InvalidNumber { text: String, line: u32, column: u32 },
}

impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LexError::UnexpectedChar { ch, line, column } =>
                write!(f, "lex error at {}:{}: unexpected character `{}`", line, column, ch),
            LexError::UnterminatedString { line, column } =>
                write!(f, "lex error at {}:{}: unterminated string literal", line, column),
            LexError::InvalidNumber { text, line, column } =>
                write!(f, "lex error at {}:{}: invalid number `{}`", line, column, text),
        }
    }
}

impl std::error::Error for LexError {}

impl From<LexError> for ParseError {
    fn from(e: LexError) -> Self {
        match e {
            LexError::UnexpectedChar { ch, line, column } =>
                ParseError { line, column, message: format!("unexpected character `{}`", ch), found: Some(ch.to_string()), expected: None },
            LexError::UnterminatedString { line, column } =>
                ParseError { line, column, message: "unterminated string literal".into(), found: None, expected: None },
            LexError::InvalidNumber { text, line, column } =>
                ParseError { line, column, message: format!("invalid number `{}`", text), found: Some(text), expected: None },
        }
    }
}