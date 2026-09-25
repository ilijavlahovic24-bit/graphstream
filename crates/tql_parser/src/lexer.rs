use crate::error::LexError;
use crate::token::{Token, TokenKind};

pub struct Lexer<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    line: u32,
    column: u32,
}
impl<'a> Lexer<'a> {
    pub fn new(src: &'a str) -> Self {
        Lexer { chars: src.chars().peekable(), line: 1, column: 1 }
    }
    pub fn tokenize(mut self) -> Result<Vec<Token>, LexError> {
        let mut out = Vec::new();
        loop {
            let tok = self.next_token()?;
            let is_eof = matches!(tok.kind, TokenKind::Eof);
            out.push(tok);
            if is_eof { break; }
        }
        Ok(out)
    }

    fn peek(&mut self) -> Option<&char> { self.chars.peek() }

    fn bump(&mut self) -> Option<char> {
        let c = self.chars.next();
        if let Some(ch) = c {
            if ch == '\n' { self.line += 1; self.column = 1; }
            else { self.column += 1; }
        }
        c
    }

    fn skip_ws(&mut self) {
        while let Some(&c) = self.peek() {
            if c.is_whitespace() { self.bump(); } else { break; }
        }
    }

    fn mk(&self, kind: TokenKind, line: u32, col: u32) -> Token {
        Token { kind, line, column: col }
    }

    fn next_token(&mut self) -> Result<Token, LexError> {
        self.skip_ws();
        let (line, col) = (self.line, self.column);
        let Some(&c) = self.peek() else {
            return Ok(self.mk(TokenKind::Eof, line, col));
        };

        match c {
            '(' => { self.bump(); Ok(self.mk(TokenKind::LParen, line, col)) }
            ')' => { self.bump(); Ok(self.mk(TokenKind::RParen, line, col)) }
            '[' => { self.bump(); Ok(self.mk(TokenKind::LBracket, line, col)) }
            ']' => { self.bump(); Ok(self.mk(TokenKind::RBracket, line, col)) }
            ',' => { self.bump(); Ok(self.mk(TokenKind::Comma, line, col)) }
            '.' => { self.bump(); Ok(self.mk(TokenKind::Dot, line, col)) }
            ':' => { self.bump(); Ok(self.mk(TokenKind::Colon, line, col)) }
            '*' => { self.bump(); Ok(self.mk(TokenKind::Star, line, col)) }
            '=' => { self.bump(); Ok(self.mk(TokenKind::Eq, line, col)) }
            '!' => {
                self.bump();
                if self.peek() == Some(&'=') { self.bump(); Ok(self.mk(TokenKind::NotEq, line, col)) }
                else { Err(LexError::UnexpectedChar { ch: '!', line, column: col }) }
            }
            '<' => {
                self.bump();
                match self.peek() {
                    Some(&'=') => { self.bump(); Ok(self.mk(TokenKind::Le, line, col)) }
                    Some(&'-') => { self.bump(); Ok(self.mk(TokenKind::ArrowLeft, line, col)) }
                    _          => Ok(self.mk(TokenKind::Lt, line, col)),
                }
            }
            '>' => {
                self.bump();
                if self.peek() == Some(&'=') { self.bump(); Ok(self.mk(TokenKind::Ge, line, col)) }
                else { Ok(self.mk(TokenKind::Gt, line, col)) }
            }
            '-' => {
                self.bump();
                if self.peek() == Some(&'>') { self.bump(); Ok(self.mk(TokenKind::ArrowRight, line, col)) }
                else { Ok(self.mk(TokenKind::Minus, line, col)) }
            }
            '"' | '\'' => self.read_string(c, line, col),
            _ if c.is_ascii_digit() => self.read_number(line, col),
            _ if c.is_ascii_alphabetic() || c == '_' => self.read_word(line, col),
            _ => Err(LexError::UnexpectedChar { ch: c, line, column: col }),
        }
    }
    fn read_string(&mut self, quote: char, line: u32, col: u32) -> Result<Token, LexError> {
        self.bump(); // opening quote
        let mut s = String::new();
        loop {
            match self.bump() {
                None => return Err(LexError::UnterminatedString { line, column: col }),
                Some(ch) if ch == quote => break,
                Some(ch) => s.push(ch),
            }
        }
        Ok(self.mk(TokenKind::Str(s), line, col))
    }

    fn read_number(&mut self, line: u32, col: u32) -> Result<Token, LexError> {
        let mut text = String::new();
        while let Some(&c) = self.peek() {
            if c.is_ascii_digit() { text.push(c); self.bump(); } else { break; }
        }

        let mut is_float = false;
        if self.peek() == Some(&'.') {
            is_float = true;
            text.push('.'); self.bump();
            while let Some(&c) = self.peek() {
                if c.is_ascii_digit() { text.push(c); self.bump(); } else { break; }
            }
        }

        if matches!(self.peek(), Some(&'e') | Some(&'E')) {
            is_float = true;
            text.push('e'); self.bump();
            if matches!(self.peek(), Some(&'+') | Some(&'-')) {
                text.push(self.bump().unwrap());
            }
            while let Some(&c) = self.peek() {
                if c.is_ascii_digit() { text.push(c); self.bump(); } else { break; }
            }
        }

        if is_float {
            text.parse::<f64>()
                .map(|v| self.mk(TokenKind::Float(v), line, col))
                .map_err(|_| LexError::InvalidNumber { text, line, column: col })
        } else {
            text.parse::<i64>()
                .map(|v| self.mk(TokenKind::Int(v), line, col))
                .map_err(|_| LexError::InvalidNumber { text, line, column: col })
        }
    }

    fn read_word(&mut self, line: u32, col: u32) -> Result<Token, LexError> {
        let mut text = String::new();
        while let Some(&c) = self.peek() {
            if c.is_ascii_alphanumeric() || c == '_' { text.push(c); self.bump(); } else { break; }
        }
        let upper = text.to_ascii_uppercase();
        let kind = match upper.as_str() {
            "MATCH" => TokenKind::Match,
            "WHERE" => TokenKind::Where,
            "WINDOW" => TokenKind::Window,
            "HAVING" => TokenKind::Having,
            "RETURN" => TokenKind::Return,
            "AND" => TokenKind::And,
            "OR" => TokenKind::Or,
            "AT" => TokenKind::At,
            "BETWEEN" => TokenKind::Between,
            "DURING" => TokenKind::During,
            "BEFORE" => TokenKind::Before,
            "AFTER" => TokenKind::After,
            "WITHIN" => TokenKind::Within,
            "DIFF" => TokenKind::Diff,
            "EVOLVE" => TokenKind::Evolve,
            "SLIDING" => TokenKind::Sliding,
            "TUMBLING" => TokenKind::Tumbling,
            "COUNT" => TokenKind::Count,
            "SUM" => TokenKind::Sum,
            "AVG" => TokenKind::Avg,
            "MIN" => TokenKind::Min,
            "MAX" => TokenKind::Max,
            "AS" => TokenKind::As,
            "TRUE" => TokenKind::True,
            "FALSE" => TokenKind::False,
            "NULL" => TokenKind::Null,
            "MILLISECONDS" => TokenKind::Milliseconds,
            "SECONDS" => TokenKind::Seconds,
            "MINUTES" => TokenKind::Minutes,
            "HOURS" => TokenKind::Hours,
            "DAYS" => TokenKind::Days,
            _ => TokenKind::Ident(text),
        };
        Ok(self.mk(kind, line, col))
    }

}