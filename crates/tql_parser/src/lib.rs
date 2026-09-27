mod tql_parser;
mod token;
mod lexer;
mod error;
pub mod ast;

use tql_parser::Parser;
pub use error::ParseError;
use ast::*;