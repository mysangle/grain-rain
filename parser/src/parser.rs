
use crate::lexer::{Lexer, Token};

pub struct Parser<'a> {
    lexer: Lexer<'a>,
    current_token: Token<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            lexer: Lexer::new(input),
            current_token: Token {
                value: &input[..0],
                token_type: None,
            },
        }
    }
}
