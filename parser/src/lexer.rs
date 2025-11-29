
use crate::token::TokenType;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Token<'a> {
    pub value: &'a [u8],
    pub token_type: Option<TokenType>, // None means Token is whitespaces or comments
}

pub struct Lexer<'a> {
    pub(crate) offset: usize,
    pub(crate) input: &'a [u8],
}

impl<'a> Lexer<'a> {
    #[inline(always)]
    pub fn new(input: &'a [u8]) -> Self {
        Lexer { input, offset: 0 }
    }
}
