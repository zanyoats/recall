use std::iter;
use std::iter::Peekable;
use std::str::Chars;
use std::fmt;

use anyhow::Context;

use crate::errors;

#[derive(Debug)]
pub enum Token {
    /* data types */
    Atom(String),
    Str(String),
    Var(String),
    Integer(i32),
    /* punctuation */
    Assertion,
    Retraction,
    Query,
    If,
    Comma,
    Plus,
    Minus,
    LP,
    RP,
    Decl,
    /* keywords */
    Not,
    /* end of file */
    End,
}

impl PartialEq for Token {
    fn eq(&self, other: &Self) -> bool {
        use Token::*;

        match (self, other) {
            // variants with payloads
            (Atom(_), Atom(_))
            | (Str(_), Str(_))
            | (Var(_), Var(_))
            | (Integer(_), Integer(_)) => true,
            // zero payload variants
            (Decl, Decl)
            | (Assertion, Assertion)
            | (Retraction, Retraction)
            | (Query, Query)
            | (Not, Not)
            | (If, If)
            | (Comma, Comma)
            | (Plus, Plus)
            | (Minus, Minus)
            | (LP, LP)
            | (RP, RP)
            | (End, End) => true,
            // every other case
            _ => false,

        }
    }
}

impl Eq for Token {}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Token::*;

        match self {
            /* data types */
            Atom(_)     => write!(f, "atom"),
            Str(_)      => write!(f, "str"),
            Var(_)      => write!(f, "var"),
            Integer(_)  => write!(f, "integer"),
            /* punctuation */
            Decl        => write!(f, "#!"),
            Assertion   => write!(f, "."),
            Retraction  => write!(f, "!"),
            Query       => write!(f, "?"),
            If          => write!(f, ":-"),
            Comma       => write!(f, ","),
            Plus        => write!(f, "+"),
            Minus       => write!(f, "-"),
            LP          => write!(f, "("),
            RP          => write!(f, ")"),
            /* keywords */
            Not         => write!(f, "not"),
            /* end of file */
            End         => write!(f, "<EOF>"),
        }
    }
}

pub struct Scanner<'a> {
    iterable: Peekable<Chars<'a>>,
    buf: Option<Token>,
}

impl<'a> Scanner<'a> {
    pub fn new(input: &'a str) -> Self {
        Scanner { iterable: input.chars().peekable(), buf: None }
    }

    pub fn peek_token(&mut self) -> Result<&Token, anyhow::Error> {
        if self.buf.is_none() {
            self.buf = Some(self.advance()?);
        }

        Ok(self.buf.as_ref().unwrap())
    }

    pub fn next_token(&mut self) -> Result<Token, anyhow::Error> {
        if let Some(token) = self.buf.take() {
            Ok(token)
        } else {
            Ok(self.advance()?)
        }
    }

    fn advance(&mut self) -> Result<Token, anyhow::Error> {
        use Token::*;
        use errors::RecallError::{ScanError};

        while let Some(ch) = self.iterable.next() {
            match ch {
                ch if ch.is_whitespace() => continue,
                /* punctuation */
                '.' => return Ok(Assertion),
                '!' => return Ok(Retraction),
                '?' => return Ok(Query),
                ',' => return Ok(Comma),
                '+' => return Ok(Plus),
                '-' => return Ok(Minus),
                '(' => return Ok(LP),
                ')' => return Ok(RP),
                ':' => {
                    if let Some('-') = self.iterable.peek() {
                        self.iterable.next();
                        return Ok(If)
                    } else {
                        return Err(ScanError(format!(
                            "unrecognized charater '{}' following colon '{}'", ch, ":",
                        )).into())
                    }
                },
                '#' => {
                    if let Some('!') = self.iterable.peek() {
                        self.iterable.next();
                        return Ok(Decl)
                    } else {
                        self.iterable.next();
                        let _ = iter::from_fn(|| self.iterable.next_if(|s| *s != '\n')).collect::<String>();
                        continue;
                    }
                }
                /* start of variables */
                'A'..='Z' | '_' => {
                    let s = iter::once(ch)
                        .chain(iter::from_fn(|| self.iterable.next_if(|s| s.is_ascii_alphanumeric() || *s == '_')))
                        .collect::<String>();
                    return Ok(Var(s))
                }
                /* start of atoms or keywords */
                'a'..='z' => {
                    let s = iter::once(ch)
                        .chain(iter::from_fn(|| self.iterable.next_if(|s| s.is_ascii_alphanumeric() || *s == '_')))
                        .collect::<String>();

                    if s == "not" { /* is this a reserved atom (keyword) */
                        return Ok(Not)
                    } else {
                        return Ok(Atom(s))
                    }
                }
                /* start if number */
                '0'..='9' => {
                    let n_str = iter::once(ch)
                        .chain(iter::from_fn(|| self.iterable.next_if(|s| s.is_ascii_digit())))
                        .collect::<String>();
                    let n = n_str
                        .as_str()
                        .parse::<i32>()
                        .with_context(|| format!("error while parsing {}", n_str))?;
                    return Ok(Integer(n))
                }
                /* start of string */
                '"' => {
                    let s = iter::from_fn(|| {
                        match self.iterable.peek() {
                            Some('"') => None,
                            Some('\\') => {
                                self.iterable.next();
                                match self.iterable.next() {
                                    Some('t') => Some('\t'),
                                    Some('n') => Some('\n'),
                                    Some('r') => Some('\r'),
                                    Some('0') => Some('\0'),
                                    Some(c) => Some(c),
                                    None => None,
                                }
                            },
                            Some(_) => self.iterable.next(),
                            None => None,
                        }
                    })
                    .collect::<String>();

                    if let Some('"') = self.iterable.next() {
                        return Ok(Str(s))
                    } else {
                        return Err(ScanError(format!(
                            "no closing quote found when parsing string",
                        )).into())
                    }
                }
                /* unregonized case */
                _ => return Err(ScanError(format!(
                    "unrecognized character '{}'", ch,
                )).into())
            }
        }
        return Ok(End)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Token::*;

    #[test]
    fn it_scans_valid_tokens() {
        let mut scanner = Scanner::new(r#"foo(a, -42, "bar(\"hello,\t\nworld\").")."#);
        assert!(matches!(scanner.next_token(), Ok(Atom(s)) if s == "foo"));
        assert!(matches!(scanner.next_token(), Ok(LP)));
        assert!(matches!(scanner.next_token(), Ok(Atom(s)) if s == "a"));
        assert!(matches!(scanner.next_token(), Ok(Comma)));
        assert!(matches!(scanner.next_token(), Ok(Minus)));
        assert!(matches!(scanner.next_token(), Ok(Integer(n)) if n == 42));
        assert!(matches!(scanner.next_token(), Ok(Comma)));
        assert!(matches!(scanner.next_token(), Ok(Str(s)) if s == "bar(\"hello,\t\nworld\")."));
        assert!(matches!(scanner.next_token(), Ok(RP)));
        assert!(matches!(scanner.next_token(), Ok(Assertion)));
        assert!(matches!(scanner.next_token(), Ok(End)));
        assert!(matches!(scanner.next_token(), Ok(End)));
    }
}
