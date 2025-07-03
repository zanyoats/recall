use std::collections::HashMap;
use std::collections::HashSet;

use crate::lang::scan::Scanner;
use crate::lang::scan::Token;
use crate::errors::RecallError;

pub type FunctorTerm = Term;
pub type Defs = HashMap<(String, u8), Vec<Literal>>;
pub type Schemas = HashMap<(String, u8), Vec<u8>>;

#[derive(Debug)]
pub struct Program {
    pub declarations: Vec<FunctorTerm>,
    pub statements: Vec<Statement>,
}

#[derive(Debug)]
pub struct TypedProgram {
    pub defs: Defs,
    pub facts: HashSet<(String, u8)>,
    pub schemas: Schemas,
    pub statements: Vec<TypedStatement>,
}

#[derive(Debug)]
pub enum Statement {
    Assertion(Literal),
    Retraction(Literal),
    Query(FunctorTerm),
}

#[derive(Debug)]
pub enum TypedStatement {
    Fact(FunctorTerm),
    Query(FunctorTerm),
    Retraction(Literal),
}

#[derive(Debug, Clone)]
pub struct Literal {
    pub head: FunctorTerm,
    pub body: Vec<FunctorTerm>,
}

impl Literal {
    fn from_head(head: FunctorTerm) -> Self {
        Literal { head, body: vec![] }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Term {
    Atom(String),
    Str(String),
    Var(String),
    Integer(i32),
    /// Grammar does not allow functor terms (at the moment). But this
    /// is still useful for packaging functor terms into literals since
    /// unification works on terms.
    Functor(String, Vec<Term>),
}

impl Term {
    pub fn functor(name: String, args: Vec<Term>) -> Self {
        Term::Functor(name, args)
    }

    pub fn functor_name(&self) -> &String {
        if let Term::Functor(name, _) = self {
            name
        } else {
            unreachable!()
        }
    }

    pub fn functor_arity(&self) -> u8 {
        if let Term::Functor(_, args) = self {
            args.len().try_into().unwrap()
        } else {
            unreachable!()
        }
    }

    pub fn functor_args(&self) -> &Vec<Term> {
        if let Term::Functor(_, args) = self {
            args
        } else {
            unreachable!()
        }
    }
}

pub struct Parser<'a> {
    scanner: Scanner<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(scanner: Scanner<'a>) -> Self {
        Parser { scanner }
    }
}

/// EBNF Grammar for Datalog
/// <program>       ::= { <declaration> | <statement> }
/// <declaration>   ::= "#!" <literal> "."
/// <statement>     ::= | <literal> "?"
///                     | <literal> "."
///                     | <literal> "!"
///                     | <literal> ":-" <body> "."
///                     | <literal> ":-" <body> "!"
/// <body>          ::= <literal> { "," <literal> }
/// <literal>       ::= | <atom> [ "(" [ <terms> ] ")" ]
///                     | <str> [ "(" [ <terms> ] ")" ]
/// <terms>         ::= <term> { "," <term> }
/// <term>          ::= | <atom>
///                     | <str>
///                     | <var>
///                     | [ "+" | "-" ] <integer>

pub fn parse_program<'a>(parser: &'a mut Parser) -> Result<Program, anyhow::Error> {
    let mut declarations = vec![];
    let mut statements = vec![];
    Ok(loop {
        match parser.scanner.peek_token()? {
            Token::End => break Program { declarations, statements },
            Token::Decl => {
                expect_token(parser, Token::Decl)?;
                declarations.push(parse_literal(parser)?);
                expect_token(parser, Token::Assertion)?;
                continue
            },
            _ => {
                statements.push(parse_statement(parser)?);
                continue
            }
        }
    })
}

fn parse_statement<'a>(parser: &'a mut Parser) -> Result<Statement, anyhow::Error> {
    let head = parse_literal(parser)?;

    match parser.scanner.next_token()? {
        Token::Query => Ok(Statement::Query(head)),
        Token::Assertion => Ok(Statement::Assertion(Literal::from_head(head))),
        Token::Retraction => Ok(Statement::Retraction(Literal::from_head(head))),
        Token::If => {
            let body = parse_body(parser)?;

            match parser.scanner.peek_token()? {
                Token::Assertion => {
                    expect_token(parser, Token::Assertion)?;
                    Ok(Statement::Assertion(Literal { head, body }))
                },
                Token::Retraction => {
                    expect_token(parser, Token::Retraction)?;
                    Ok(Statement::Retraction(Literal { head, body }))
                },
                token => Err(RecallError::ParseError(format!(
                    "unpected token '{}' when parsing statement", token,
                )).into()),
            }
        },
        token => Err(RecallError::ParseError(format!(
            "unpected token '{}' when parsing statement", token,
        )).into()),
    }
}

fn parse_body<'a>(parser: &'a mut Parser) -> Result<Vec<FunctorTerm>, anyhow::Error> {
    let mut result = vec![parse_literal(parser)?];

    while let Token::Comma = parser.scanner.peek_token()? {
        expect_token(parser, Token::Comma)?;
        result.push(parse_literal(parser)?);
    }

    Ok(result)
}

fn parse_literal<'a>(parser: &'a mut Parser) -> Result<FunctorTerm, anyhow::Error> {
    let name = match parse_term(parser)? {
        Term::Atom(s) | Term::Str(s) => s,
        _ => return Err(RecallError::ParseError(format!(
            "head position of literal needs to be an atom or str term",
        )).into()),
    };
    let args =
        if let Token::LP = parser.scanner.peek_token()? {
            expect_token(parser, Token::LP)?;

            if let Token::RP = parser.scanner.peek_token()? {
                expect_token(parser, Token::RP)?;
                vec![]
            } else {
                let terms = parse_terms(parser)?;
                expect_token(parser, Token::RP)?;
                terms
            }
        } else {
            vec![]
        };

    Ok(Term::functor(name, args))
}

fn parse_terms<'a>(parser: &'a mut Parser) -> Result<Vec<Term>, anyhow::Error> {
    let mut result = vec![parse_term(parser)?];

    while let Token::Comma = parser.scanner.peek_token()? {
        expect_token(parser, Token::Comma)?;
        result.push(parse_term(parser)?);
    }

    Ok(result)
}

fn parse_term<'a>(parser: &'a mut Parser) -> Result<Term, anyhow::Error> {
    match parser.scanner.next_token()? {
        Token::Atom(s) => Ok(Term::Atom(s)),
        Token::Str(s) => Ok(Term::Str(s)),
        Token::Var(s) => Ok(Term::Var(s)),
        Token::Integer(n) => Ok(Term::Integer(n)),
        Token::Plus => {
            let token = parser.scanner.next_token()?;

            if let Token::Integer(n) = token {
                Ok(Term::Integer(n))
            } else {
                Err(RecallError::ParseError(format!(
                    "expected integer term but got '{}'", token,
                )).into())
            }
        },
        Token::Minus => {
            let token = parser.scanner.next_token()?;

            if let Token::Integer(n) = token {
                Ok(Term::Integer(-n))
            } else {
                Err(RecallError::ParseError(format!(
                    "expected integer term but got '{}'", token,
                )).into())
            }
        },
        token => Err(RecallError::ParseError(format!(
            "expected term but got '{}'", token,
        )).into()),
    }
}

fn expect_token<'a>(parser: &'a mut Parser, expected: Token) -> Result<(), anyhow::Error> {
    let actual = parser.scanner.next_token()?;
    if actual == expected {
        Ok(())
    } else {
        Err(RecallError::ParseError(format!(
            "expected '{}' but got '{}'",
            expected, actual,
        )).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_valid_program() {
        let program = "
            # example program
            #! link(atom, atom).
            link(a, b).
            link(b, c).
            link(c, c).
            link(c, d).
            link(e, f).
            same(X) :- link(X, X).
            link(X, Y)?
            same(X)?
            same(a) :- link(a,a)!
            same(X)?
        ";
        let scanner = Scanner::new(program);
        let mut parser = Parser::new(scanner);
        let result = parse_program(&mut parser);
        assert!(result.is_ok());
        // if result.is_ok() {
        //     println!("{:?}", result.unwrap());
        // } else {
        //     println!("{:?}", result.unwrap_err());
        // }
    }
}
