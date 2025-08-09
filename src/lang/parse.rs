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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Literal {
    pub head: FunctorTerm,
    pub body: Vec<FunctorTerm>,
}

impl Literal {
    fn from_head(head: FunctorTerm) -> Self {
        Literal { head, body: vec![] }
    }

    pub fn is_rule(&self) -> bool {
           self.body.len() > 0
        || self.head.functor_arity() == 0
    }

    pub fn is_factlike_rule(&self) -> bool {
           self.head.functor_arity() == 0
        && self.body.len() == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Term {
    Atom(String),
    Str(String),
    Var(String),
    AggVar(String, String),
    Int(i32),
    /// Grammar does not allow functor terms (at the moment). But this
    /// is still useful for packaging functor terms into literals since
    /// unification works on terms.
    Functor(String, Vec<Term>, bool),
}

impl Term {
    pub fn functor_spec(&self) -> String {
        if let Term::Functor(name, args, _) = self {
            format!("{}/{}", name, args.len())
        } else {
            unreachable!()
        }
    }

    pub fn functor_name(&self) -> &String {
        if let Term::Functor(name, _, _) = self {
            name
        } else {
            unreachable!()
        }
    }

    pub fn functor_arity(&self) -> u8 {
        if let Term::Functor(_, args, _) = self {
            args.len().try_into().unwrap()
        } else {
            unreachable!()
        }
    }

    pub fn functor_args(&self) -> &Vec<Term> {
        if let Term::Functor(_, args, _) = self {
            args
        } else {
            unreachable!()
        }
    }

    pub fn functor_negated(&self) -> bool {
        if let Term::Functor(_, _, negated) = self {
            *negated
        } else {
            unreachable!()
        }
    }

    pub fn functor_aggregated(&self) -> bool {
        self
        .functor_args()
        .iter()
        .any(|arg| match arg {
            Term::AggVar(_, _) => true,
            _ => false,
        })
    }

    pub fn functor_agg_vars(&self) -> Vec<(&str, &str)> {
        let mut result = vec![];
        for arg in self.functor_args().iter() {
            match arg {
                Term::AggVar(fun, s) => result.push((fun.as_str(), s.as_str())),
                _ => continue,
            }
        }
        result
    }

    pub fn functor_partition_head_vars(&self) -> (Vec<&str>, Vec<&str>) {
        let mut pivots = vec![];
        let mut grouped = vec![];
        for arg in self.functor_args().iter() {
            match arg {
                Term::Var(s) => pivots.push(s.as_str()),
                Term::AggVar(_, s) => {
                    if !grouped.contains(&s.as_str()) {
                        grouped.push(s.as_str());
                    }
                },
                _ => continue
            }
        }
        (pivots, grouped)
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
/// <body>          ::= [ "not" ] <literal> { "," [ "not" ] <literal> }
/// <literal>       ::= | <atom> [ "(" [ <terms> ] ")" ]
///                     | <str> [ "(" [ <terms> ] ")" ]
/// <terms>         ::= <term> { "," <term> }
/// <term>          ::= | <atom> [ "<" <var> ">" ]
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
                declarations.push(parse_literal(parser, false)?);
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
    let head = parse_literal(parser, false)?;

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
                    "unexpected token '{}' when parsing statement", token,
                )).into()),
            }
        },
        token => Err(RecallError::ParseError(format!(
            "unexpected token '{}' when parsing statement", token,
        )).into()),
    }
}

fn parse_body<'a>(parser: &'a mut Parser) -> Result<Vec<FunctorTerm>, anyhow::Error> {
    let mut negated = false;

    if let Token::Not = parser.scanner.peek_token()? {
        negated = true;
        parser.scanner.next_token()?;
    }

    let mut result = vec![parse_literal(parser, negated)?];

    while let Token::Comma = parser.scanner.peek_token()? {
        expect_token(parser, Token::Comma)?;

        let mut negated = false;

        if let Token::Not = parser.scanner.peek_token()? {
            negated = true;
            parser.scanner.next_token()?;
        }


        result.push(parse_literal(parser, negated)?);
    }

    Ok(result)
}

fn parse_literal<'a>(parser: &'a mut Parser, negated: bool) -> Result<FunctorTerm, anyhow::Error> {
    let name = match parse_term(parser)? {
        Term::Atom(s) | Term::Str(s) => s,
        term => return Err(RecallError::ParseError(format!(
            "head position of literal needs to be an atom or str term, got {}", term,
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

    Ok(Term::Functor(name, args, negated))
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
        Token::Atom(s) => {
            if let Token::LT = parser.scanner.peek_token()? {
                expect_token(parser, Token::LT)?;

                let token = parser.scanner.next_token()?;

                if let Token::Var(s2) = token {
                    expect_token(parser, Token::GT)?;
                    Ok(Term::AggVar(s, s2))
                } else {
                    Err(RecallError::ParseError(format!(
                        "expected variable term but got '{}'", token,
                    )).into())
                }
            } else {
                Ok(Term::Atom(s))
            }
        },
        Token::Str(s) => Ok(Term::Str(s)),
        Token::Var(s) => Ok(Term::Var(s)),
        Token::Integer(n) => Ok(Term::Int(n)),
        Token::Plus => {
            let token = parser.scanner.next_token()?;

            if let Token::Integer(n) = token {
                Ok(Term::Int(n))
            } else {
                Err(RecallError::ParseError(format!(
                    "expected integer term but got '{}'", token,
                )).into())
            }
        },
        Token::Minus => {
            let token = parser.scanner.next_token()?;

            if let Token::Integer(n) = token {
                Ok(Term::Int(-n))
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
    fn it_parses_valid_program() -> Result<(), anyhow::Error> {
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
            person(alice).
            person(bob).
            num_people(count<X>) :- person(X).
        ";
        let scanner = Scanner::new(program);
        let mut parser = Parser::new(scanner);
        parse_program(&mut parser)?;
        Ok(())
    }
}
