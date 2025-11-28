use crate::errors::RecallError;
use crate::lang::Atom;
use crate::lang::Term;
use crate::lang::scan::Scanner;
use crate::lang::scan::Token;

pub type Program = Vec<Statement>;

#[derive(Debug)]
pub enum Statement {
    Assertion(AssertionKind),
    Retraction(AssertionKind),
    Query(Atom),
}

#[derive(Debug)]
pub enum AssertionKind {
    Fact { head: Atom },
    RuleClause { head: Atom, body: Vec<Atom> },
}

fn is_zero_arity_rule(term: &Term) -> bool {
    if let Term::Atom(_, args, _) = term {
        args.len() == 0
    } else {
        unreachable!()
    }
}

impl Term {
    pub fn atom_parts(self) -> (String, Vec<Term>, bool) {
        if let Term::Atom(name, args, negated) = self {
            (name, args, negated)
        } else {
            unreachable!()
        }
    }

    pub fn atom_spec(&self) -> String {
        if let Term::Atom(name, args, _) = self {
            format!("{}/{}", name, args.len())
        } else {
            unreachable!()
        }
    }

    pub fn atom_name(&self) -> &String {
        if let Term::Atom(name, _, _) = self {
            name
        } else {
            unreachable!()
        }
    }

    pub fn atom_arity(&self) -> u8 {
        if let Term::Atom(_, args, _) = self {
            args.len().try_into().unwrap()
        } else {
            unreachable!()
        }
    }

    pub fn atom_args(&self) -> &Vec<Term> {
        if let Term::Atom(_, args, _) = self {
            args
        } else {
            unreachable!()
        }
    }

    pub fn atom_negated(&self) -> bool {
        if let Term::Atom(_, _, negated) = self {
            *negated
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
/// <program>       ::= { <statement> }
/// <statement>     ::= | <atom> "?"
///                     | <atom> [ ":-" <body> ] "."
///                     | <atom> [ ":-" <body> ] "!"
/// <body>          ::= [ "not" ] <atom> { "," [ "not" ] <atom> }
/// <atom>          ::= | <sym> [ "(" [ <terms> ] ")" ]
///                     | <str> [ "(" [ <terms> ] ")" ]
/// <terms>         ::= <term> { "," <term> }
/// <term>          ::= | <sym>
///                     | <str>
///                     | [ "+" | "-" ] <int>
///                     | <var>

pub fn parse_program<'a>(parser: &'a mut Parser) -> Result<Program, anyhow::Error> {
    let mut statements = vec![];
    Ok(loop {
        match parser.scanner.peek_token()? {
            Token::End => {
                break statements
            },
            _ => {
                statements.push(parse_statement(parser)?);
                continue;
            },
        }
    })
}

fn parse_statement<'a>(parser: &'a mut Parser) -> Result<Statement, anyhow::Error> {
    let head = parse_atom(parser, false)?;

    match parser.scanner.next_token()? {
        Token::Query => Ok(Statement::Query(head)),
        Token::Assertion => {
            Ok(Statement::Assertion(
                if is_zero_arity_rule(&head) {
                    AssertionKind::RuleClause { head, body: vec![] }
                } else {
                    AssertionKind::Fact { head }
                }
            ))
        },
        Token::Retraction => {
            Ok(Statement::Retraction(
                if is_zero_arity_rule(&head) {
                    AssertionKind::RuleClause { head, body: vec![] }
                } else {
                    AssertionKind::Fact { head }
                }
            ))
        },
        Token::If => {
            let body = parse_body(parser)?;

            match parser.scanner.peek_token()? {
                Token::Assertion => {
                    expect_token(parser, Token::Assertion)?;
                    Ok(Statement::Assertion(AssertionKind::RuleClause { head, body }))
                }
                Token::Retraction => {
                    expect_token(parser, Token::Retraction)?;
                    Ok(Statement::Retraction(AssertionKind::RuleClause { head, body }))
                }
                token => Err(RecallError::ParseError(format!(
                    "unexpected token '{}' when parsing statement",
                    token,
                ))
                .into()),
            }
        }
        token => Err(RecallError::ParseError(format!(
            "unexpected token '{}' when parsing statement",
            token,
        ))
        .into()),
    }
}

fn parse_body<'a>(parser: &'a mut Parser) -> Result<Vec<Atom>, anyhow::Error> {
    let mut negated = false;

    if let Token::Not = parser.scanner.peek_token()? {
        negated = true;
        parser.scanner.next_token()?;
    }

    let mut result = vec![parse_atom(parser, negated)?];

    while let Token::Comma = parser.scanner.peek_token()? {
        expect_token(parser, Token::Comma)?;

        let mut negated = false;

        if let Token::Not = parser.scanner.peek_token()? {
            negated = true;
            parser.scanner.next_token()?;
        }

        result.push(parse_atom(parser, negated)?);
    }

    Ok(result)
}

fn parse_atom<'a>(parser: &'a mut Parser, negated: bool) -> Result<Atom, anyhow::Error> {
    let name = match parse_term(parser)? {
        Term::Sym(s) | Term::Str(s) => s,
        term => {
            return Err(RecallError::ParseError(format!(
                "head position of literal needs to be an symbol or string, got {}",
                term,
            ))
            .into())
        }
    };
    let args = if let Token::LP = parser.scanner.peek_token()? {
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

    Ok(Term::Atom(name, args, negated))
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
        /* <sym> */
        Token::Atom(s) => Ok(Term::Sym(s)),
        /* <str> */
        Token::Str(s) => Ok(Term::Str(s)),
        /* <int> */
        Token::Integer(n) => Ok(Term::Int(n)),
        /* + <int> */
        Token::Plus => {
            let token = parser.scanner.next_token()?;

            if let Token::Integer(n) = token {
                Ok(Term::Int(n))
            } else {
                Err(
                    RecallError::ParseError(format!("expected integer term but got '{}'", token,))
                        .into(),
                )
            }
        },
        /* - <int> */
        Token::Minus => {
            let token = parser.scanner.next_token()?;

            if let Token::Integer(n) = token {
                Ok(Term::Int(-n))
            } else {
                Err(
                    RecallError::ParseError(format!("expected integer term but got '{}'", token,))
                        .into(),
                )
            }
        },
        /* <var> */
        Token::Var(s) => Ok(Term::Var(s)),
        token => Err(RecallError::ParseError(format!("expected term but got '{}'", token,)).into()),
    }
}

fn expect_token<'a>(parser: &'a mut Parser, expected: Token) -> Result<(), anyhow::Error> {
    let actual = parser.scanner.next_token()?;
    if actual == expected {
        Ok(())
    } else {
        Err(
            RecallError::ParseError(format!("expected '{}' but got '{}'", expected, actual,))
                .into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_valid_program() -> Result<(), anyhow::Error> {
        let program = "
            # example program
            true.
            succeed :- true.
            fail :- fruit(oats).
            fruit(pear).
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
        ";
        let scanner = Scanner::new(program);
        let mut parser = Parser::new(scanner);
        parse_program(&mut parser)?;
        Ok(())
    }
}
