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

fn is_ground_term(term: &Term) -> bool {
    match term {
        Term::Sym(_) | Term::Str(_) | Term::Int(_) => true,
        Term::Var(_) => false,
        Term::Atom(_, args, _) => args.iter().all(is_ground_term),
    }
}

fn is_fact_assertion(term: &Term) -> bool {
    if let Term::Atom(_, args, _) = term {
        args.iter().all(is_ground_term)
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
/// <terms>         ::= <term> { "," <term> }
/// <term>          ::= | <str>
///                     | [ "+" | "-" ] <int>
///                     | <var>
///                     | <atom>

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
                if is_fact_assertion(&head) {
                    AssertionKind::Fact { head }
                } else {
                    AssertionKind::RuleClause { head, body: vec![] }
                }
            ))
        },
        Token::Retraction => {
            Ok(Statement::Retraction(
                if is_fact_assertion(&head) {
                    AssertionKind::Fact { head }
                } else {
                    AssertionKind::RuleClause { head, body: vec![] }
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
    let name =
        if let Token::Atom(s) = parser.scanner.next_token()? {
            s
        } else {
            return Err(RecallError::ParseError(format!(
                "head position of atom needs to be a symbol",
            ))
            .into())
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
    match parser.scanner.peek_token()? {
        /* <sym> */
        Token::Atom(_) => {
            let term = parse_atom(parser, false)?;
            if let Term::Atom(name, args, negated) = term {
                if args.is_empty() && !negated {
                    Ok(Term::Sym(name))
                } else {
                    Ok(Term::Atom(name, args, negated))
                }
            } else {
                unreachable!()
            }
        },
        _ => {
            match parser.scanner.next_token()? {
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
        },
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

    #[test]
    fn it_parses_valid_functor_program() -> Result<(), anyhow::Error> {
        let program = "
            # facts
            person(
                alice,
                cons(apple, cons(pear, nil))
            ).

            len(nil, z).
            len(cons(H, T), s(N)) :- len(T, N).
        ";
        let scanner = Scanner::new(program);
        let mut parser = Parser::new(scanner);
        parse_program(&mut parser)?;
        Ok(())
    }

    #[test]
    fn it_treats_non_ground_assertions_as_rules() -> Result<(), anyhow::Error> {
        let program = "
            first(cons(H, T), H).
            person(alice, cons(apple, nil)).
        ";
        let scanner = Scanner::new(program);
        let mut parser = Parser::new(scanner);
        let parsed = parse_program(&mut parser)?;

        assert_eq!(parsed.len(), 2);

        match &parsed[0] {
            Statement::Assertion(AssertionKind::RuleClause { head, body }) => {
                assert_eq!(head.atom_name(), "first");
                assert!(body.is_empty());
            }
            _ => panic!("expected first clause to be a rule"),
        }

        match &parsed[1] {
            Statement::Assertion(AssertionKind::Fact { head }) => {
                assert_eq!(head.atom_name(), "person");
            }
            _ => panic!("expected second clause to be a fact"),
        }

        Ok(())
    }
}
