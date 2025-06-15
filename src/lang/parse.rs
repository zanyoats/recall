use crate::lang::scan::Scanner;
use crate::lang::scan::Token;
use crate::errors::RecallError;

pub type Predicate = Term;
pub type Declaration = Predicate;

#[derive(Debug)]
pub struct Program {
    pub declarations: Vec<Declaration>,
    pub statements: Vec<Statement>,
}

#[derive(Debug)]
pub enum Statement {
    Asserted(Literal),
    Retracted(Literal),
    Query(Predicate),
}

#[derive(Debug, Clone)]
pub struct Literal {
    pub head: Predicate,
    pub body: Vec<Predicate>,
}

impl Literal {
    fn from_head(head: Predicate) -> Self {
        Literal { head, body: vec![] }
    }
}

impl Predicate {
    pub fn new(name: String, args: Vec<Term>) -> Self {
        Term::Functor(name, args)
    }

    pub fn name(term: &Term) -> &String{
        if let Term::Functor(name, _) = term {
            name
        } else {
            panic!("predicate not functor term type")
        }
    }

    pub fn arguments(term: &Term) -> &Vec<Term> {
        if let Term::Functor(_, args) = term {
            args
        } else {
            panic!("predicate not functor term type")
        }
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

pub struct Parser<'a> {
    scanner: Scanner<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(scanner: Scanner<'a>) -> Self {
        Parser { scanner }
    }
}

/// Used to parse rules stored (as text) in db into Literal
pub fn parse_rule<'a>(mut parser: Parser) -> Result<Literal, RecallError> {
    let head = parse_literal(&mut parser)?;
    expect_token(&mut parser, Token::If)?;
    let body = parse_body(&mut parser)?;
    expect_token(&mut parser, Token::Assertion)?;
    Ok(Literal { head, body })
}

/// EBNF Grammar for Datalog
/// <program>       ::= { <declaration> | <statement> }
/// <declaration>   ::= "%!" <literal> "."
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

pub fn parse_program<'a>(parser: &'a mut Parser) -> Result<Program, RecallError> {
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

fn parse_statement<'a>(parser: &'a mut Parser) -> Result<Statement, RecallError> {
    let head = parse_literal(parser)?;

    match parser.scanner.next_token()? {
        Token::Query => Ok(Statement::Query(head)),
        Token::Assertion => Ok(Statement::Asserted(Literal::from_head(head))),
        Token::Retraction => Ok(Statement::Retracted(Literal::from_head(head))),
        Token::If => {
            let body = parse_body(parser)?;

            match parser.scanner.peek_token()? {
                Token::Assertion => {
                    expect_token(parser, Token::Assertion)?;
                    Ok(Statement::Asserted(Literal { head, body }))
                },
                Token::Retraction => {
                    expect_token(parser, Token::Retraction)?;
                    Ok(Statement::Retracted(Literal { head, body }))
                },
                token => Err(RecallError::ParseError(format!(
                    "unpected token '{}' when parsing statement", token,
                ))),
            }
        },
        token => Err(RecallError::ParseError(format!(
            "unpected token '{}' when parsing statement", token,
        ))),
    }
}

fn parse_body<'a>(parser: &'a mut Parser) -> Result<Vec<Predicate>, RecallError> {
    let mut result = vec![parse_literal(parser)?];

    while let Token::Comma = parser.scanner.peek_token()? {
        expect_token(parser, Token::Comma)?;
        result.push(parse_literal(parser)?);
    }

    Ok(result)
}

fn parse_literal<'a>(parser: &'a mut Parser) -> Result<Predicate, RecallError> {
    let name = match parse_term(parser)? {
        Term::Atom(s) | Term::Str(s) => s,
        _ => return Err(RecallError::ParseError(format!(
            "head position of literal needs to be an atom or str term",
        ))),
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

    Ok(Predicate::new(name, args))
}

fn parse_terms<'a>(parser: &'a mut Parser) -> Result<Vec<Term>, RecallError> {
    let mut result = vec![parse_term(parser)?];

    while let Token::Comma = parser.scanner.peek_token()? {
        expect_token(parser, Token::Comma)?;
        result.push(parse_term(parser)?);
    }

    Ok(result)
}

fn parse_term<'a>(parser: &'a mut Parser) -> Result<Term, RecallError> {
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
                )))
            }
        },
        Token::Minus => {
            let token = parser.scanner.next_token()?;

            if let Token::Integer(n) = token {
                Ok(Term::Integer(-n))
            } else {
                Err(RecallError::ParseError(format!(
                    "expected integer term but got '{}'", token,
                )))
            }
        },
        token => Err(RecallError::ParseError(format!(
            "expected term but got '{}'", token,
        ))),
    }
}

fn expect_token<'a>(parser: &'a mut Parser, expected: Token) -> Result<(), RecallError> {
    let actual = parser.scanner.next_token()?;
    if actual == expected {
        Ok(())
    } else {
        Err(RecallError::ParseError(format!(
            "expected '{}' but got '{}'",
            expected, actual,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_valid_program() {
        let program = "
            % example program
            %! link(atom, atom).
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
