use crate::lang::scan::Scanner;
use crate::lang::scan::Token;
use crate::lang::program::Program;
use crate::lang::program::Rule;
use crate::lang::program::Term;

pub fn parse(scanner: &mut Scanner) -> Result<Program, String> {
    let mut facts = vec![];
    let mut rules = vec![];

    loop {
        match try_parse_definition(scanner)? {
            Some(Rule { head, body }) if body.is_empty() => {
                facts.push(head);
                continue
            }
            Some(rule) => {
                rules.push(rule);
                continue
            }
            None => break Ok(Program { facts, rules }),
        }
    }
}

fn try_parse_definition(scanner: &mut Scanner) -> Result<Option<Rule>, String> {
    try_parse_functor(scanner).and_then(|option| {
        option.map_or_else(
            || Ok(None),
            |head| {
                parse_rem_definition(scanner, head)
                    .map(|rule| Some(rule))
            },
        )
    })
}

fn parse_rem_definition(scanner: &mut Scanner, head: Term) -> Result<Rule, String> {
    scanner.next_token().and_then(|token| match token {
        Token::Period => {
            Ok(Rule { head, body: vec![] })
        }
        Token::If => {
            let body = parse_body(scanner)?;

            parse_symbol(scanner, &Token::Period)?;

            Ok(Rule { head, body })
        }
        Token::Eof => {
            Err(String::from(
                "Error during parsing: unexpected end of input",
            ))
        }
        _ => {
            Err(format!(
                "Error during parsing: unexpected token {:?}",
                token,
            ))
        }
    })
}

fn parse_body(scanner: &mut Scanner) -> Result<Vec<Term>, String> {
    let mut functors = vec![];

    let functor = parse_functor(scanner)?;
    functors.push(functor);

    loop {
        match scanner.next_token()? {
            Token::Comma => {
                let functor = parse_functor(scanner)?;
                functors.push(functor);
                continue
            },
            token => {
                scanner.put_back(token);
                break Ok(functors)
            }
        }
    }
}

fn try_parse_functor(scanner: &mut Scanner) -> Result<Option<Term>, String> {
    try_parse_ident(scanner).and_then(|option| {
        if let Some(ident) = option {
            parse_rem_functor(scanner, ident)
                .map(|v| Some(v))
        } else {
            Ok(None)
        }
    })
}

fn parse_functor(scanner: &mut Scanner) -> Result<Term, String> {
    parse_ident(scanner).and_then(|ident| {
        parse_rem_functor(scanner, ident)
    })
}

fn parse_rem_functor(scanner: &mut Scanner, ident: String) -> Result<Term, String> {
    scanner.next_token().and_then(|token| match token {
        Token::OpenParen => {
            let term_list = parse_term_list(scanner)?;

            parse_symbol(scanner, &Token::CloseParen)?;

            Ok(Term::Functor(ident, term_list))
        }
        token => {
            scanner.put_back(token);
            Ok(Term::Functor(ident, vec![]))
        }
    })
}

fn parse_term_list(scanner: &mut Scanner) -> Result<Vec<Term>, String> {
    try_parse_term(scanner).and_then(|option| {
        option.map_or_else(
            || Ok(vec![]),
            |first_term| {
                let mut term_list = vec![first_term];

                loop {
                    match scanner.next_token()? {
                        Token::Comma => {
                            let term = parse_term(scanner)?;
                            term_list.push(term);
                            continue
                        },
                        token => {
                            scanner.put_back(token);
                            break Ok(term_list)
                        }
                    }
                }
            },
        )
    })
}

fn try_parse_term(scanner: &mut Scanner) -> Result<Option<Term>, String> {
    match scanner.next_token()? {
        Token::Ident(x) => Ok(Some(Term::Ident(x))),
        Token::Var(x) => Ok(Some(Term::Var(x))),
        Token::Int(x) => Ok(Some(Term::Int(x))),
        token => {
            scanner.put_back(token);
            Ok(None)
        }
    }
}

fn parse_term(scanner: &mut Scanner) -> Result<Term, String> {
    match scanner.next_token()? {
        Token::Ident(x) => Ok(Term::Ident(x)),
        Token::Var(x) => Ok(Term::Var(x)),
        Token::Int(x) => Ok(Term::Int(x)),
        Token::String(x) => Ok(Term::String(x)),
        Token::Eof => Err(String::from(
            "Error during parsing term: unexpected end of input",
        )),
        token => Err(format!(
            "Error during parsing: expected a term, found: {:?}",
            token,
        )),
    }
}

fn try_parse_ident(scanner: &mut Scanner) -> Result<Option<String>, String> {
    match scanner.next_token()? {
        Token::Ident(ident) => Ok(Some(ident)),
        Token::Eof => Ok(None),
        token => Err(format!(
            "Error during parsing: expected an identifier found: {:?}",
            token,
        )),
    }
}

fn parse_ident(scanner: &mut Scanner) -> Result<String, String> {
    match scanner.next_token()? {
        Token::Ident(ident) => Ok(ident),
        Token::Eof => Err(String::from(
            "Error during parsing: unexpected end of input",
        )),
        token => Err(format!(
            "Error during parsing: expected an identifier found: {:?}",
            token,
        )),
    }
}

fn parse_symbol(scanner: &mut Scanner, symbol: &Token) -> Result<(), String> {
    match scanner.next_token()? {
        token if token == *symbol => Ok(()),
        Token::Eof => Err(String::from(
            "Error during parsing: unexpected end of input",
        )),
        token => Err(format!(
            "Error during parsing: expected symbol {:?} found {:?}",
            symbol,
            token,
        )),
    }
}
