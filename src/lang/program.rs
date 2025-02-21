// EBNF Grammer for simple datalog
//
// Program     ::= { Defs }
// Defs        ::= Rule | Fact
// Fact        ::= Functor '.'
// Rule        ::= Functor ':-' Body '.'
// Body        ::= Functor { ',' Functor }
// Functor     ::= Identifier [ '(' TermList ')' ]
// TermList    ::= [ Term { ',' Term } ]
// Term        ::= Identifier | Variable | Number

// Sample program
//
// true.
// unfalse().
// edge(a, b).
// edge(b, c).
// path(X, Y) :- edge(X, Y).
// path(X, Y) :- edge(X, Z), path(Z, Y).

mod helpers;
mod unify;

use std::fmt;
use std::collections::HashMap;

use crate::lang::scan::Scanner;
use crate::lang::program;
pub use unify::Bindings;

// Program

#[derive(Debug, Eq, PartialEq)]
pub struct Program {
    pub facts: Vec<Fact>,
    pub rules: Vec<Rule>,
}

type Fact = Term;

impl Program {
    pub fn from_string(text: &str) -> Result<Self, String> {
        let mut scanner = Scanner::new(text);
        Self::build(&mut scanner)
    }

    pub fn build(scanner: &mut Scanner) -> Result<Self, String> {
        program::helpers::parse(scanner)
    }
}

// Rule

#[derive(Debug, Eq, PartialEq)]
pub struct Rule {
    pub head: Term,
    pub body: Vec<Term>,
}

// Term

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum Term {
    Ident(String),
    Int(i64),
    String(String),
    Var(String),
    Functor(String, Vec<Term>),
}

impl Term {
    pub fn unify<'a>(
        t0: &'a Term,
        t1: &'a Term
    ) -> Option<Bindings<'a>> {
        Self::unify_with_bindings(t0, t1, HashMap::new())
    }

    pub fn unify_with_bindings<'a>(
        t0: &'a Term,
        t1: &'a Term,
        mut bindings: Bindings<'a>,
    ) -> Option<Bindings<'a>> {
        if program::unify::unify_terms(t0, t1, &mut bindings) {
            Some(bindings)
        } else {
            None
        }
    }

    pub fn subsitute_bindings<'a>(
        term: &'a Term,
        bindings: &Bindings<'a>,
    ) -> Self {
        match term {
            Term::Ident(value) => Term::Ident(value.to_string()),
            Term::Int(value) => Term::Int(*value),
            Term::String(value) => Term::String(value.to_string()),
            Term::Var(value) => Self::lookup(value, bindings),
            Term::Functor(name, terms) => {
                let terms: Vec<Term> =
                    terms
                    .into_iter()
                    .map(|term| {
                        Self::subsitute_bindings(term, bindings)
                    })
                    .collect();
                Term::Functor(name.to_string(), terms)
            }
        }
    }

    fn lookup<'a>(var_name: &str, bindings: &Bindings<'a>) -> Term {
        match bindings.get(var_name) {
            Some(&term) => term.clone(),
            None => Term::Var(var_name.to_string()),
        }
    }
}

impl Term {
    pub fn get_functor_spec(&self) -> (&str, usize) {
        if let Term::Functor(name, args) = self {
            (name, args.len())
        } else {
            panic!("Error call on non-functor term")
        }
    }
}

// Display

impl fmt::Display for Program {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Program:")?;

        writeln!(f, "  Facts:")?;
        for fact in &self.facts {
            writeln!(f, "    {}.", fact)?;
        }
        writeln!(f, "  Rules:")?;
        for rule in &self.rules {
            writeln!(f, "    {}.", rule)?;
        }
        Ok(())
    }
}

impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted: Vec<String> = self.body
            .iter()
            .map(|term| format!("{}", term))
            .collect();
        let body = formatted.join(", ");

        write!(f, "{} :- {}", self.head, body)
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Term::Ident(value) => write!(f, "{}", value),
            Term::Int(value) => write!(f, "{}", value),
            Term::String(value) => write!(f, "\"{}\"", value),
            Term::Var(value) => write!(f, "{}", value),
            Term::Functor(name, terms) => {
                write!(f, "{}", name)?;
                if terms.len() > 0 {
                    write!(f, "(")?;

                    let formatted: Vec<String> = terms
                        .iter()
                        .map(|term| format!("{}", term))
                        .collect();

                    write!(f, "{}", formatted.join(", "))?;

                    write!(f, ")")?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Term::*;

    macro_rules! program {
        ($($input:tt)*) => {{
            let code = stringify!($($input)*);
            Program::from_string(code).unwrap()
        }};
    }

    #[test]
    fn it_parsed_valid_program() {
        let got = program! {
            edge(a, b).
            edge(b, c).
            path(X, Y) :- edge(X, Y).
            path(X, Y) :- edge(X, Z), path(Z, Y).
        };
        let want = Program {
            facts: vec![
                Functor(
                    "edge".to_string(),
                    vec![Ident("a".to_string()), Ident("b".to_string())],
                ),
                Functor(
                    "edge".to_string(),
                    vec![Ident("b".to_string()), Ident("c".to_string())],
                ),
            ],
            rules: vec![
                Rule {
                    head: Functor(
                        "path".to_string(),
                        vec![Var("X".to_string()), Var("Y".to_string())],
                    ),
                    body: vec![
                        Functor(
                            "edge".to_string(),
                            vec![Var("X".to_string()), Var("Y".to_string())],
                        ),
                    ],
                },
                Rule {
                    head: Functor(
                        "path".to_string(),
                        vec![Var("X".to_string()), Var("Y".to_string())],
                    ),
                    body: vec![
                        Functor(
                            "edge".to_string(),
                            vec![Var("X".to_string()), Var("Z".to_string())],
                        ),
                        Functor(
                            "path".to_string(),
                            vec![Var("Z".to_string()), Var("Y".to_string())],
                        ),
                    ],
                },
            ],
        };

        assert_eq!(got, want);
    }

    #[test]
    fn it_reports_error_for_invalid_program() {
        let test_cases = [
            ("foo(a,b)", "Error during parsing: unexpected end of input"),
            ("foo(a,b),", "Error during parsing: unexpected token Comma"),
            ("foo(a,,)", "Error during parsing: expected a term, found: Comma"),
            ("foo(a,", "Error during parsing term: unexpected end of input"),
            (".(a,b)", "Error during parsing: expected an identifier found: Period"),
            ("foo(X,Y) :- ,(X,Y)", "Error during parsing: expected an identifier found: Comma"),
            ("foo(X,Y) :- bar(X,Y)(", "Error during parsing: expected symbol Period found OpenParen"),
        ];
        for (program, expected_error) in test_cases {
            match Program::from_string(program) {
                Ok(_) => panic!("expected to receive parse error"),
                Err(e) => assert_eq!(e, expected_error),
            }
        }
    }
}
