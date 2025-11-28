pub mod analysis;
pub mod parse;
pub mod scan;
pub mod unify;

use std::fmt;
use std::collections::HashMap;
use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lang::parse::AssertionKind;

pub type Atom = Term;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum Term {
    Sym(String),
    Str(String),
    Int(i32),
    Var(String),
    Atom(String, Vec<Term>, bool),
}

pub type Defs = HashMap<String, Rule>;
pub type Strata = Vec<Defs>;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Rule {
    pub head: Atom,
    pub clauses: Vec<Vec<Atom>>,
}

pub trait DBAux {
    fn get_rule(&self, name: &str, arity: u8) -> Option<Rule>;
    fn lookup(&mut self, search_term: Atom) -> Box<dyn Iterator<Item = (u32, Atom)>>;
}

impl Term {
    pub fn encode(term: &Term) -> Result<Vec<u8>, anyhow::Error> {
        let result = bincode::serde::encode_to_vec(term, bincode::config::standard())?;
        Ok(result)
    }

    pub fn decode(slice: &[u8]) -> Result<Term, anyhow::Error> {
        let (result, _) = bincode::serde::decode_from_slice(slice, bincode::config::standard())?;
        Ok(result)
    }
}

#[derive(Debug)]
pub struct QueryAnswer {
    pub query: Atom,
    pub answer: Vec<unify::Bindings>,
}

impl QueryAnswer {
    pub fn get_atoms(&self) -> HashSet<Atom> {
        self.answer
            .iter()
            .map(|bindings| unify::substitute_bindings(&self.query, bindings))
            .collect()
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Term::Sym(value) => write!(f, "{}", value),
            Term::Str(value) => write!(
                f,
                "\"{}\"",
                value
                    .replace("\"", "\\\"")
                    .replace("\t", "\\t")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\0", "\\0")
            ),
            Term::Var(value) => write!(f, "{}", value),
            Term::Int(value) => write!(f, "{}", value),
            Term::Atom(name, terms, negated) => {
                if *negated {
                    write!(f, "not ")?;
                }
                write!(f, "{}", name)?;
                if terms.len() > 0 {
                    write!(f, "(")?;

                    let formatted: Vec<String> =
                        terms.iter().map(|term| format!("{}", term)).collect();

                    write!(f, "{}", formatted.join(", "))?;

                    write!(f, ")")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for parse::AssertionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AssertionKind::Fact { head: _ } => {
                unreachable!()
            },
            AssertionKind::RuleClause { head, body } => {
                write!(f, "{}", head)?;

                if !body.is_empty() {
                    write!(f, " :- ")?;
                    for (i, pred) in body.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{}", pred)?;
                    }
                }
                write!(f, ".")
            }
        }
    }
}
