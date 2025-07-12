// TODO: drop fact from schema cf (also drops any rows in data cf too)
// TODO: inlcude builtin predicates: == != < <= > >=

use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

pub mod lang;
pub mod storage;
pub mod eval;
pub mod errors {
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum RecallError {
        #[error("type error: {0}")]
        TypeError(String),

        #[error("runtime error: {0}")]
        RuntimeError(String),

        #[error("syntax error: {0}")]
        ParseError(String),

        #[error("syntax error: {0}")]
        ScanError(String),

        #[error("error found in rocksdb: {0}")]
        RocksDB(#[from] rocksdb::Error),

        #[error("error found in os input/output: {0}")]
        IOError(#[from] std::io::Error),
    }
}

use storage::tuple::ParameterType;
use lang::parse::Term;
use lang::parse::Literal;

impl From<ParameterType> for Term {
    fn from(p: ParameterType) -> Term {
        match p {
            ParameterType::Atom(s) => Term::Atom(s),
            ParameterType::Str(s) => Term::Str(s),
            ParameterType::Int(i) => Term::Integer(i),
            _ => unreachable!(),
        }
    }
}

impl From<&Term> for ParameterType {
    fn from(t: &Term) -> Self {
        match t {
            Term::Atom(s) => ParameterType::Atom(s.to_string()),
            Term::Str(s) => ParameterType::Str(s.to_string()),
            Term::Integer(i) => ParameterType::Int(*i),
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Term::Atom(value) => write!(f, "{}", value),
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
            Term::Integer(value) => write!(f, "{}", value),
            Term::Functor(name, terms, negated) => {
                if *negated {
                    write!(f, "not ")?;
                }
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

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // head
        write!(f, "{}", self.head)?;
        // body, if any
        if !self.body.is_empty() {
            write!(f, " :- ")?;
            for (i, pred) in self.body.iter().enumerate() {
                if i > 0 { write!(f, ", ")?; }
                write!(f, "{}", pred)?;
            }
        }
        write!(f, ".")
    }
}

/// app-wide config
static mut VERBOSE: bool = false;

pub fn set_verbose() {
    unsafe {
        VERBOSE = true;
    }
}

pub fn get_verbose() -> bool {
    unsafe { VERBOSE }
}
