use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

pub mod lang;
pub mod storage;
pub mod eval;
pub mod errors {
    use std::fmt;
    use std::error::Error;

    pub type GenericError = Box<dyn Error + Send + Sync + 'static>;

    #[derive(Debug)]
    pub enum RecallError {
        CLIError(String),
        TypeError(String),
        UniqueKeyError,
        PredicateNotFound(String, i32),
        ReplaceKeyNotFound,
        ParseError(String),
        ScanError(String),
        ScanErrorUnderlying(Box<dyn Error + Send + Sync + 'static>),
        RocksDB(Box<rocksdb::Error>),
        Readline(Box<rustyline::error::ReadlineError>),
        IOError(Box<std::io::Error>),
    }


    impl From<rocksdb::Error> for RecallError {
        fn from(e: rocksdb::Error) -> RecallError {
            RecallError::RocksDB(Box::new(e))
        }
    }

    impl From<rustyline::error::ReadlineError> for RecallError {
        fn from(e: rustyline::error::ReadlineError) -> RecallError {
            RecallError::Readline(Box::new(e))
        }
    }

    impl From<std::io::Error> for RecallError {
        fn from(e: std::io::Error) -> RecallError {
            RecallError::IOError(Box::new(e))
        }
    }

    impl fmt::Display for RecallError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            use RecallError::*;

            match self {
                ParseError(s) | ScanError(s) => write!(f, "syntax error: {}", s),
                TypeError(s) => write!(f, "type error: {}", s),
                ScanErrorUnderlying(err) => {
                    writeln!(f, "error found during parsing: {}", err)?;

                    // Walk the chain of error sources
                    let mut source = err.source();
                    while let Some(inner) = source {
                        writeln!(f, "caused by: {}", inner)?;
                        source = inner.source();
                    }
                    Ok(())
                },
                RocksDB(err) => {
                    writeln!(f, "error found in rocksdb: {}", err)?;

                    // Walk the chain of error sources
                    let mut source = err.source();
                    while let Some(inner) = source {
                        writeln!(f, "caused by: {}", inner)?;
                        source = inner.source();
                    }
                    Ok(())
                },
                Readline(err) => {
                    writeln!(f, "error found in readline: {}", err)?;

                    // Walk the chain of error sources
                    let mut source = err.source();
                    while let Some(inner) = source {
                        writeln!(f, "caused by: {}", inner)?;
                        source = inner.source();
                    }
                    Ok(())
                },
                IOError(err) => {
                    writeln!(f, "error found in os input/output: {}", err)?;

                    // Walk the chain of error sources
                    let mut source = err.source();
                    while let Some(inner) = source {
                        writeln!(f, "caused by: {}", inner)?;
                        source = inner.source();
                    }
                    Ok(())
                },
                UniqueKeyError => write!(f, "unqiue key violation"),
                ReplaceKeyNotFound => write!(f, "replace key not found violation"),
                PredicateNotFound(name, arity) => write!(f, "predicate not found: {}/{}", name, arity),
                CLIError(s) => write!(f, "{}", s),
            }
        }
    }

    impl Error for RecallError {}
}

use storage::tuple::ParameterType;
use lang::parse::Term;
use lang::parse::Literal;

/*
    Convert from Paramter Type -> Term
*/
impl From<ParameterType> for Term {
    fn from(p: ParameterType) -> Term {
        match p {
            ParameterType::Atom(s) => Term::Atom(s),
            ParameterType::Str(s) => Term::Str(s),
            ParameterType::Int(i) => Term::Integer(i),
        }
    }
}

impl From<Vec<ParameterType>> for Term {
    fn from(mut params: Vec<ParameterType>) -> Term {
        // pull off the first element as the functor name
        let name = match params
            .drain(..1)
            .next()
            .unwrap() {
            ParameterType::Atom(s) => s,
            _ => unreachable!(),
        };

        // convert the rest into Terms
        let args: Vec<Term> = params
            .into_iter()
            .map(Term::from)
            .collect();

        Term::Functor(name, args)
    }
}

/*
    Convert from Term -> Paramter Type
*/
impl From<Term> for Vec<ParameterType> {
    fn from(t: Term) -> Vec<ParameterType> {
        match t {
            Term::Atom(s) => vec![ParameterType::Atom(s)],
            Term::Str(s) => vec![ParameterType::Str(s)],
            Term::Integer(i) => vec![ParameterType::Int(i)],
            Term::Functor(name, args) => {
                let mut result = vec![ParameterType::Atom(name)];
                for arg in args {
                    let arg: Vec<ParameterType> = arg.into();
                    result.extend(arg.into_iter());
                }
                result
            },
            _ => unreachable!(),
        }
    }
}

// Literal, Term to string
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

pub trait QueryEvaluator<'a> {
    type Iter: Iterator<Item = lang::parse::Predicate> + Default;
    fn evaluate(&self, query: lang::parse::Predicate, txn: &'a storage::db::TransactionOp) -> Result<Self::Iter, errors::RecallError>;
}
