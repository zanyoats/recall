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
        PredicateNotFound(String, u32),
        ReplaceKeyNotFound,
        ParseError(String),
        ScanError(String),
        ScanErrorUnderlying(Box<dyn Error + Send + Sync + 'static>),
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
                UniqueKeyError => write!(f, "unqiue key violation"),
                ReplaceKeyNotFound => write!(f, "replace key not found violation"),
                PredicateNotFound(name, arity) => write!(f, "predicate not found: {}/{}", name, arity),
                CLIError(s) => write!(f, "{}", s),
            }
        }
    }

    impl Error for RecallError {}
}
