// TODO: drop fact from schema cf (also drops any rows in data cf too)
// TODO: inlcude builtin predicates: == != < <= > >=

// use std::fmt::Display;
// use std::fmt::Formatter;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub mod eval;
pub mod lang;
pub mod storage;
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

#[derive(Debug)]
pub struct Config {
    pub name: String,
    pub h: bool,
    pub v: bool,
    pub c: bool,
    pub program: String,
    pub db: Option<PathBuf>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ArgType {
    #[serde(rename = "atom")]
    Atom,
    #[serde(rename = "int")]
    Int,
    #[serde(rename = "string")]
    Str,
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
