pub mod lang;
pub mod storage;
pub mod eval;

pub mod errors {
    #[derive(Debug, PartialEq, Eq)]
    pub enum RecallError {
        TypeError(String),
        UniqueKeyError(u32),
    }
}
