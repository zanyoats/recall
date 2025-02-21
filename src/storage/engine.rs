use std::collections::HashMap;
use std::mem;
use std::fmt;
use std::str::FromStr;

pub mod linked_list;

pub trait RecordValue {
    fn value(&self) -> &Vec<ArgType>;
}

pub trait Engine {
    type Iter<'a>: Iterator<Item: RecordValue> where Self: 'a;
    fn create(
        base_dir: &str,
        name: &str,
        schema: Vec<SchemaArg>,
        keys: Vec<usize>,
    ) -> Self;
    fn open(base_dir: &str, name: &str) -> Self;
    fn artiy(&mut self) -> usize;
    fn iter<'a>(&'a mut self) -> Self::Iter<'a>;
    fn assertz(&mut self, value: &Vec<ArgType>);
    fn retract(&mut self, filters: &HashMap<usize, ArgType>) -> Result<(), EngineError>;
    fn save(&mut self);
}

#[derive(Debug)]
pub enum EngineError {
    RecordNotFound,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ArgType {
    String(String),
    Int(i64),
}

pub enum SchemaType {
    String(usize),
    Int,
}

pub struct SchemaArg {
    pub schema_type: SchemaType,
    pub flags: u32,
}

// Schema Arg Flags
pub const NO_FLAGS: u32 = 0b00000000;
// other flags ideas: auto, nullable, etc.

impl SchemaType {
    fn size(&self) -> usize {
        match self {
            SchemaType::String(max_length) => *max_length,
            SchemaType::Int => mem::size_of::<i64>(),
        }
    }
}

impl fmt::Display for SchemaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaType::String(max_length) => write!(f, "string({})", max_length),
            SchemaType::Int => write!(f, "int"),
        }
    }
}

impl FromStr for SchemaType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let option = s
            .strip_prefix("string(")
            .and_then(|s| s.strip_suffix(")"));

        if let Some(inner) = option {
            inner
                .parse::<usize>()
                .map(SchemaType::String)
                .map_err(|e| e.to_string())
        } else if s == "int" || s == "uint" {
            Ok(SchemaType::Int)
        } else {
            Err(format!("Invalid SchemaType: {}", s))
        }
    }
}
