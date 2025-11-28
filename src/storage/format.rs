use std::convert::TryInto;
use std::mem;
use std::str;

use crate::errors;
use crate::lang::Term;

pub const BYTE_SIZE: usize = mem::size_of::<u8>();
pub const U16_SIZE: usize = mem::size_of::<u16>();
pub const U32_SIZE: usize = mem::size_of::<u32>();
pub const I32_SIZE: usize = mem::size_of::<i32>();
pub const USIZE_SIZE: usize = mem::size_of::<usize>();

////////////////////////////////////////////////////////////////////////////////
/// Data types
////////////////////////////////////////////////////////////////////////////////

pub const ATOM_TYPE: u8 = 0;
pub const STRING_TYPE: u8 = 1;
pub const INT_TYPE: u8 = 2;
pub const U8_TYPE: u8 = 3;
pub const U16_TYPE: u8 = 4;
pub const U32_TYPE: u8 = 5;

////////////////////////////////////////////////////////////////////////////////
/// Column families
////////////////////////////////////////////////////////////////////////////////

pub const FIRST_RULE_SEQ: u16 = 1 << 15;

// Metadata Keys
pub const NEXT_PREFIX_SEQ: u8 = 0;
pub const NEXT_TUP_SEQ: u8 = 1;

/// Declarations of facts + rules (same namespace)
/// Key: [string] [u8]
/// Val: [u16]
///
/// The key is the name/arity of the atom. Facts are between [0, 2^15) and rules
/// are between [2^15, 2^16).
pub const DECL: &'static str = "decl";

/// Reverse lookup of prefix codes
/// Key: [u16]
/// Val: [string]
///
/// This is useful in cases where you have the prefix code and want to get the
/// name associated with it.
pub const RVLK: &'static str = "rvlk";

/// Metadata
///
/// Holds the next prefix code available. It is a 2 byte number with values
/// greater than or equal 1 << 15 denoting rules and less than 1 << 15 denoting
/// facts. In other words, a 1 in the high order bit flags a rule, a 0 flags
/// a fact.
/// Key: [NEXT_PREFIX_SEQ]
/// Val: [u16]
///
/// Holds the next tuple id as a 4 byte number.
/// Key: [NEXT_TUP_SEQ]
/// Val: [u32]
///
pub const META: &'static str = "meta";

/// Definitions (rules text)
/// Key: [u16]
/// Val: [string]
pub const DEF: &'static str = "def";

/// Tuples (term data)
/// Key: [u16] [u32]
/// Val: [encoded term data]
///
/// example:
///     0x0042 42 -> (a, b)
///
/// The key is the prefix code followed by the tuple id.
pub const TUP: &'static str = "tup";

/// TODO: Not used with functors yet
///
/// Indexes
/// Key: [prefix] [index] [encoded value] [tuple id]
/// Val: [tuple id]
pub const IDX: &'static str = "idx";

////////////////////////////////////////////////////////////////////////////////
/// Encoding values
////////////////////////////////////////////////////////////////////////////////

pub trait ValEncodable {
    fn encode_append(&self, out: &mut Vec<u8>);
}

impl ValEncodable for u8 {
    fn encode_append(&self, out: &mut Vec<u8>) {
        out.push(*self)
    }
}
impl ValEncodable for u16 {
    fn encode_append(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_be_bytes())
    }
}
impl ValEncodable for u32 {
    fn encode_append(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_be_bytes())
    }
}
impl ValEncodable for i32 {
    fn encode_append(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_be_bytes())
    }
}

impl ValEncodable for str {
    fn encode_append(&self, out: &mut Vec<u8>) {
        let len = self.len() as u32;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(self.as_bytes())
    }
}

impl ValEncodable for String {
    fn encode_append(&self, out: &mut Vec<u8>) {
        self.as_str().encode_append(out)
    }
}

impl<T: ValEncodable> ValEncodable for Option<T> {
    fn encode_append(&self, out: &mut Vec<u8>) {
        match self {
            None => out.push(0), // tag 0 -> null
            Some(v) => {
                out.push(1); // tag 1 -> something
                v.encode_append(out)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Decoding values
////////////////////////////////////////////////////////////////////////////////

/// Types that can decode themselves from a byte slice, returning the value
/// and the remaining slice.
pub trait FromSlice: Sized {
    fn decode_from(input: &[u8]) -> (Self, &[u8]);
}

impl FromSlice for u8 {
    fn decode_from(input: &[u8]) -> (Self, &[u8]) {
        let b = input[0];
        (b, &input[1..])
    }
}

impl FromSlice for u16 {
    fn decode_from(input: &[u8]) -> (Self, &[u8]) {
        let x = u16::from_be_bytes(input[0..2].try_into().unwrap());
        (x, &input[2..])
    }
}

impl FromSlice for u32 {
    fn decode_from(input: &[u8]) -> (Self, &[u8]) {
        let x = u32::from_be_bytes(input[0..4].try_into().unwrap());
        (x, &input[4..])
    }
}

impl FromSlice for i32 {
    fn decode_from(input: &[u8]) -> (Self, &[u8]) {
        let x = i32::from_be_bytes(input[0..4].try_into().unwrap());
        (x, &input[4..])
    }
}

impl FromSlice for String {
    fn decode_from(input: &[u8]) -> (Self, &[u8]) {
        let (len, rest) = <u32 as FromSlice>::decode_from(input);
        let len = len as usize;
        let s = str::from_utf8(&rest[..len]).unwrap();
        (s.to_owned(), &rest[len..])
    }
}

pub trait ValDecodable {
    fn decode_u8(self) -> (u8, Self)
    where
        Self: Sized;
    fn decode_u16(self) -> (u16, Self)
    where
        Self: Sized;
    fn decode_u32(self) -> (u32, Self)
    where
        Self: Sized;
    fn decode_i32(self) -> (i32, Self)
    where
        Self: Sized;
    fn decode_string(self) -> (String, Self)
    where
        Self: Sized;

    /// Decode Option<T> where T knows how to decode itself from a slice.
    fn decode_option<T: FromSlice>(self) -> (Option<T>, Self)
    where
        Self: Sized;
}

impl<'a> ValDecodable for &'a [u8] {
    fn decode_u8(self) -> (u8, Self) {
        <u8 as FromSlice>::decode_from(self)
    }
    fn decode_u16(self) -> (u16, Self) {
        <u16 as FromSlice>::decode_from(self)
    }
    fn decode_u32(self) -> (u32, Self) {
        <u32 as FromSlice>::decode_from(self)
    }
    fn decode_i32(self) -> (i32, Self) {
        <i32 as FromSlice>::decode_from(self)
    }
    fn decode_string(self) -> (String, Self) {
        <String as FromSlice>::decode_from(self)
    }
    fn decode_option<T: FromSlice>(self) -> (Option<T>, Self) {
        let (tag, rest) = <u8 as FromSlice>::decode_from(self);
        if tag == 0 {
            (None, rest)
        } else {
            let (val, rest) = T::decode_from(rest);
            (Some(val), rest)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Tagged values
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub enum TaggedVal {
    Atom(String),
    Str(String),
    Int(i32),
}

impl TaggedVal {
    pub fn encode_from_scm(
        tagged_vals: &[Self],
        scm: &[u8],
        pos_iter: impl Iterator<Item = u8>,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let mut buf = vec![];

        for pos in pos_iter {
            let tagged_val = &tagged_vals[usize::try_from(pos).unwrap()];
            let ty = &scm[usize::try_from(pos).unwrap()];

            match *ty {
                ATOM_TYPE => {
                    if let TaggedVal::Atom(x) = tagged_val {
                        x.as_str().encode_append(&mut buf);
                    } else {
                        return Err(errors::RecallError::RuntimeError(format!(
                            "tuple position {} expected type atom",
                            pos
                        ))
                        .into());
                    }
                }

                STRING_TYPE => {
                    if let TaggedVal::Str(x) = tagged_val {
                        x.as_str().encode_append(&mut buf);
                    } else {
                        return Err(errors::RecallError::RuntimeError(format!(
                            "tuple position {} expected type string",
                            pos
                        ))
                        .into());
                    }
                }

                INT_TYPE => {
                    if let TaggedVal::Int(x) = tagged_val {
                        (*x).encode_append(&mut buf);
                    } else {
                        return Err(errors::RecallError::RuntimeError(format!(
                            "tuple position {} expected type int",
                            pos
                        ))
                        .into());
                    }
                }

                _ => unreachable!(),
            }
        }

        Ok(buf)
    }

    pub fn decode_from_scm(
        mut tuple: &[u8],
        scm: &[u8],
        pos_iter: impl Iterator<Item = u8>,
    ) -> Vec<Self> {
        let mut buf = vec![];

        for pos in pos_iter {
            let ty = &scm[usize::try_from(pos).unwrap()];

            let (x, rest) = match *ty {
                ATOM_TYPE => {
                    let (x, rest) = tuple.decode_string();
                    (TaggedVal::Atom(x), rest)
                }

                STRING_TYPE => {
                    let (x, rest) = tuple.decode_string();
                    (TaggedVal::Str(x), rest)
                }

                INT_TYPE => {
                    let (x, rest) = tuple.decode_i32();
                    (TaggedVal::Int(x), rest)
                }

                _ => unreachable!(),
            };
            buf.push(x);
            tuple = rest;
        }

        buf
    }
}

impl From<TaggedVal> for Term {
    fn from(p: TaggedVal) -> Term {
        match p {
            TaggedVal::Atom(s) => Term::Sym(s),
            TaggedVal::Str(s) => Term::Str(s),
            TaggedVal::Int(i) => Term::Int(i),
        }
    }
}

impl From<&Term> for TaggedVal {
    fn from(t: &Term) -> Self {
        match t {
            Term::Sym(s) => TaggedVal::Atom(s.to_string()),
            Term::Str(s) => TaggedVal::Str(s.to_string()),
            Term::Int(i) => TaggedVal::Int(*i),
            _ => unreachable!(),
        }
    }
}
