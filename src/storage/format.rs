use std::mem;

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

/// DECL makes up the namespace for facts and ruless

/// Declarations (facts + rules)
/// Key: Tuple(string, u8)
/// Val: u16 (prefix code)
///
/// examples:
///     link/2 -> 0x0042
///     path/2 -> 0xFEFE
///
/// The key is the name and arity of the predicate. The value is a 2 byte prefix
/// to identify the predicate when referencing in other column families. Even
/// codes are facts and odd ones are rules.
pub const DECL: &'static str = "decl";

/// Reverse lookup of prefix codes
/// Key: u16 (prefix code)
/// Val: string
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
/// Key: NEXT_PREFIX_SEQ
/// Val: u16
///
/// Holds the next tuple id as a 4 byte number.
/// Key: NEXT_TUP_SEQ
/// Val: u32
///
pub const META: &'static str = "meta";

/// Schemas (facts + rules)
/// Key: u16 (prefix code)
/// Val: Vec<u8>
///
/// example:
///     0x0042 -> vec![ATOM_TYPE, ATOM_TYPE]
///
/// The key is the 2 byte prefix code for the predicate. The value is a string
/// of u8 numbers, each denoting a type for the respective arg of the predicate.
pub const SCM: &'static str = "scm";

/// Definitions (rules)
/// Key: u16 (prefix code)
/// Val: string
///
/// assuming path/2 has prefix 0xFEFE
/// example:
///     0xFEFE -> "path(X,Y) :- link(X, Y). path(X,Z) :- link(X, Y), path(Y, Z)."
///
/// The key is the 2 byte prefix code for the predicate. The value is the
/// collection of all rule definitions for path/2 it can find.
pub const DEF: &'static str = "def";

/// Tuples (data)
/// Key: u16 (prefix code) + u32 (id)
/// Val: Vec<u8> (tuple)
///
/// example:
///     0x0042, 42 -> (a, b)
///
/// The key is the 2 byte prefix code followed by the id. The value is a string
/// of u8 numbers representing the tuple args. The example represents:
///   link(a, b) with id=42
pub const TUP: &'static str = "tup";

/// Indices
/// Key: u16 (prefix code) + Vec<u8>
/// Val: u32 (id)
///
/// example:
///     0x0042, "foo" -> 42
///
/// The key is the 2 byte prefix code followed by some domain value encoded as
/// a string of bytes. The value is the id of the predicate.
pub const IDX: &'static str = "idx";
