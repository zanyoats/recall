pub mod sizedbuf;
pub mod tuple;
pub mod page;

use std::mem;

pub const PAGE_SIZE_4K: usize = 4096; // 4 KB pages
pub const BYTE_SIZE: usize = mem::size_of::<u8>();
pub const U16_SIZE: usize = mem::size_of::<u16>();
pub const U32_SIZE: usize = mem::size_of::<u32>();
pub const CATALOG_ROOT_PAGE_NUM: u32 = 0;
pub const NULL_PTR: u32 = 0;
const INTERNAL_NODE: u8 = 0;
const LEAF_NODE: u8 = 1;
const FALSE: u8 = 0;
const TRUE: u8 = 1;

// Data types
pub const ATOM_TYPE: u8 = 0;
pub const STRING_TYPE: u8 = 1;
pub const UINT_TYPE: u8 = 2;
pub const INT_TYPE: u8 = 3;
pub const BYTES_TYPE: u8 = 4;

// Catalog object keys
pub const PREDICATES_KEY: u32 = 0;
// pub const SCHEMAS_KEY: u32 = 1;
// pub const INDEXES_KEY: u32 = 2;
// pub const SEQUENCES_KEY: u32 = 3;
