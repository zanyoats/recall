use std::mem;

pub const BYTE_SIZE: usize = mem::size_of::<u8>();
pub const U16_SIZE: usize = mem::size_of::<u16>();
pub const I32_SIZE: usize = mem::size_of::<i32>();
pub const USIZE_SIZE: usize = mem::size_of::<usize>();

// Data types
pub const ATOM_TYPE: u8 = 0;
pub const STRING_TYPE: u8 = 1;
pub const INT_TYPE: u8 = 3;
