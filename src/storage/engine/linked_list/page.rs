use std::num;
use std::mem;
use std::ptr;
use std::slice;
use crate::storage::engine::SchemaArg;

pub type Page = [u8; PAGE_SIZE];

pub const PAGE_SIZE: usize = 4096; // 4 KB pages
pub const DIR_PAGE_NUM: usize = 0;
pub const NULL_PAGE_PTR: usize = 0;
pub const SINGLE_BYTE: usize = mem::size_of::<u8>();
pub const USIZE_SIZE: usize = mem::size_of::<usize>();
pub const INT_SIZE: usize = mem::size_of::<i32>();
pub const LONGINT_SIZE: usize = mem::size_of::<i64>();

pub fn read_u32_offset(page: &Page, offset: usize) -> u32 {
    unsafe {
        let ptr = page.as_ptr().add(offset);
        let value = ptr.cast::<u32>().read_unaligned();
        u32::from_be(value)
    }
}

pub fn read_i64_offset(page: &Page, offset: usize) -> i64 {
    unsafe {
        let ptr = page.as_ptr().add(offset);
        let value = ptr.cast::<i64>().read_unaligned();
        i64::from_be(value)
    }
}

pub fn read_usize_offset(page: &Page, offset: usize) -> usize {
    unsafe {
        let ptr = page.as_ptr().add(offset);
        let value = ptr.cast::<usize>().read_unaligned();
        usize::from_be(value)
    }
}

pub fn write_u32_offset(page: &mut Page, offset: usize, value: u32) {
    unsafe {
        let ptr = page.as_mut_ptr().add(offset);
        ptr.cast::<u32>().write(u32::to_be(value));
    }
}

pub fn write_i64_offset(page: &mut Page, offset: usize, value: i64) {
    unsafe {
        let ptr = page.as_mut_ptr().add(offset);
        ptr.cast::<i64>().write(i64::to_be(value));
    }
}

pub fn write_usize_offset(page: &mut Page, offset: usize, value: usize) {
    unsafe {
        let ptr = page.as_mut_ptr().add(offset);
        ptr.cast::<usize>().write(usize::to_be(value));
    }
}

pub fn read_string_offset(page: &Page, offset: usize) -> String {
    unsafe {
        let str_len = read_usize_offset(page, offset);
        let str_ptr = page.as_ptr().add(offset + USIZE_SIZE);

        String::from_utf8(slice::from_raw_parts(
            str_ptr,
            str_len,
        ).to_vec())
        .unwrap()
    }
}

pub fn write_string_offset(
    page: &mut Page,
    offset: usize,
    value: &str,
    max_length: usize,
) {
    let str_len = value.len();

    assert!(
        USIZE_SIZE + str_len <= max_length,
        "Error: input string too large to fit",
    );

    unsafe {
        let ptr = page.as_mut_ptr().add(offset);

        // Write string length
        ptr.cast::<usize>().write(usize::to_be(str_len));

        // then len bytes
        ptr::copy_nonoverlapping(
            value.as_ptr(),
            ptr.add(USIZE_SIZE),
            str_len,
        );
    }
}

// Layout of directory page (page 0)
// NOTE: This page should remain in memory for the duration of the program
//
// Directory Page Layout, arity of degree n:
//   dir_header
//   arg_cell_0
//   ...
//   arg_cell_n
//   padding to make it a full 4k block

// dir_header:
//   num_pages   usize
//   num_cells   usize
//   head_page   usize
//   tail_page   usize
//   arity       usize
//   stride      usize
//   key_offsets String(64)

// arg_cell_i:
//   type   String(64)
//   flags  u32
//   offset usize

pub const NUM_PAGES_OFFSET: usize = 0;
const NUM_PAGES_SIZE: usize       = USIZE_SIZE;

pub const NUM_CELLS_OFFSET: usize = NUM_PAGES_OFFSET + NUM_PAGES_SIZE;
const NUM_CELLS_SIZE: usize       = USIZE_SIZE;

pub const HEAD_PAGE_OFFSET: usize = NUM_CELLS_OFFSET + NUM_CELLS_SIZE;
const HEAD_PAGE_SIZE: usize       = USIZE_SIZE;

pub const TAIL_PAGE_OFFSET: usize = HEAD_PAGE_OFFSET + HEAD_PAGE_SIZE;
const TAIL_PAGE_SIZE: usize       = USIZE_SIZE;

pub const ARITY_OFFSET: usize = TAIL_PAGE_OFFSET + TAIL_PAGE_SIZE;
const ARITY_SIZE: usize       = USIZE_SIZE;

pub const STRIDE_OFFSET: usize = ARITY_OFFSET + ARITY_SIZE;
const STRIDE_SIZE: usize       = USIZE_SIZE;

pub const KEYS_OFFSET_OFFSET: usize = STRIDE_OFFSET + STRIDE_SIZE;
const KEYS_OFFSET_SIZE: usize       = 64;

pub const DIR_HEADER_SIZE: usize =
      NUM_PAGES_SIZE
    + NUM_CELLS_SIZE
    + HEAD_PAGE_SIZE
    + TAIL_PAGE_SIZE
    + ARITY_SIZE
    + STRIDE_SIZE
    + KEYS_OFFSET_SIZE;

pub const ARG_TYPE_SIZE:  usize = 64;
pub const ARG_FLAGS_SIZE: usize = USIZE_SIZE;
pub const ARG_OFFSET_SIZE: usize = USIZE_SIZE;

pub const ARG_CELL_SIZE: usize =
      ARG_TYPE_SIZE
    + ARG_FLAGS_SIZE
    + ARG_OFFSET_SIZE;
pub const ARG_CELL_SPACE: usize = PAGE_SIZE - DIR_HEADER_SIZE;
pub const ARG_CELL_CAPACITY: usize = ARG_CELL_SPACE / ARG_CELL_SIZE;

fn stringify_keys(input: &[usize]) -> String {
    input.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(",")
}

pub fn parse_stringified_keys(input: &str) -> Result<Vec<usize>, num::ParseIntError> {
    input
        .split(',')
        .filter(|s| !s.is_empty())
        .map(str::trim) // Remove any accidental whitespace
        .map(str::parse::<usize>) // Convert to usize
        .collect() // Collect into a Vec<usize>
}

pub fn init_dir_page(
    page: &mut Page,
    schema: Vec<SchemaArg>,
    keys: Vec<usize>,
) {
    let arity = schema.len();

    assert!(
        arity <= ARG_CELL_CAPACITY,
        "Error: arity of predicate is larger then the max number: {}",
        ARG_CELL_CAPACITY,
    );

    for key in keys.iter() {
        assert!(*key < arity, "Error: key not in range");
    }

    write_usize_offset(page, ARITY_OFFSET, arity);

    let mut stride = 0usize;

    for i in 0..arity {
        let arg = &schema[i];

        let offset = DIR_HEADER_SIZE + i * ARG_CELL_SIZE;

        write_string_offset(
            page,
            offset,
            &arg.schema_type.to_string(),
            ARG_TYPE_SIZE,
        );

        write_u32_offset(
            page,
            offset + ARG_TYPE_SIZE,
            arg.flags,
        );

        write_usize_offset(
            page,
            offset + ARG_TYPE_SIZE + ARG_FLAGS_SIZE,
            stride,
        );

        stride += arg.schema_type.size();
    }

    write_usize_offset(page, STRIDE_OFFSET, stride);
    write_string_offset(
        page,
        KEYS_OFFSET_OFFSET,
        &stringify_keys(&keys),
        KEYS_OFFSET_SIZE,
    );
}

// Layout of data page (pages 1 .. n)
// NOTE: May evict to reclaim memory
//
// Data Page Layout, arity of degree n:
//   data_header
//   data_cell_0
//   ...
//   data_cell_n
//   padding to make it a full 4k block

// data_header:
//   pred      usize (0 means no predecessor)
//   succ      usize (0 means no successor)
//   num_cells usize

// data_cell_i block of bytes os length stride

pub const PRED_OFFSET: usize = 0;
const PRED_SIZE: usize       = USIZE_SIZE;

pub const SUCC_OFFSET: usize = PRED_OFFSET + PRED_SIZE;
const SUCC_SIZE: usize       = USIZE_SIZE;

pub const DATA_NUM_CELLS_OFFSET: usize = SUCC_OFFSET + SUCC_SIZE;
const DATA_NUM_CELLS_SIZE: usize       = USIZE_SIZE;

pub const DATA_HEADER_SIZE: usize =
      PRED_SIZE
    + SUCC_SIZE
    + DATA_NUM_CELLS_SIZE;

pub const DATA_CELL_SPACE: usize = PAGE_SIZE - DATA_HEADER_SIZE;
