pub mod sizedbuf;
pub mod tuple;
pub mod page;

use std::mem;
use sizedbuf::SizedBuf;

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
pub const ATOM_TYPE: u8 = 0;
pub const STRING_TYPE: u8 = 1;
pub const UINT_TYPE: u8 = 2;
pub const INT_TYPE: u8 = 3;
pub const BYTES_TYPE: u8 = 4;

pub struct CatalogPage {
    buf: SizedBuf,
}

impl CatalogPage {
    pub fn new() -> Self {
        let buf = SizedBuf::new(PAGE_SIZE_4K);

        CatalogPage { buf }
    }

    pub fn get(&self) -> &[u8] {
        self.buf.get()
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
        self.buf.get_mut()
    }
}

/// Catalog page
///   size_i      u8
///   predicate_i predicate_def
///   ...

/// predicate_def
///   name          atom
///   root_page_num u32
///   last_tuple_id u32
pub trait CatalogOps {
    fn define_predicate(&mut self, root_page_num: u32, name: &str);
    fn get_predicate_root_page_num(&self, find_name: &str) -> u32;
    fn get_predicate_last_tuple_id(&self, find_name: &str) -> u32;
    fn inc_predicate_last_tuple_id(&mut self, find_name: &str);
}

impl CatalogOps for CatalogPage {
    fn define_predicate(
        &mut self,
        root_page_num: u32,
        name: &str
    ) {
        let mut i = 0;
        let mut offset = loop {
            let (offset, predicate_size) = self.buf.read_u8_offset(i);
            if predicate_size == 0 {
                break i
            } else {
                i = offset + predicate_size as usize;
                continue
            }
        };
        let predicate_size = u8::try_from(
              SizedBuf::atom_storage_size(name.len())
            + U32_SIZE
            + U32_SIZE
        ).unwrap();
        offset = self.buf.write_u8_offset(offset, predicate_size);
        offset = self.buf.write_atom_offset(offset, name);
        offset = self.buf.write_u32_offset(offset, root_page_num);
        offset = self.buf.write_u32_offset(offset, 0);
        assert!(offset <= PAGE_SIZE_4K - 1, "Catalog page is full");
    }

    fn get_predicate_root_page_num(&self, find_name: &str) -> u32 {
        let mut i = 0;
        loop {
            let (offset, predicate_size) = self.buf.read_u8_offset(i);
            assert!(predicate_size > 0, "Predicate name not found");
            i = offset + predicate_size as usize;
            let (offset, stored_name) = self.buf.read_atom_offset(offset);
            if stored_name == find_name {
                let (_, root_page_num) = self.buf.read_u32_offset(offset);
                break root_page_num
            } else {
                continue
            }
        }
    }

    fn get_predicate_last_tuple_id(&self, find_name: &str) -> u32 {
        let mut i = 0;
        loop {
            let (offset, predicate_size) = self.buf.read_u8_offset(i);
            assert!(predicate_size > 0, "Predicate name not found");
            i = offset + predicate_size as usize;
            let (offset, stored_name) = self.buf.read_atom_offset(offset);
            if stored_name == find_name {
                let (offset, _) = self.buf.read_u32_offset(offset);
                let (_, last_tuple_id) = self.buf.read_u32_offset(offset);
                break last_tuple_id
            } else {
                continue
            }
        }
    }

    fn inc_predicate_last_tuple_id(&mut self, find_name: &str) {
        let mut i = 0;
        loop {
            let (offset, predicate_size) = self.buf.read_u8_offset(i);
            assert!(predicate_size > 0, "Predicate name not found");
            i = offset + predicate_size as usize;
            let (offset, stored_name) = self.buf.read_atom_offset(offset);
            if stored_name == find_name {
                let (offset, _) = self.buf.read_u32_offset(offset);
                let (_, last_tuple_id) = self.buf.read_u32_offset(offset);
                let _ = self.buf.write_u32_offset(offset, last_tuple_id + 1);
                break
            } else {
                continue
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_can_use_a_catalog_page() {
        let mut page = CatalogPage::new();
        page.define_predicate(
            1,
            "true",
        );
        page.define_predicate(
            10,
            "foo",
        );
        page.define_predicate(
            42,
            "bar",
        );
        page.define_predicate(
            69,
            "baz",
        );

        assert!(page.get_predicate_root_page_num("foo") == 10);
        assert!(page.get_predicate_last_tuple_id("foo") == 0);

        page.inc_predicate_last_tuple_id("foo");
        assert!(page.get_predicate_last_tuple_id("foo") == 1);

        assert!(page.get_predicate_root_page_num("bar") == 42);
        assert!(page.get_predicate_last_tuple_id("bar") == 0);

        assert!(page.get_predicate_root_page_num("baz") == 69);
        assert!(page.get_predicate_last_tuple_id("baz") == 0);

        assert!(page.get_predicate_root_page_num("true") == 1);
        assert!(page.get_predicate_last_tuple_id("true") == 0);
    }
}
