use std::ptr;

use super::PAGE_SIZE_4K;
use super::U16_SIZE;
use super::U32_SIZE;
use super::BYTE_SIZE;
use super::LEAF_NODE;
use super::{TRUE, FALSE};
use super::INTERNAL_NODE;
use super::NULL_PTR;

use crate::errors;
use super::tuple::Tuple;
use super::sizedbuf::SizedBuf;

pub struct SlottedPage {
    buf: SizedBuf,
    page_size: usize,
    cap_limit: Option<usize>,
    split_strategy: SplitStrategy,
}

pub struct SlottedPageBuilder {
    pub page_size: usize,
    pub cap_limit: Option<usize>,
    pub split_strategy: SplitStrategy,
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum SplitStrategy {
    EmptyPage,
    HalfFullPage,
}

impl SlottedPageBuilder {
    pub fn new() -> Self {
        Self { /* default values */
            cap_limit: None,
            split_strategy: SplitStrategy::HalfFullPage,
            page_size: PAGE_SIZE_4K,
        }
    }

    pub fn page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }

    pub fn cap_limit(mut self, cap_limit: Option<usize>) -> Self {
        self.cap_limit =
            cap_limit
            .inspect(|cap_limit| { assert!(*cap_limit > 2); });

        self
    }

    pub fn split_strategy_half_full_page(mut self) -> Self {
        self.split_strategy = SplitStrategy::HalfFullPage;
        self
    }

    pub fn split_strategy_empty_page(mut self) -> Self {
        self.split_strategy = SplitStrategy::EmptyPage;
        self
    }

    pub fn build(&self) -> SlottedPage {
        let buf = SizedBuf::new(self.page_size);
        SlottedPage {
            buf,
            page_size: self.page_size,
            cap_limit: self.cap_limit,
            split_strategy: self.split_strategy,
         }
    }
}

impl SlottedPage {
    pub fn copy(src: &Self, dst: &mut Self) {
        SizedBuf::copy(&src.buf, &mut dst.buf);
    }

    pub fn get(&self) -> &[u8] {
        self.buf.get()
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
        self.buf.get_mut()
    }

    fn bin_search<F>(
        &self,
        find_key: &u32,
        begin_index: usize,
        end_index: usize,
        get_stored_key: F,
    ) -> usize
    where
        F: Fn(usize) -> u32
    {
        if begin_index >= end_index {
            return begin_index
        }
        let i = (begin_index + end_index) / 2;
        let stored_key = get_stored_key(i);
        if *find_key == stored_key {
            i
        } else if *find_key < stored_key {
            self.bin_search(find_key, begin_index, i, get_stored_key)
        } else { /* find_key > stored_key */
            self.bin_search(find_key, i + 1, end_index, get_stored_key)
        }
    }
}

/// Common page header
///   format           u8   {0=internal, 1=leaf}
///   is_root          u8   {0=false, 1=true}
///   num              u16
///   parent_ptr       u32
pub trait PageHeader {
    const FORMAT_OFFSET: usize     = 0;
    const IS_ROOT_OFFSET: usize    = Self::FORMAT_OFFSET + BYTE_SIZE;
    const NUM_OFFSET: usize        = Self::IS_ROOT_OFFSET + BYTE_SIZE;
    const PARENT_PTR_OFFSET: usize = Self::NUM_OFFSET + U16_SIZE;
    fn set_format(&mut self, format: u8);
    fn is_leaf_node(&self) -> bool;
    fn is_root_node(&self) -> bool;
    fn set_root_node(&mut self, make_root: bool);
    fn num_tuples(&self) -> usize;
    fn reset_num_tuples(&mut self);
    fn inc_num_tuples(&mut self);
    fn dec_num_tuples(&mut self);
    fn parent_ptr(&self) -> u32;
    fn set_parent_ptr(&mut self, val: u32);
}

impl PageHeader for SlottedPage {
    fn set_format(&mut self, format: u8) {
        self.buf.write_u8_offset(Self::FORMAT_OFFSET, format);
    }

    fn is_leaf_node(&self) -> bool {
        let (_, val) = self.buf.read_u8_offset(Self::FORMAT_OFFSET);
        val == LEAF_NODE
    }

    fn is_root_node(&self) -> bool {
        let (_, val) = self.buf.read_u8_offset(Self::IS_ROOT_OFFSET);
        val == TRUE
    }

    fn set_root_node(&mut self, make_root: bool) {
        self.buf.write_u8_offset(
            Self::IS_ROOT_OFFSET,
            if make_root { TRUE } else { FALSE },
        );
    }

    fn num_tuples(&self) -> usize {
        let (_, val) = self.buf.read_u16_offset(Self::NUM_OFFSET);
        val as usize
    }

    fn reset_num_tuples(&mut self) {
        self.buf.write_u16_offset(Self::NUM_OFFSET, 0);
    }

    fn inc_num_tuples(&mut self) {
        let (_, val) = self.buf.read_u16_offset(Self::NUM_OFFSET);
        let val = val.checked_add(1).expect("Overflow occurred: reached the maximum value for u32");
        self.buf.write_u16_offset(Self::NUM_OFFSET, val);
    }

    fn dec_num_tuples(&mut self) {
        let (_, val) = self.buf.read_u16_offset(Self::NUM_OFFSET);
        let val = val.checked_sub(1).expect("Underflow occurred: cannot subtract from zero");
        self.buf.write_u16_offset(Self::NUM_OFFSET, val);
    }

    fn parent_ptr(&self) -> u32 {
        let (_, val) = self.buf.read_u32_offset(Self::PARENT_PTR_OFFSET);
        val
    }

    fn set_parent_ptr(&mut self, val: u32) {
        self.buf.write_u32_offset(Self::PARENT_PTR_OFFSET, val);
    }
}

/// Internal Node (Slotted Page)
/// begin header
///   format            u8   {0=internal}
///   is_root           u8   {0=false, 1=true}
///   num               u16
///   parent_ptr        u32
/// end common header
///   right_child_ptr   u32
///   free_begin        u16
///   free_end          u16
/// end internal header
///   key_0             u32
///   val_0_offset      u16
///   ...
///   key_n             u32
///   val_n_offset      u16
///   ... <free space> ...
///   ... <free space> ...
///   ... <free space> ...
///   val_0             u32
///   ...
///   val_n             u32
pub trait InternalOps: PageHeader {
    const RIGHT_CHILD_PTR_OFFSET: usize = Self::PARENT_PTR_OFFSET + U32_SIZE;
    const INTERNAL_FREE_BEGIN_PTR_OFFSET: usize = Self::RIGHT_CHILD_PTR_OFFSET + U32_SIZE;
    const INTERNAL_FREE_END_PTR_OFFSET: usize = Self::INTERNAL_FREE_BEGIN_PTR_OFFSET + U16_SIZE;
    const INTERNAL_NODE_HEADER_SIZE: usize =
          BYTE_SIZE //format
        + BYTE_SIZE //is_root
        + U16_SIZE  //num
        + U32_SIZE  //parent_ptr
        + U32_SIZE  //right_child_ptr
        + U16_SIZE  //free_begin
        + U16_SIZE; //free_end
    const INTERNAL_KEY_STRIDE: usize = U32_SIZE + U16_SIZE;
    fn internal_initialize_node(&mut self, make_root: bool);
    fn right_child(&self) -> u32;
    fn set_right_child(&mut self, val: &u32);
    fn internal_find_tuple_index(&self, find_key: &u32) -> usize;
    fn internal_get_key(&self, tuple_index: usize) -> u32;
    fn internal_get_val(&self, tuple_index: usize) -> u32;
    fn internal_free_space(&mut self) -> u16;
    fn internal_put(&mut self, key: &u32, val: &u32, tuple_index: usize) -> bool;
    fn internal_split_into(
        &self,
        pivot_index: usize,
        left: &mut Self,
        right: &mut Self,
        tuple_index: usize,
        key: u32,
        val: &u32,
    );
}

impl InternalOps for SlottedPage {
    fn internal_initialize_node(&mut self, make_root: bool) {
        self.set_format(INTERNAL_NODE);
        self.set_root_node(make_root);
        self.reset_num_tuples();
        self.set_right_child(&NULL_PTR);
        self.buf.write_u16_offset(Self::INTERNAL_FREE_BEGIN_PTR_OFFSET, Self::INTERNAL_NODE_HEADER_SIZE as u16);
        self.buf.write_u16_offset(Self::INTERNAL_FREE_END_PTR_OFFSET, self.page_size as u16);
    }

    fn right_child(&self) -> u32 {
        let (_, val) = self.buf.read_u32_offset(Self::RIGHT_CHILD_PTR_OFFSET);
        val
    }

    fn set_right_child(&mut self, val: &u32) {
        self.buf.write_u32_offset(Self::RIGHT_CHILD_PTR_OFFSET, *val);
    }

    fn internal_find_tuple_index(&self, find_key: &u32) -> usize {
        self.bin_search(find_key, 0, self.num_tuples(), |i| {
            self.internal_get_key(i)
        })
    }

    fn internal_get_key(&self, tuple_index: usize) -> u32 {
        let offset = Self::INTERNAL_NODE_HEADER_SIZE + tuple_index * Self::INTERNAL_KEY_STRIDE;
        let (_, val) = self.buf.read_u32_offset(offset);
        val
    }

    fn internal_get_val(&self, tuple_index: usize) -> u32 {
        let (_, val_offset) = self.buf.read_u16_offset(Self::INTERNAL_NODE_HEADER_SIZE + tuple_index * Self::INTERNAL_KEY_STRIDE + U32_SIZE);
        let (_, val) = self.buf.read_u32_offset(val_offset as usize);
        val
    }

    fn internal_free_space(&mut self) -> u16 {
        let (_, begin_offset) = self.buf.read_u16_offset(Self::INTERNAL_FREE_BEGIN_PTR_OFFSET);
        let (_, end_offset) = self.buf.read_u16_offset(Self::INTERNAL_FREE_END_PTR_OFFSET);
        end_offset - begin_offset
    }

    fn internal_put(&mut self, key: &u32, val: &u32, tuple_index: usize) -> bool {
        let (_, begin_offset) = self.buf.read_u16_offset(Self::INTERNAL_FREE_BEGIN_PTR_OFFSET);
        let (_, end_offset) = self.buf.read_u16_offset(Self::INTERNAL_FREE_END_PTR_OFFSET);
        let free_space = end_offset - begin_offset;
        let val_len = U32_SIZE;
        let space_needed = u16::try_from(Self::INTERNAL_KEY_STRIDE + val_len).unwrap();
        if space_needed > free_space {
            return false
        }

        let num = self.num_tuples();

        if let Some(cap_limit) = self.cap_limit {
            if num == cap_limit {
                // println!("DEBUG: cap limit ({}) reached for internal node.", cap_limit);
                return false
            }
        }

        // shift all the keys to the right by 1
        for i in (tuple_index..num).rev() {
            let src_offset = Self::INTERNAL_NODE_HEADER_SIZE + i * Self::INTERNAL_KEY_STRIDE;
            let dst_offset = Self::INTERNAL_NODE_HEADER_SIZE + (i + 1) * Self::INTERNAL_KEY_STRIDE;
            self.buf.get_mut().copy_within(
                src_offset..src_offset + Self::INTERNAL_KEY_STRIDE,
                dst_offset
            );
        }

        // insert key at offset
        let offset = self.buf.write_u32_offset(Self::INTERNAL_NODE_HEADER_SIZE + tuple_index * Self::INTERNAL_KEY_STRIDE, *key);
        let val_offset = end_offset - u16::try_from(val_len).unwrap();
        self.buf.write_u16_offset(offset, val_offset);

        // insert val
        self.buf.write_u32_offset(val_offset as usize, *val);
        self.inc_num_tuples();

        // update free begin and end offsets
        self.buf.write_u16_offset(Self::INTERNAL_FREE_BEGIN_PTR_OFFSET, begin_offset + u16::try_from(Self::INTERNAL_KEY_STRIDE).unwrap());
        self.buf.write_u16_offset(Self::INTERNAL_FREE_END_PTR_OFFSET, val_offset);

        true
    }

    /// Assumes left and right were initialized already
    fn internal_split_into(
        &self,
        mut pivot_index: usize,
        left: &mut Self,
        right: &mut Self,
        tuple_index: usize,
        key: u32,
        val: &u32,
    ) {
        let num = self.num_tuples();

        assert!(num > 1);
        assert!(pivot_index < num);

        if self.split_strategy == SplitStrategy::EmptyPage {
            // left will be full and right will be empty
            pivot_index = num;

            assert!(right.internal_put(&key, val, 0));
        }

        {
            let mut shift = 0;
            for i in 0..pivot_index {
                if i == tuple_index {
                    shift = 1;
                    assert!(left.internal_put(&key, val, i));
                }

                let key = self.internal_get_key(i);
                let val = self.internal_get_val(i);
                assert!(left.internal_put(&key, &val, i + shift));
            }
        }

        {
            let mut shift = 0;
            for i in pivot_index..num {
                if i == tuple_index {
                    shift = 1;
                    assert!(right.internal_put(&key, val, i - pivot_index));
                }

                let key = self.internal_get_key(i);
                let val = self.internal_get_val(i);
                assert!(right.internal_put(&key, &val, (i + shift) - pivot_index));
            }
        }
    }
}

/// Leaf Node (Slotted Page)
/// begin header
///   format            u8   {0=internal, 1=leaf}
///   is_root           u8   {0=false, 1=true}
///   num               u16
///   parent_ptr        u32
/// end common header
///   r_sibling_ptr     u32
///   l_sibling_ptr     u32
///   free_begin        u16
///   free_end          u16
/// end leaf header
///   key_0             u32
///   val_0_offset      u16
///   val_0_size        u16
///   ...
///   key_n             u32
///   val_n_offset      u16
///   val_n_size        u16
///   ... <free space> ...
///   ... <free space> ...
///   ... <free space> ...
///   val_0             <tuple data>
///   ...
///   val_n             <tuple data>
pub trait LeafOps: PageHeader {
    const R_SIBLING_PTR_OFFSET: usize = Self::PARENT_PTR_OFFSET + U32_SIZE;
    const L_SIBLING_PTR_OFFSET: usize = Self::R_SIBLING_PTR_OFFSET + U32_SIZE;
    const LEAF_FREE_BEGIN_PTR_OFFSET: usize = Self::L_SIBLING_PTR_OFFSET + U32_SIZE;
    const LEAF_FREE_END_PTR_OFFSET: usize = Self::LEAF_FREE_BEGIN_PTR_OFFSET + U16_SIZE;
    const LEAF_NODE_HEADER_SIZE: usize =
          BYTE_SIZE //format
        + BYTE_SIZE //is_root
        + U16_SIZE  //num
        + U32_SIZE  //parent_ptr
        + U32_SIZE  //r_sibling_ptr
        + U32_SIZE  //l_sibling_ptr
        + U16_SIZE  //free_begin
        + U16_SIZE; //free_end
    const LEAF_KEY_STRIDE: usize = U32_SIZE + U16_SIZE + U16_SIZE;
    fn leaf_initialize_node(&mut self, make_root: bool);
    fn r_sibling(&self) -> u32;
    fn set_r_sibling(&mut self, val: u32);
    fn l_sibling(&self) -> u32;
    fn set_l_sibling(&mut self, val: u32);
    fn leaf_find_tuple_index(&self, find_key: &u32) -> usize;
    fn leaf_get_key(&self, tuple_index: usize) -> u32;
    fn leaf_get_val(&self, tuple_index: usize) -> Tuple;
    fn leaf_free_space(&mut self) -> u16;
    fn leaf_put(
        &mut self,
        key: &u32,
        val: &Tuple,
        tuple_index: usize,
    ) -> Result<bool, errors::RecallError>;
    fn leaf_split_into(
        &self,
        pivot_index: usize,
        left: &mut Self,
        right: &mut Self,
        tuple_index: usize,
        key: u32,
        val: &Tuple,
    ) -> Result<(), errors::RecallError>;
    fn min_key(&self) -> Option<u32>;
    fn max_key(&self) -> Option<u32>;
}

impl LeafOps for SlottedPage {
    fn leaf_initialize_node(&mut self, make_root: bool) {
        self.set_format(LEAF_NODE);
        self.set_root_node(make_root);
        self.reset_num_tuples();
        self.set_l_sibling(NULL_PTR);
        self.set_r_sibling(NULL_PTR);
        self.buf.write_u16_offset(Self::LEAF_FREE_BEGIN_PTR_OFFSET, Self::LEAF_NODE_HEADER_SIZE as u16);
        self.buf.write_u16_offset(Self::LEAF_FREE_END_PTR_OFFSET, self.page_size as u16);
    }

    fn r_sibling(&self) -> u32 {
        let (_, val) = self.buf.read_u32_offset(Self::R_SIBLING_PTR_OFFSET);
        val
    }

    fn set_r_sibling(&mut self, val: u32) {
        self.buf.write_u32_offset(Self::R_SIBLING_PTR_OFFSET, val);
    }

    fn l_sibling(&self) -> u32 {
        let (_, val) = self.buf.read_u32_offset(Self::L_SIBLING_PTR_OFFSET);
        val
    }

    fn set_l_sibling(&mut self, val: u32) {
        self.buf.write_u32_offset(Self::L_SIBLING_PTR_OFFSET, val);
    }

    fn leaf_find_tuple_index(&self, find_key: &u32) -> usize {
        self.bin_search(find_key, 0, self.num_tuples(), |i| {
            self.leaf_get_key(i)
        })
    }

    fn leaf_get_key(&self, tuple_index: usize) -> u32 {
        let offset = Self::LEAF_NODE_HEADER_SIZE + tuple_index * Self::LEAF_KEY_STRIDE;
        let (_, val) = self.buf.read_u32_offset(offset);
        val
    }

    fn leaf_get_val(&self, tuple_index: usize) -> Tuple {
        unsafe {
            let (offset, val_offset) = self.buf.read_u16_offset(Self::LEAF_NODE_HEADER_SIZE + tuple_index * Self::LEAF_KEY_STRIDE + U32_SIZE);
            let (_, val_size) = self.buf.read_u16_offset(offset);

            let ptr = self.buf.get().as_ptr().add(val_offset as usize);
            let mut tuple = Tuple::new(val_size as usize);

            ptr::copy_nonoverlapping(
                ptr,
                tuple.get_mut().as_mut_ptr(),
                val_size as usize,
            );

            tuple
        }
    }

    fn leaf_free_space(&mut self) -> u16 {
        let (_, begin_offset) = self.buf.read_u16_offset(Self::LEAF_FREE_BEGIN_PTR_OFFSET);
        let (_, end_offset) = self.buf.read_u16_offset(Self::LEAF_FREE_END_PTR_OFFSET);
        end_offset - begin_offset
    }

    fn leaf_put(&mut self, key: &u32, val: &Tuple, tuple_index: usize) -> Result<bool, errors::RecallError> {
        let num = self.num_tuples();

        // check that key does not already exists
        //
        // This needs to be keep as the first check since before splitting
        // we attempt to call the method. The splitting routine does not check
        // that the key exists before inserting, so when refactoring we may need
        // to check for key unique error in the splitting routine.
        if tuple_index < num {
            let stored_key = self.leaf_get_key(tuple_index);
            if *key == stored_key {
                return Err(errors::RecallError::UniqueKeyError(*key))
            }
        }

        // check space requirements
        let (_, begin_offset) = self.buf.read_u16_offset(Self::LEAF_FREE_BEGIN_PTR_OFFSET);
        let (_, end_offset) = self.buf.read_u16_offset(Self::LEAF_FREE_END_PTR_OFFSET);
        let free_space = end_offset - begin_offset;
        let val_len = val.get().len();
        let space_needed = u16::try_from(Self::LEAF_KEY_STRIDE + val_len).unwrap();
        if space_needed > free_space {
            return Ok(false)
        }

        // check cap limit
        if let Some(cap_limit) = self.cap_limit {
            if num == cap_limit {
                // println!("DEBUG: cap limit ({}) reached for leaf node.", cap_limit);
                return Ok(false)
            }
        }

        // shift all the keys to the right by 1
        for i in (tuple_index..num).rev() {
            let src_offset = Self::LEAF_NODE_HEADER_SIZE + i * Self::LEAF_KEY_STRIDE;
            let dst_offset = Self::LEAF_NODE_HEADER_SIZE + (i + 1) * Self::LEAF_KEY_STRIDE;
            self.buf.get_mut().copy_within(
                src_offset..src_offset + Self::LEAF_KEY_STRIDE,
                dst_offset
            );
        }

        // insert key at offset
        let offset = self.buf.write_u32_offset(Self::LEAF_NODE_HEADER_SIZE + tuple_index * Self::LEAF_KEY_STRIDE, *key);
        let val_offset = end_offset - u16::try_from(val_len).unwrap();
        let offset = self.buf.write_u16_offset(offset, val_offset);
        let val_size = u16::try_from(val_len).unwrap();
        self.buf.write_u16_offset(offset, val_size);

        // insert val
        unsafe {
            let ptr = self.buf.get_mut().as_mut_ptr().add(val_offset as usize);
            ptr::copy_nonoverlapping(val.get().as_ptr(), ptr, val_len);
        }
        self.inc_num_tuples();

        // update free begin and end offsets
        self.buf.write_u16_offset(Self::LEAF_FREE_BEGIN_PTR_OFFSET, begin_offset + u16::try_from(Self::LEAF_KEY_STRIDE).unwrap());
        self.buf.write_u16_offset(Self::LEAF_FREE_END_PTR_OFFSET, val_offset);

        Ok(true)
    }

    /// Assumes left and right were initialized already
    fn leaf_split_into(
        &self,
        mut pivot_index: usize,
        left: &mut Self,
        right: &mut Self,
        tuple_index: usize,
        key: u32,
        val: &Tuple,
    ) -> Result<(), errors::RecallError> {
        let num = self.num_tuples();

        assert!(num > 1);
        assert!(pivot_index < num);

        if self.split_strategy == SplitStrategy::EmptyPage {
            // left will be full and right will be empty
            pivot_index = num;

            assert!(right.leaf_put(&key, &val, 0)?);
        }

        {
            let mut shift = 0;
            for i in 0..pivot_index {
                if i == tuple_index {
                    shift = 1;
                    assert!(left.leaf_put(&key, val, i)?);
                }

                let key = self.leaf_get_key(i);
                let val = self.leaf_get_val(i);
                assert!(left.leaf_put(&key, &val, i + shift)?);
            }
        }

        {
            let mut shift = 0;
            for i in pivot_index..num {
                if i == tuple_index {
                    shift = 1;
                    assert!(right.leaf_put(&key, val, i - pivot_index)?);
                }

                let key = self.leaf_get_key(i);
                let val = self.leaf_get_val(i);
                assert!(right.leaf_put(&key, &val, (i + shift) - pivot_index)?);
            }
        }

        Ok(())
    }

    fn min_key(&self) -> Option<u32> {
        let num = self.num_tuples();

        if num > 0 {
            return Some(self.leaf_get_key(0))
        }
        None
    }

    fn max_key(&self) -> Option<u32> {
        let num = self.num_tuples();

        if num > 0 {
            return Some(self.leaf_get_key(num - 1))
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_tests_bin_search() {
        let mut page =
            SlottedPageBuilder::new()
            .build();

        page.internal_initialize_node(true);

        // keys: 1, 3, 6, 12, 69
        let keys = vec![6, 1, 3, 69, 12];

        // put tuples
        for key in keys.iter() {
            let tuple_index: usize = page.internal_find_tuple_index(key);
            assert!(page.internal_put(key, &42, tuple_index))
        }

        // assert that the correct index was found for each find key
        for (key, want_index) in vec![
            (5, 2usize),
            (7, 3),
            (15, 4),
            (12, 3),
            (6, 2),
            (69, 4),
            (77, 5),
            (0, 0),
            (2, 1),
        ] {
            let got_index = page.bin_search(&key, 0, page.num_tuples(), |i| {
                page.internal_get_key(i)
            });
            assert!(got_index == want_index);
        }
    }

    #[test]
    fn it_initializes_slotted_leaf_page() {
        let mut page =
            SlottedPageBuilder::new()
            .build();
        page.leaf_initialize_node(true);
        assert!(page.is_leaf_node());
        assert!(page.is_root_node());
        assert!(page.num_tuples() == 0);
        assert!(page.parent_ptr() == NULL_PTR);
        assert!(page.r_sibling() == NULL_PTR);
        assert!(page.l_sibling() == NULL_PTR);
    }

    #[test]
    fn it_initializes_slotted_internal_page() {
        let mut page =
            SlottedPageBuilder::new()
            .build();
        page.internal_initialize_node(true);
        assert!(!page.is_leaf_node());
        assert!(page.is_root_node());
        assert!(page.num_tuples() == 0);
        assert!(page.parent_ptr() == NULL_PTR);
        assert!(page.right_child() == NULL_PTR);
    }

    #[test]
    fn it_can_get_and_put_tuples_internal_page() {
        let mut page =
            SlottedPageBuilder::new()
            .build();
        page.internal_initialize_node(true);

        let pairs = vec![(6, 100), (0, 101), (3, 102), (69, 103), (12, 104)];
        let right_child_val = 105;

        // put tuples
        for (key, val) in pairs.iter() {
            let tuple_index: usize = page.internal_find_tuple_index(key);
            assert!(page.internal_put(key, val, tuple_index))
        }
        page.set_right_child(&right_child_val);

        assert!(page.num_tuples() == pairs.len());

        // keys are correctly ordered
        let mut ordered_keys: Vec<u32> = pairs.iter().map(|pair| { pair.0 }).collect();
        ordered_keys.sort();

        assert!(
            (0..page.num_tuples())
            .map(|tuple_index| {
                page.internal_get_key(tuple_index)
            })
            .collect::<Vec<_>>() == ordered_keys
        );

        // get tuples
        for (key, val) in pairs.iter() {
            let tuple_index: usize = page.internal_find_tuple_index(key);
            let stored_key = page.internal_get_key(tuple_index);
            let stored_val = page.internal_get_val(tuple_index);
            assert!(&stored_key == key);
            assert!(&stored_val == val);
        }
        assert!(page.right_child() == right_child_val);
    }

    #[test]
    fn it_can_get_and_put_tuples_leaf_page() {
        let mut page =
            SlottedPageBuilder::new()
            .build();
        page.leaf_initialize_node(true);

        fn build_tuple(key: u32) -> Tuple {
            let mut size = 0;

            // tuple schema: (Id: uint, Note: string, N: int, Atom: atom)
            let id = key + 1000;
            size += SizedBuf::uint_storage_size();
            let note = format!("My key is {}", key);
            size += SizedBuf::string_storage_size(note.len());
            let n = -42;
            size += SizedBuf::int_storage_size();
            let sym = "foo";
            size += SizedBuf::atom_storage_size(sym.len());

            let mut tuple = Tuple::new(size);
            let offset = tuple.write_u32_offset(0, id);
            let offset = tuple.write_string_offset(offset, &note);
            let offset = tuple.write_i32_offset(offset, n);
            tuple.write_atom_offset(offset, &sym);
            tuple
        }

        let pairs: Vec<(u32, Tuple)> =
            vec![6, 0, 3, 69, 12]
            .into_iter()
            .map(|key| {
                (key, build_tuple(key))
            })
            .collect();

        // put tuples
        for (key, val) in pairs.iter() {
            let tuple_index: usize = page.leaf_find_tuple_index(key);
            assert!(page.leaf_put(key, val, tuple_index).unwrap());
        }

        assert!(page.num_tuples() == pairs.len());

        // keys are correctly ordered
        let mut ordered_keys: Vec<u32> = pairs.iter().map(|pair| { pair.0 }).collect();
        ordered_keys.sort();

        assert!(
            (0..page.num_tuples())
            .map(|tuple_index| {
                page.leaf_get_key(tuple_index)
            })
            .collect::<Vec<_>>() == ordered_keys
        );

        // get tuples
        for (key, val) in pairs.iter() {
            let tuple_index: usize = page.leaf_find_tuple_index(key);
            let stored_key = page.leaf_get_key(tuple_index);
            let stored_val = page.leaf_get_val(tuple_index);
            assert!(&stored_key == key);
            assert!(&stored_val == val);
        }
    }

    #[test]
    fn it_will_return_uniq_key_error_on_existing_key() {
        let mut page =
            SlottedPageBuilder::new()
            .build();
        page.leaf_initialize_node(false);

        // put tuples
        // [0 3 6 12 69], insert 69
        let insert_key = 69;
        let keys = vec![6, 0, 3, 69, 12];

        for key in keys.iter() {
            let tuple_index: usize = page.leaf_find_tuple_index(key);
            let tuple = {
                let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
                tuple.write_u32_offset(0, *key);
                tuple
            };
            assert!(page.leaf_put(key, &tuple, tuple_index).unwrap());
        }

        let tuple_index: usize = page.leaf_find_tuple_index(&insert_key);
        let tuple = {
            let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
            tuple.write_u32_offset(0, insert_key);
            tuple
        };
        let result = page.leaf_put(&insert_key, &tuple, tuple_index);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), errors::RecallError::UniqueKeyError(insert_key));
    }

    #[test]
    fn it_can_split_tuples_internal_half_strategy() {
        let mut page =
            SlottedPageBuilder::new()
            .build();
        page.internal_initialize_node(true);

        // put tuples
        // [0 3 6 12 69], insert 42
        let insert_key = 42;
        let insert_val = 105;
        let pairs = vec![
            (6, 100),
            (0, 101),
            (3, 102),
            (69, 103),
            (12, 104),
        ];
        let left_pairs_ordered = vec![
            (0, 101),
            (3, 102),
        ];
        let right_pairs_ordered = vec![
            (6, 100),
            (12, 104),
            (insert_key, insert_val),
            (69, 103),
        ];

        for (key, val) in pairs.iter() {
            let tuple_index: usize = page.internal_find_tuple_index(key);
            assert!(page.internal_put(key, val, tuple_index))
        }

        // initialize left & right pages
        let mut left =
            SlottedPageBuilder::new()
            .build();
        left.internal_initialize_node(false);

        let mut right =
            SlottedPageBuilder::new()
            .build();
        right.internal_initialize_node(false);

        let pivot_index = pairs.len() / 2;
        let tuple_index: usize = page.internal_find_tuple_index(&insert_key);
        page.internal_split_into(
            pivot_index,
            &mut left,
            &mut right,
            tuple_index,
            insert_key,
            &insert_val,
        );

        // re-initialize page
        page.internal_initialize_node(true);
        assert!(!page.is_leaf_node());
        assert!(page.is_root_node());
        assert!(page.num_tuples() == 0);
        assert!(page.parent_ptr() == NULL_PTR);
        assert!(page.right_child() == NULL_PTR);
        let max_free_space = page.page_size - SlottedPage::INTERNAL_NODE_HEADER_SIZE;
        assert!(page.internal_free_space() == max_free_space as u16);

        // check left page
        assert!(left.num_tuples() == pivot_index);
        assert!(
            (0..left.num_tuples())
            .into_iter()
            .map(|tuple_index| {
                let key = left.internal_get_key(tuple_index);
                let val = left.internal_get_val(tuple_index);
                (key, val)
            })
            .collect::<Vec<(_, _)>>() == left_pairs_ordered
        );

        // check right page
        assert!(right.num_tuples() == pairs.len() - pivot_index + 1); // +1 for insert key
        assert!(
            (0..right.num_tuples())
            .into_iter()
            .map(|tuple_index| {
                let key = right.internal_get_key(tuple_index);
                let val = right.internal_get_val(tuple_index);
                (key, val)
            })
            .collect::<Vec<(_, _)>>() == right_pairs_ordered
        );
    }

    #[test]
    fn it_can_split_tuples_leaf_half_strategy() {
        let mut page =
            SlottedPageBuilder::new()
            .build();
        page.leaf_initialize_node(false);

        // put tuples
        // [0 3 6 12 69], insert 42
        let insert_key = 42;
        let keys = vec![6, 0, 3, 69, 12];
        let left_keys_ordered = vec![0, 3];
        let right_keys_ordered = vec![6, 12, insert_key, 69];

        for key in keys.iter() {
            let tuple_index: usize = page.leaf_find_tuple_index(key);
            let tuple = {
                let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
                tuple.write_u32_offset(0, *key);
                tuple
            };
            assert!(page.leaf_put(key, &tuple, tuple_index).unwrap());
        }

        // initialize left & right pages
        let mut left =
            SlottedPageBuilder::new()
            .build();
        left.leaf_initialize_node(false);

        let mut right =
            SlottedPageBuilder::new()
            .build();
        right.leaf_initialize_node(false);

        let pivot_index = keys.len() / 2;
        let tuple_index = page.leaf_find_tuple_index(&insert_key);
        let tuple = {
            let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
            tuple.write_u32_offset(0, insert_key);
            tuple
        };
        page.leaf_split_into(
            pivot_index,
            &mut left,
            &mut right,
            tuple_index,
            insert_key,
            &tuple,
        ).unwrap();

        // re-initialize page
        page.leaf_initialize_node(false);
        assert!(page.is_leaf_node());
        assert!(!page.is_root_node());
        assert!(page.num_tuples() == 0);
        assert!(page.parent_ptr() == NULL_PTR);
        assert!(page.l_sibling() == NULL_PTR);
        assert!(page.r_sibling() == NULL_PTR);
        let max_free_space = page.page_size - SlottedPage::LEAF_NODE_HEADER_SIZE;
        assert!(page.leaf_free_space() == max_free_space as u16);

        // check left page
        assert!(left.num_tuples() == pivot_index);
        assert!(
            (0..left.num_tuples())
            .into_iter()
            .map(|tuple_index| {
                let key = left.leaf_get_key(tuple_index);
                let val = left.leaf_get_val(tuple_index);
                let (_, val) = val.read_u32_offset(0);
                assert_eq!(key, val);
                key
            })
            .collect::<Vec<_>>() == left_keys_ordered
        );

        // check right page
        assert!(right.num_tuples() == keys.len() - pivot_index + 1); // +1 for insert key
        assert!(
            (0..right.num_tuples())
            .into_iter()
            .map(|tuple_index| {
                let key = right.leaf_get_key(tuple_index);
                let val = right.leaf_get_val(tuple_index);
                let (_, val) = val.read_u32_offset(0);
                assert_eq!(key, val);
                key
            })
            .collect::<Vec<_>>() == right_keys_ordered
        );
    }

    #[test]
    fn it_will_return_false_on_cap_limit_leaf() {
        let mut page =
            SlottedPageBuilder::new()
            .cap_limit(Some(5))
            .build();
        page.leaf_initialize_node(true);

        // put tuples
        // [0 3 6 12 69], insert 42
        let insert_key = 42;
        let keys = vec![6, 0, 3, 69, 12];

        for key in keys.iter() {
            let tuple_index: usize = page.leaf_find_tuple_index(key);
            let tuple = {
                let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
                tuple.write_u32_offset(0, *key);
                tuple
            };
            assert!(page.leaf_put(key, &tuple, tuple_index).unwrap());
        }

        let tuple = {
            let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
            tuple.write_u32_offset(0, insert_key);
            tuple
        };
        let tuple_index: usize = page.leaf_find_tuple_index(&insert_key);
        assert!(!page.leaf_put(&insert_key, &tuple, tuple_index).unwrap());
    }

    #[test]
    fn it_will_return_false_on_cap_limit_internal() {
        let mut page =
            SlottedPageBuilder::new()
            .cap_limit(Some(5))
            .build();
        page.internal_initialize_node(true);

        // put tuples
        // [0 3 6 12 69], insert 42
        let insert_key = 42;
        let right_child_val = 75;
        let keys = vec![6, 0, 3, 69, 12];

        for key in keys.iter() {
            let tuple_index: usize = page.internal_find_tuple_index(key);
            assert!(page.internal_put(key, key, tuple_index))
        }
        page.set_right_child(&right_child_val);

        let tuple_index: usize = page.internal_find_tuple_index(&insert_key);
        assert!(!page.internal_put(&insert_key, &insert_key, tuple_index))
    }

    #[test]
    fn it_can_split_tuples_leaf_empty_strategy() {
        let mut page =
            SlottedPageBuilder::new()
            .split_strategy_empty_page()
            .build();
        page.leaf_initialize_node(false);

        // put tuples
        // [0 3 6 12 69], insert 75
        let insert_key = 75;
        let keys = vec![6, 0, 3, 69, 12];
        let left_keys_ordered = vec![0, 3, 6, 12, 69];
        let right_keys_ordered = vec![insert_key];

        for key in keys.iter() {
            let tuple_index: usize = page.leaf_find_tuple_index(key);
            let tuple = {
                let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
                tuple.write_u32_offset(0, *key);
                tuple
            };
            assert!(page.leaf_put(key, &tuple, tuple_index).unwrap());
        }

        // initialize left & right pages
        let mut left =
            SlottedPageBuilder::new()
            .build();
        left.leaf_initialize_node(false);

        let mut right =
            SlottedPageBuilder::new()
            .build();
        right.leaf_initialize_node(false);

        let pivot_index = keys.len() / 2;
        let tuple_index = page.leaf_find_tuple_index(&insert_key);
        let tuple = {
            let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
            tuple.write_u32_offset(0, insert_key);
            tuple
        };
        page.leaf_split_into(
            pivot_index,
            &mut left,
            &mut right,
            tuple_index,
            insert_key,
            &tuple,
        ).unwrap();

        // check left page
        assert!(left.num_tuples() == keys.len());
        assert!(
            (0..left.num_tuples())
            .into_iter()
            .map(|tuple_index| {
                let key = left.leaf_get_key(tuple_index);
                let val = left.leaf_get_val(tuple_index);
                let (_, val) = val.read_u32_offset(0);
                assert_eq!(key, val);
                key
            })
            .collect::<Vec<_>>() == left_keys_ordered
        );

        // check right page
        assert!(right.num_tuples() == 1);
        assert!(
            (0..right.num_tuples())
            .into_iter()
            .map(|tuple_index| {
                let key = right.leaf_get_key(tuple_index);
                let val = right.leaf_get_val(tuple_index);
                let (_, val) = val.read_u32_offset(0);
                assert_eq!(key, val);
                key
            })
            .collect::<Vec<_>>() == right_keys_ordered
        );
    }
}
