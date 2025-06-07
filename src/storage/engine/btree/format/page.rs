use std::ptr;

use super::U16_SIZE;
use super::U32_SIZE;
use super::BYTE_SIZE;
use super::LEAF_NODE;
use super::{TRUE, FALSE};
use super::INTERNAL_NODE;
use super::NULL_PTR;

use crate::errors;
use super::tuple::Tuple;
use super::tuple::ParameterType;
use super::sizedbuf::SizedBuf;

pub type KeyVal = Vec<ParameterType>;
pub type TupVal = Vec<ParameterType>;

pub struct SlottedPage {
    buf: SizedBuf,
    page_size: usize,
    cap_limit: Option<usize>,
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum SplitStrategy {
    EmptyPage,
    HalfFullPage,
}

impl SlottedPage {
    pub fn new(page_size: usize, cap_limit: Option<usize>) -> Self {
        let buf = SizedBuf::new(page_size);

        SlottedPage {
            buf,
            page_size,
            cap_limit,
         }
    }

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
        key: &KeyVal,
        begin_index: usize,
        end_index: usize,
        get_stored_key: F,
    ) -> usize
    where
        F: Fn(usize) -> KeyVal
    {
        if begin_index >= end_index {
            return begin_index
        }
        let i = (begin_index + end_index) / 2;
        let stored_key = get_stored_key(i);
        if *key == stored_key {
            i
        } else if *key < stored_key {
            self.bin_search(key, begin_index, i, get_stored_key)
        } else { /* key > stored_key */
            self.bin_search(key, i + 1, end_index, get_stored_key)
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
///   key_scm           bytes
/// end internal header
///                            __| stride
///   key_0_offset      u16      |
///   key_0_size        u16      |
///   val_0_offset      u16    __|
///   ...
///   key_n_offset      u16
///   key_n_size        u16
///   val_n_offset      u16
///   ... <free space> ...
///   ... <free space> ...
///   ... <free space> ...
///   key_0             tuple
///   val_0             u32
///   ...
///   key_n             tuple
///   val_n             u32
pub trait InternalOps: PageHeader {
    const RIGHT_CHILD_PTR_OFFSET: usize = Self::PARENT_PTR_OFFSET + U32_SIZE;
    const INTERNAL_FREE_BEGIN_PTR_OFFSET: usize = Self::RIGHT_CHILD_PTR_OFFSET + U32_SIZE;
    const INTERNAL_FREE_END_PTR_OFFSET: usize = Self::INTERNAL_FREE_BEGIN_PTR_OFFSET + U16_SIZE;
    const INTERNAL_KEY_SCM_OFFSET: usize = Self::INTERNAL_FREE_END_PTR_OFFSET + U16_SIZE;
    const INTERNAL_KEY_STRIDE: usize = U16_SIZE * 3;
    fn internal_header_size(key_scm: &[u8]) -> usize;
    fn internal_initialize_node(&mut self, make_root: bool, key_scm: &[u8]);
    fn right_child(&self) -> u32;
    fn set_right_child(&mut self, val: u32);
    fn internal_key_scm(&self) -> Vec<u8>;
    fn internal_find_tuple_index(&self, key: &KeyVal) -> usize;
    fn internal_get_key(&self, tuple_index: usize) -> KeyVal;
    fn internal_get_val(&self, tuple_index: usize) -> u32;
    fn internal_free_space(&mut self) -> u16;
    fn internal_put(
        &mut self,
        key: &KeyVal,
        val: u32,
        tuple_index: usize,
    ) -> Result<bool, errors::RecallError>;
    fn internal_split_into(
        &self,
        pivot_index: usize,
        left: &mut Self,
        right: &mut Self,
        tuple_index: usize,
        key: &KeyVal,
        val: u32,
        split_strategy: SplitStrategy,
    ) -> Result<(), errors::RecallError>;
}

impl InternalOps for SlottedPage {
    fn internal_header_size(key_scm: &[u8]) -> usize {
        BYTE_SIZE   //format
        + BYTE_SIZE //is_root
        + U16_SIZE  //num
        + U32_SIZE  //parent_ptr
        + U32_SIZE  //right_child_ptr
        + U16_SIZE  //free_begin
        + U16_SIZE  //free_end
        + SizedBuf::bytes_storage_size(key_scm.len())
    }

    fn internal_initialize_node(&mut self, make_root: bool, key_scm: &[u8]) {
        self.set_format(INTERNAL_NODE);
        self.set_root_node(make_root);
        self.reset_num_tuples();
        self.set_right_child(NULL_PTR);
        self.buf.write_u16_offset(Self::INTERNAL_FREE_BEGIN_PTR_OFFSET, Self::internal_header_size(key_scm) as u16);
        self.buf.write_u16_offset(Self::INTERNAL_FREE_END_PTR_OFFSET, self.page_size as u16);
        self.buf.write_bytes_offset(Self::INTERNAL_KEY_SCM_OFFSET, key_scm);
    }

    fn right_child(&self) -> u32 {
        let (_, val) = self.buf.read_u32_offset(Self::RIGHT_CHILD_PTR_OFFSET);
        val
    }

    fn set_right_child(&mut self, val: u32) {
        self.buf.write_u32_offset(Self::RIGHT_CHILD_PTR_OFFSET, val);
    }

    fn internal_key_scm(&self) -> Vec<u8> {
        let (_, val) = self.buf.read_bytes_offset(Self::INTERNAL_KEY_SCM_OFFSET);
        val
    }

    fn internal_find_tuple_index(&self, key: &KeyVal) -> usize {
        self.bin_search(key, 0, self.num_tuples(), |i| {
            self.internal_get_key(i)
        })
    }

    fn internal_get_key(&self, tuple_index: usize) -> KeyVal {
        let key_scm = &self.internal_key_scm();
        let header_size = Self::internal_header_size(key_scm);
        let offset = header_size + tuple_index * Self::INTERNAL_KEY_STRIDE;
        let (offset, key_offset) = self.buf.read_u16_offset(offset);
        let (_, key_size) = self.buf.read_u16_offset(offset);

        // safe slice copy
        let buf = self.buf.get();
        let start = key_offset as usize;
        let end = start + key_size as usize;
        let src = &buf[start..end];

        let mut tup = Tuple::new(key_size as usize);
        tup.get_mut().copy_from_slice(src);

        tup.decode(key_scm)
    }

    fn internal_get_val(&self, tuple_index: usize) -> u32 {
        let header_size = Self::internal_header_size(&self.internal_key_scm());
        let offset = header_size + tuple_index * Self::INTERNAL_KEY_STRIDE + U16_SIZE * 2;
        let (_, val_offset) = self.buf.read_u16_offset(offset);
        let (_, val) = self.buf.read_u32_offset(val_offset as usize);
        val
    }

    fn internal_free_space(&mut self) -> u16 {
        let (_, begin_offset) = self.buf.read_u16_offset(Self::INTERNAL_FREE_BEGIN_PTR_OFFSET);
        let (_, end_offset) = self.buf.read_u16_offset(Self::INTERNAL_FREE_END_PTR_OFFSET);
        end_offset - begin_offset
    }

    fn internal_put(&mut self, key: &KeyVal, val: u32, tuple_index: usize) -> Result<bool, errors::RecallError> {
        let key_scm = &self.internal_key_scm();
        ParameterType::typecheck(key, key_scm)?;

        let header_size = Self::internal_header_size(key_scm);

        // check space requirements
        let (_, begin_offset) = self.buf.read_u16_offset(Self::INTERNAL_FREE_BEGIN_PTR_OFFSET);
        let (_, end_offset) = self.buf.read_u16_offset(Self::INTERNAL_FREE_END_PTR_OFFSET);
        let free_space = end_offset - begin_offset;
        let key = Tuple::encode(key, key_scm).unwrap();
        let key_size = key.len();
        let val_size = U32_SIZE;
        let space_needed = u16::try_from(Self::INTERNAL_KEY_STRIDE + key_size + val_size).unwrap();
        if space_needed > free_space {
            return Ok(false)
        }

        let num = self.num_tuples();

        if let Some(cap_limit) = self.cap_limit {
            if num == cap_limit {
                // println!("DEBUG: cap limit ({}) reached for internal node.", cap_limit);
                return Ok(false)
            }
        }

        // shift all the offsets to the right by 1
        for i in (tuple_index..num).rev() {
            let src_offset = header_size + i * Self::INTERNAL_KEY_STRIDE;
            let dst_offset = header_size + (i + 1) * Self::INTERNAL_KEY_STRIDE;
            self.buf.get_mut().copy_within(
                src_offset..src_offset + Self::INTERNAL_KEY_STRIDE,
                dst_offset
            );
        }

        // insert offsets for key/val pair
        let offset = header_size + tuple_index * Self::INTERNAL_KEY_STRIDE;
        let key_offset = end_offset - u16::try_from(key_size + val_size).unwrap();
        let offset = self.buf.write_u16_offset(offset, key_offset);
        let offset = self.buf.write_u16_offset(offset, u16::try_from(key_size).unwrap());
        let val_offset = end_offset - u16::try_from(val_size).unwrap();
        self.buf.write_u16_offset(offset, val_offset);

        // insert key
        let key_bytes = key.get();
        let key_dst_start = key_offset as usize;
        let key_dst = &mut self.buf.get_mut()[key_dst_start .. key_dst_start + key_size];
        key_dst.copy_from_slice(key_bytes);

        // insert val
        self.buf.write_u32_offset(val_offset as usize, val);

        self.inc_num_tuples();

        // update free begin and end offsets
        self.buf.write_u16_offset(Self::INTERNAL_FREE_BEGIN_PTR_OFFSET, begin_offset + u16::try_from(Self::INTERNAL_KEY_STRIDE).unwrap());
        self.buf.write_u16_offset(Self::INTERNAL_FREE_END_PTR_OFFSET, key_offset);

        Ok(true)
    }

    /// Assumes left and right were initialized already
    fn internal_split_into(
        &self,
        mut pivot_index: usize,
        left: &mut Self,
        right: &mut Self,
        tuple_index: usize,
        key: &KeyVal,
        val: u32,
        split_strategy: SplitStrategy,
    ) -> Result<(), errors::RecallError> {
        let num = self.num_tuples();

        assert!(num > 1);
        assert!(pivot_index < num);

        if split_strategy == SplitStrategy::EmptyPage {
            // left will be full and right will be empty
            pivot_index = num;

            assert!(right.internal_put(&key, val, 0)?);
        }

        {
            let mut shift = 0;
            for i in 0..pivot_index {
                if i == tuple_index {
                    shift = 1;
                    assert!(left.internal_put(&key, val, i)?);
                }

                let key = self.internal_get_key(i);
                let val = self.internal_get_val(i);
                assert!(left.internal_put(&key, val, i + shift)?);
            }
        }

        {
            let mut shift = 0;
            for i in pivot_index..num {
                if i == tuple_index {
                    shift = 1;
                    assert!(right.internal_put(&key, val, i - pivot_index)?);
                }

                let key = self.internal_get_key(i);
                let val = self.internal_get_val(i);
                assert!(right.internal_put(&key, val, (i + shift) - pivot_index)?);
            }
        }

        Ok(())
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
///   key_scm           bytes
///   tup_scm           bytes
/// end leaf header
///                             __| stride
///   key_0_offset      u16       |
///   key_0_size        u16       |
///   val_0_offset      u16       |
///   val_0_size        u16     __|
///   ...
///   key_n_offset      u16
///   key_n_size        u16
///   val_n_offset      u16
///   val_n_size        u16
///   ... <free space> ...
///   ... <free space> ...
///   ... <free space> ...
///   key_0             tuple
///   val_0             tuple
///   ...
///   key_n             tuple
///   val_n             tuple
pub trait LeafOps: PageHeader {
    const R_SIBLING_PTR_OFFSET: usize = Self::PARENT_PTR_OFFSET + U32_SIZE;
    const L_SIBLING_PTR_OFFSET: usize = Self::R_SIBLING_PTR_OFFSET + U32_SIZE;
    const LEAF_FREE_BEGIN_PTR_OFFSET: usize = Self::L_SIBLING_PTR_OFFSET + U32_SIZE;
    const LEAF_FREE_END_PTR_OFFSET: usize = Self::LEAF_FREE_BEGIN_PTR_OFFSET + U16_SIZE;
    const LEAF_KEY_SCM_OFFSET: usize = Self::LEAF_FREE_END_PTR_OFFSET + U16_SIZE;
    const LEAF_KEY_STRIDE: usize = U16_SIZE * 4;
    fn leaf_header_size(key_scm: &[u8], tup_scm: &[u8]) -> usize;
    fn leaf_initialize_node(&mut self, make_root: bool, key_scm: &[u8], tup_scm: &[u8]);
    fn r_sibling(&self) -> u32;
    fn set_r_sibling(&mut self, val: u32);
    fn l_sibling(&self) -> u32;
    fn set_l_sibling(&mut self, val: u32);
    fn leaf_key_and_tup_scm(&self) -> (Vec<u8>, Vec<u8>);
    fn leaf_find_tuple_index(&self, key: &KeyVal) -> usize;
    fn leaf_get_key(&self, tuple_index: usize) -> KeyVal;
    fn leaf_get_val(&self, tuple_index: usize) -> TupVal;
    fn leaf_free_space(&mut self) -> u16;
    fn leaf_put(
        &mut self,
        key: &KeyVal,
        val: &TupVal,
        tuple_index: usize,
    ) -> Result<bool, errors::RecallError>;
    fn leaf_replace(
        &mut self,
        val: &TupVal,
        tuple_index: usize,
    ) -> Result<(), errors::RecallError>;
    fn leaf_split_into(
        &self,
        pivot_index: usize,
        left: &mut Self,
        right: &mut Self,
        tuple_index: usize,
        key: &KeyVal,
        val: &TupVal,
        split_strategy: SplitStrategy,
    ) -> Result<(), errors::RecallError>;
    fn min_key(&self) -> Option<KeyVal>;
    fn max_key(&self) -> Option<KeyVal>;
}

impl LeafOps for SlottedPage {
    fn leaf_header_size(key_scm: &[u8], tup_scm: &[u8]) -> usize {
        BYTE_SIZE   //format
        + BYTE_SIZE //is_root
        + U16_SIZE  //num
        + U32_SIZE  //parent_ptr
        + U32_SIZE  //r_sibling_ptr
        + U32_SIZE  //l_sibling_ptr
        + U16_SIZE  //free_begin
        + U16_SIZE  //free_end
        + SizedBuf::bytes_storage_size(key_scm.len())
        + SizedBuf::bytes_storage_size(tup_scm.len())
    }

    fn leaf_initialize_node(&mut self, make_root: bool, key_scm: &[u8], tup_scm: &[u8]) {
        self.set_format(LEAF_NODE);
        self.set_root_node(make_root);
        self.reset_num_tuples();
        self.set_l_sibling(NULL_PTR);
        self.set_r_sibling(NULL_PTR);
        self.buf.write_u16_offset(Self::LEAF_FREE_BEGIN_PTR_OFFSET, Self::leaf_header_size(key_scm, tup_scm) as u16);
        self.buf.write_u16_offset(Self::LEAF_FREE_END_PTR_OFFSET, self.page_size as u16);

        // write the key schema
        let first = self.buf.write_bytes_offset(Self::LEAF_KEY_SCM_OFFSET, key_scm);
        // eprintln!(
        //     "  → after key_scm write, bytes 20..25 = {:02X?}",
        //     &self.buf.get()[20..25]
        // );
        // write the tuple schema
        let second = self.buf.write_bytes_offset(first, tup_scm);
        // eprintln!(
        //     "  → after tup_scm write, bytes {}..{} = {:02X?}",
        //     first,
        //     first + 4,
        //     &self.buf.get()[first..first+4]
        // );
        // let offset = self.buf.write_bytes_offset(Self::LEAF_KEY_SCM_OFFSET, key_scm);
        // self.buf.write_bytes_offset(offset, tup_scm);
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

    fn leaf_key_and_tup_scm(&self) -> (Vec<u8>, Vec<u8>) {
        let (offset, key_scm) = self.buf.read_bytes_offset(Self::LEAF_KEY_SCM_OFFSET);
        let (_, tup_scm) = self.buf.read_bytes_offset(offset);
        (key_scm, tup_scm)
    }

    fn leaf_find_tuple_index(&self, key: &KeyVal) -> usize {
        self.bin_search(key, 0, self.num_tuples(), |i| {
            self.leaf_get_key(i)
        })
    }

    fn leaf_get_key(&self, tuple_index: usize) -> KeyVal {
        let (key_scm, tup_scm) = &self.leaf_key_and_tup_scm();
        let header_size = Self::leaf_header_size(key_scm, tup_scm);
        let offset = header_size + tuple_index * Self::LEAF_KEY_STRIDE;
        let (offset, key_offset) = self.buf.read_u16_offset(offset);
        let (_, key_size) = self.buf.read_u16_offset(offset);

        let buf = self.buf.get();
        let start = key_offset as usize;
        let end = start + key_size as usize;
        let src = &buf[start..end];

        let mut tup = Tuple::new(key_size as usize);
        tup.get_mut().copy_from_slice(src);

        tup.decode(key_scm)
    }

    fn leaf_get_val(&self, tuple_index: usize) -> TupVal {
        let (key_scm, tup_scm) = &self.leaf_key_and_tup_scm();
        let header_size = Self::leaf_header_size(key_scm, tup_scm);
        let offset = header_size + tuple_index * Self::LEAF_KEY_STRIDE + U16_SIZE * 2;
        let (offset, val_offset) = self.buf.read_u16_offset(offset);
        let (_, val_size) = self.buf.read_u16_offset(offset);

        let buf = self.buf.get();
        let start = val_offset as usize;
        let end = start + val_size as usize;
        let src = &buf[start..end];

        let mut tuple = Tuple::new(val_size as usize);
        tuple.get_mut().copy_from_slice(src);

        tuple.decode(tup_scm)
    }

    fn leaf_free_space(&mut self) -> u16 {
        let (_, begin_offset) = self.buf.read_u16_offset(Self::LEAF_FREE_BEGIN_PTR_OFFSET);
        let (_, end_offset) = self.buf.read_u16_offset(Self::LEAF_FREE_END_PTR_OFFSET);
        end_offset - begin_offset
    }

    fn leaf_put(&mut self, key: &KeyVal, val: &TupVal, tuple_index: usize) -> Result<bool, errors::RecallError> {
        let (key_scm, tup_scm) = &self.leaf_key_and_tup_scm();

        // snapshot *only* the two schema‐slots
        let key_schema_start = Self::LEAF_KEY_SCM_OFFSET;
        let key_schema_len   = SizedBuf::bytes_storage_size(key_scm.len());
        let tup_schema_start = key_schema_start + key_schema_len;
        let tup_schema_len   = SizedBuf::bytes_storage_size(tup_scm.len());

        let before_key_schema = self.buf.get()[ key_schema_start
            .. key_schema_start + key_schema_len ].to_vec();
        let before_tup_schema = self.buf.get()[ tup_schema_start
            .. tup_schema_start + tup_schema_len ].to_vec();

        let res = (|| {
            let (key_scm, tup_scm) = &self.leaf_key_and_tup_scm();
            ParameterType::typecheck(key, key_scm)?;
            ParameterType::typecheck(val, tup_scm)?;

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
                    return Err(errors::RecallError::UniqueKeyError)
                }
            }

            let header_size = Self::leaf_header_size(key_scm, tup_scm);
            // check space requirements
            let (_, begin_offset) = self.buf.read_u16_offset(Self::LEAF_FREE_BEGIN_PTR_OFFSET);
            let (_, end_offset) = self.buf.read_u16_offset(Self::LEAF_FREE_END_PTR_OFFSET);
            let free_space = end_offset - begin_offset;
            let key = Tuple::encode(key, key_scm).unwrap();
            let val = Tuple::encode(val, tup_scm).unwrap();
            let key_size = key.len();
            let val_size = val.len();
            let space_needed = u16::try_from(Self::LEAF_KEY_STRIDE + key_size + val_size).unwrap();
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
                let src_offset = header_size + i * Self::LEAF_KEY_STRIDE;
                let dst_offset = header_size + (i + 1) * Self::LEAF_KEY_STRIDE;
                self.buf.get_mut().copy_within(
                    src_offset..src_offset + Self::LEAF_KEY_STRIDE,
                    dst_offset
                );
            }

            // insert offsets for key/val pair
            let offset = header_size + tuple_index * Self::LEAF_KEY_STRIDE;
            let key_offset = end_offset - u16::try_from(key_size + val_size).unwrap();
            let offset = self.buf.write_u16_offset(offset, key_offset);
            let offset = self.buf.write_u16_offset(offset, u16::try_from(key_size).unwrap());
            let val_offset = end_offset - u16::try_from(val_size).unwrap();
            let offset = self.buf.write_u16_offset(offset, val_offset);
            self.buf.write_u16_offset(offset, u16::try_from(val_size).unwrap());

            // insert key
            let key_bytes = key.get();
            let key_dst_start = key_offset as usize;
            let key_dst = &mut self.buf.get_mut()[key_dst_start .. key_dst_start + key_size];
            key_dst.copy_from_slice(key_bytes);

            // insert val
            let val_bytes = val.get();
            let val_dst_start = val_offset as usize;
            let val_dst = &mut self.buf.get_mut()[val_dst_start .. val_dst_start + val_size];
            val_dst.copy_from_slice(val_bytes);

            self.inc_num_tuples();

            // update free begin and end offsets
            self.buf.write_u16_offset(Self::LEAF_FREE_BEGIN_PTR_OFFSET, begin_offset + u16::try_from(Self::LEAF_KEY_STRIDE).unwrap());
            self.buf.write_u16_offset(Self::LEAF_FREE_END_PTR_OFFSET, key_offset);

            Ok(true)
        })();

        let after = self.buf.get();
        debug_assert_eq!(
            &before_key_schema[..],
            &after[key_schema_start .. key_schema_start + key_schema_len],
            "Key schema bytes were overwritten!"
        );
        debug_assert_eq!(
            &before_tup_schema[..],
            &after[tup_schema_start .. tup_schema_start + tup_schema_len],
            "Tuple schema bytes were overwritten!"
        );

        res
    }

    fn leaf_replace(
        &mut self,
        val: &TupVal,
        tuple_index: usize,
    ) -> Result<(), errors::RecallError> {
        let (key_scm, tup_scm) = &self.leaf_key_and_tup_scm();
        let val = Tuple::encode(val, tup_scm).unwrap();
        let header_size = Self::leaf_header_size(&key_scm, &tup_scm);
        let offset = header_size + tuple_index * Self::LEAF_KEY_STRIDE + U16_SIZE * 2;
        let (offset, val_offset) = self.buf.read_u16_offset(offset);
        let (_, val_size) = self.buf.read_u16_offset(offset);
        assert!(val_size as usize == val.len(), "expected replaced tuple to be exact size of existing tuple");
        let dst_start = val_offset as usize;
        let dst = &mut self.buf.get_mut()[dst_start .. dst_start + val_size as usize];
        let new_bytes = val.get();
        dst.copy_from_slice(new_bytes);
        Ok(())
    }

    /// Assumes left and right were initialized already
    fn leaf_split_into(
        &self,
        mut pivot_index: usize,
        left: &mut Self,
        right: &mut Self,
        tuple_index: usize,
        key: &KeyVal,
        val: &TupVal,
        split_strategy: SplitStrategy,
    ) -> Result<(), errors::RecallError> {
        let num = self.num_tuples();

        assert!(num > 1);
        assert!(pivot_index < num);

        if split_strategy == SplitStrategy::EmptyPage {
            // left will be full and right will be empty
            pivot_index = num;

            assert!(right.leaf_put(key, val, 0)?);
        }

        {
            let mut shift = 0;
            for i in 0..pivot_index {
                if i == tuple_index {
                    shift = 1;
                    assert!(left.leaf_put(key, val, i)?);
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
                    assert!(right.leaf_put(key, val, i - pivot_index)?);
                }

                let key = self.leaf_get_key(i);
                let val = self.leaf_get_val(i);
                assert!(right.leaf_put(&key, &val, (i + shift) - pivot_index)?);
            }
        }

        Ok(())
    }

    fn min_key(&self) -> Option<KeyVal> {
        let num = self.num_tuples();

        if num > 0 {
            return Some(self.leaf_get_key(0))
        }
        None
    }

    fn max_key(&self) -> Option<KeyVal> {
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
    use crate::storage::engine::btree::format::PAGE_SIZE_4K;
    use crate::storage::engine::btree::format::ATOM_TYPE;
    use crate::storage::engine::btree::format::STRING_TYPE;
    use crate::storage::engine::btree::format::UINT_TYPE;
    use crate::storage::engine::btree::format::INT_TYPE;

    fn u32_key(key: u32) -> KeyVal {
        let tuple = {
            let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
            tuple.write_u32_offset(0, key);
            tuple
        };
        tuple.decode(&[UINT_TYPE])
    }

    #[test]
    fn it_tests_bin_search() {
        let key_scm = &[UINT_TYPE];
        let mut page = SlottedPage::new(PAGE_SIZE_4K, None);

        page.internal_initialize_node(true, key_scm);

        // keys: 1, 3, 6, 12, 69
        let keys_iter =
            vec![6, 1, 3, 69, 12]
            .into_iter()
            .map(|key| { u32_key(key) });

        // put tuples
        for key in keys_iter {
            let tuple_index: usize = page.internal_find_tuple_index(&key);
            assert!(page.internal_put(&key, 42, tuple_index).unwrap())
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
        ].into_iter().map(|(key, index)| { (u32_key(key), index) }) {
            let got_index = page.bin_search(&key, 0, page.num_tuples(), |i| {
                page.internal_get_key(i)
            });
            assert!(got_index == want_index);
        }
    }

    #[test]
    fn it_initializes_slotted_leaf_page() {
        let key_scm = vec![UINT_TYPE];
        let tup_scm = vec![UINT_TYPE];
        let mut page = SlottedPage::new(PAGE_SIZE_4K, None);
        page.leaf_initialize_node(true, &key_scm, &tup_scm);
        assert!(page.is_leaf_node());
        assert!(page.is_root_node());
        assert!(page.num_tuples() == 0);
        assert!(page.parent_ptr() == NULL_PTR);
        assert!(page.r_sibling() == NULL_PTR);
        assert!(page.l_sibling() == NULL_PTR);
        assert_eq!(page.leaf_key_and_tup_scm(), (key_scm, tup_scm));
    }

    #[test]
    fn it_initializes_slotted_internal_page() {
        let key_scm = &[UINT_TYPE];
        let mut page = SlottedPage::new(PAGE_SIZE_4K, None);
        page.internal_initialize_node(true, key_scm);
        assert!(!page.is_leaf_node());
        assert!(page.is_root_node());
        assert!(page.num_tuples() == 0);
        assert!(page.parent_ptr() == NULL_PTR);
        assert!(page.right_child() == NULL_PTR);
        assert_eq!(page.internal_key_scm(), key_scm);
    }

    #[test]
    fn it_can_get_and_put_tuples_internal_page() {
        let key_scm = &[UINT_TYPE];
        let mut page = SlottedPage::new(PAGE_SIZE_4K, None);
        page.internal_initialize_node(true, key_scm);

        let pairs_iter =
            vec![(6, 100), (0, 101), (3, 102), (69, 103), (12, 104)]
            .into_iter()
            .map(|(key, val)| { (u32_key(key), val) });

        let right_child_val = 105;

        // put tuples
        for (key, val) in pairs_iter.clone() {
            let tuple_index: usize = page.internal_find_tuple_index(&key);
            assert!(page.internal_put(&key, val, tuple_index).unwrap())
        }
        page.set_right_child(right_child_val);

        assert!(page.num_tuples() == pairs_iter.clone().len());

        // keys are correctly ordered
        let mut ordered_keys: Vec<KeyVal> = pairs_iter.clone().map(|pair| { pair.0 }).collect();
        ordered_keys.sort();

        assert!(
            (0..page.num_tuples())
            .map(|tuple_index| {
                page.internal_get_key(tuple_index)
            })
            .collect::<Vec<_>>() == ordered_keys
        );

        // get tuples
        for (key, val) in pairs_iter.clone() {
            let tuple_index: usize = page.internal_find_tuple_index(&key);
            let stored_key = page.internal_get_key(tuple_index);
            let stored_val = page.internal_get_val(tuple_index);
            assert!(stored_key == key);
            assert!(stored_val == val);
        }
        assert!(page.right_child() == right_child_val);
    }

    #[test]
    fn it_can_get_and_put_tuples_leaf_page() {
        let key_scm = &[UINT_TYPE];
        let tup_scm = &[UINT_TYPE, STRING_TYPE, INT_TYPE, ATOM_TYPE];
        let mut page = SlottedPage::new(PAGE_SIZE_4K, None);
        page.leaf_initialize_node(true, key_scm, tup_scm);

        let build_tuple = |key: u32| -> TupVal {
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
            tuple.decode(tup_scm)
        };

        let pairs_iter =
            vec![6, 0, 3, 69, 12]
            .into_iter()
            .map(|key| {
                (u32_key(key), build_tuple(key))
            });

        // put tuples
        for (key, val) in pairs_iter.clone() {
            let tuple_index: usize = page.leaf_find_tuple_index(&key);
            assert!(page.leaf_put(&key, &val, tuple_index).unwrap());
        }

        assert!(page.num_tuples() == pairs_iter.clone().len());

        // keys are correctly ordered
        let mut ordered_keys: Vec<KeyVal> = pairs_iter.clone().map(|pair| { pair.0 }).collect();
        ordered_keys.sort();

        assert!(
            (0..page.num_tuples())
            .map(|tuple_index| {
                page.leaf_get_key(tuple_index)
            })
            .collect::<Vec<_>>() == ordered_keys
        );

        // get tuples
        for (key, val) in pairs_iter.clone() {
            let tuple_index: usize = page.leaf_find_tuple_index(&key);
            let stored_key = page.leaf_get_key(tuple_index);
            let stored_val = page.leaf_get_val(tuple_index);
            assert!(stored_key == key);
            assert!(stored_val == val);
        }
    }

    #[test]
    fn it_will_return_uniq_key_error_on_existing_key() {
        let key_scm = &[UINT_TYPE];
        let tup_scm = &[UINT_TYPE];
        let mut page = SlottedPage::new(PAGE_SIZE_4K, None);
        page.leaf_initialize_node(false, key_scm, tup_scm);

        // put tuples
        // [0 3 6 12 69], insert 69
        let insert_pair = (u32_key(69), 69);
        let pairs_iter =
            vec![6, 0, 3, 69, 12]
            .into_iter()
            .map(|key| { (u32_key(key), key) });

        for (key, val) in pairs_iter.clone() {
            let tuple_index: usize = page.leaf_find_tuple_index(&key);
            let tuple = {
                let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
                tuple.write_u32_offset(0, val);
                tuple.decode(tup_scm)
            };
            assert!(page.leaf_put(&key, &tuple, tuple_index).unwrap());
        }

        let tuple_index: usize = page.leaf_find_tuple_index(&insert_pair.0);
        let tuple = {
            let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
            tuple.write_u32_offset(0, insert_pair.1);
            tuple.decode(tup_scm)
        };
        let result = page.leaf_put(&insert_pair.0, &tuple, tuple_index);
        assert!(result.is_err());
        matches!(result, Err(errors::RecallError::UniqueKeyError));
        // assert_eq!(result.unwrap_err(), errors::RecallError::UniqueKeyError);
    }

    #[test]
    fn it_can_split_tuples_internal_half_strategy() {
        let key_scm = &[UINT_TYPE];
        let mut page = SlottedPage::new(PAGE_SIZE_4K, None);
        page.internal_initialize_node(true, key_scm);

        // put tuples
        // [0 3 6 12 69], insert 42
        let insert_val = 105;
        let pairs_iter = vec![
            (6, 100),
            (0, 101),
            (3, 102),
            (69, 103),
            (12, 104),
        ]
        .into_iter()
        .map(|(key, val)| { (u32_key(key), val) });
        let left_pairs_ordered = vec![
            (u32_key(0), 101),
            (u32_key(3), 102),
        ];
        let right_pairs_ordered = vec![
            (u32_key(6), 100),
            (u32_key(12), 104),
            (u32_key(42), insert_val),
            (u32_key(69), 103),
        ];

        for (key, val) in pairs_iter.clone() {
            let tuple_index: usize = page.internal_find_tuple_index(&key);
            assert!(page.internal_put(&key, val, tuple_index).unwrap())
        }

        // initialize left & right pages
        let mut left = SlottedPage::new(PAGE_SIZE_4K, None);
        left.internal_initialize_node(false, key_scm);

        let mut right = SlottedPage::new(PAGE_SIZE_4K, None);
        right.internal_initialize_node(false, key_scm);

        let pivot_index = pairs_iter.clone().len() / 2;
        let tuple_index: usize = page.internal_find_tuple_index(&u32_key(42));
        page.internal_split_into(
            pivot_index,
            &mut left,
            &mut right,
            tuple_index,
            &u32_key(42),
            insert_val,
            SplitStrategy::HalfFullPage,
        ).unwrap();

        // re-initialize page
        page.internal_initialize_node(true, key_scm);
        assert!(!page.is_leaf_node());
        assert!(page.is_root_node());
        assert!(page.num_tuples() == 0);
        assert!(page.parent_ptr() == NULL_PTR);
        assert!(page.right_child() == NULL_PTR);
        let max_free_space = page.page_size - SlottedPage::internal_header_size(key_scm);
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
        assert!(right.num_tuples() == pairs_iter.clone().len() - pivot_index + 1); // +1 for insert key
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
        let key_scm = &[UINT_TYPE];
        let tup_scm = &[UINT_TYPE];
        let mut page = SlottedPage::new(PAGE_SIZE_4K, None);
        page.leaf_initialize_node(false, key_scm, tup_scm);

        // put tuples
        // [0 3 6 12 69], insert 42
        let pairs_iter =
            vec![6, 0, 3, 69, 12]
            .into_iter()
            .map(|key| { (u32_key(key), key) });
        let left_keys_ordered = vec![u32_key(0), u32_key(3)];
        let right_keys_ordered = vec![u32_key(6), u32_key(12), u32_key(42), u32_key(69)];

        for (key, val) in pairs_iter.clone() {
            let tuple_index: usize = page.leaf_find_tuple_index(&key);
            let tuple = {
                let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
                tuple.write_u32_offset(0, val);
                tuple.decode(tup_scm)
            };
            assert!(page.leaf_put(&key, &tuple, tuple_index).unwrap());
        }

        // initialize left & right pages
        let mut left = SlottedPage::new(PAGE_SIZE_4K, None);
        left.leaf_initialize_node(false, key_scm, tup_scm);

        let mut right = SlottedPage::new(PAGE_SIZE_4K, None);
        right.leaf_initialize_node(false, key_scm, tup_scm);

        let pivot_index = pairs_iter.clone().len() / 2;
        let tuple_index = page.leaf_find_tuple_index(&u32_key(42));
        let tuple = {
            let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
            tuple.write_u32_offset(0, 42);
            tuple.decode(tup_scm)
        };
        page.leaf_split_into(
            pivot_index,
            &mut left,
            &mut right,
            tuple_index,
            &u32_key(42),
            &tuple,
            SplitStrategy::HalfFullPage,
        ).unwrap();

        // re-initialize page
        page.leaf_initialize_node(false, key_scm, tup_scm);
        assert!(page.is_leaf_node());
        assert!(!page.is_root_node());
        assert!(page.num_tuples() == 0);
        assert!(page.parent_ptr() == NULL_PTR);
        assert!(page.l_sibling() == NULL_PTR);
        assert!(page.r_sibling() == NULL_PTR);
        let max_free_space = page.page_size - SlottedPage::leaf_header_size(key_scm, tup_scm);
        assert!(page.leaf_free_space() == max_free_space as u16);

        // check left page
        assert!(left.num_tuples() == pivot_index);
        assert!(
            (0..left.num_tuples())
            .into_iter()
            .map(|tuple_index| {
                let key = left.leaf_get_key(tuple_index);
                let val = left.leaf_get_val(tuple_index);
                assert_eq!(key, val);
                key
            })
            .collect::<Vec<_>>() == left_keys_ordered
        );

        // check right page
        assert!(right.num_tuples() == pairs_iter.clone().len() - pivot_index + 1); // +1 for insert key
        assert!(
            (0..right.num_tuples())
            .into_iter()
            .map(|tuple_index| {
                let key = right.leaf_get_key(tuple_index);
                let val = right.leaf_get_val(tuple_index);
                assert_eq!(key, val);
                key
            })
            .collect::<Vec<_>>() == right_keys_ordered
        );
    }

    #[test]
    fn it_will_return_false_on_cap_limit_leaf() {
        let key_scm = &[UINT_TYPE];
        let tup_scm = &[UINT_TYPE];
        let mut page = SlottedPage::new(PAGE_SIZE_4K, Some(5));
        page.leaf_initialize_node(true, key_scm, tup_scm);

        // put tuples
        // [0 3 6 12 69], insert 42
        let pairs_iter =
            vec![6, 0, 3, 69, 12]
            .into_iter()
            .map(|key| { (u32_key(key), key) });

        for (key, val) in pairs_iter.clone() {
            let tuple_index: usize = page.leaf_find_tuple_index(&key);
            let tuple = {
                let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
                tuple.write_u32_offset(0, val);
                tuple.decode(tup_scm)
            };
            assert!(page.leaf_put(&key, &tuple, tuple_index).unwrap());
        }

        let tuple = {
            let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
            tuple.write_u32_offset(0, 42);
            tuple.decode(tup_scm)
        };
        let tuple_index: usize = page.leaf_find_tuple_index(&u32_key(42));
        assert!(!page.leaf_put(&u32_key(42), &tuple, tuple_index).unwrap());
    }

    #[test]
    fn it_will_return_false_on_cap_limit_internal() {
        let key_scm = &[UINT_TYPE];
        let mut page = SlottedPage::new(PAGE_SIZE_4K, Some(5));
        page.internal_initialize_node(true, key_scm);

        // put tuples
        // [0 3 6 12 69], insert 42
        let right_child_val = 75;
        let pairs_iter =
            vec![6, 0, 3, 69, 12]
            .into_iter()
            .map(|key| { (u32_key(key), key) });

        for (key, val) in pairs_iter.clone() {
            let tuple_index: usize = page.internal_find_tuple_index(&key);
            assert!(page.internal_put(&key, val, tuple_index).unwrap())
        }
        page.set_right_child(right_child_val);

        let tuple_index: usize = page.internal_find_tuple_index(&u32_key(42));
        assert!(!page.internal_put(&u32_key(42), 42, tuple_index).unwrap())
    }

    #[test]
    fn it_can_split_tuples_leaf_empty_strategy() {
        let key_scm = &[UINT_TYPE];
        let tup_scm = &[UINT_TYPE];
        let mut page = SlottedPage::new(PAGE_SIZE_4K, None);
        page.leaf_initialize_node(false, key_scm, tup_scm);

        // put tuples
        // [0 3 6 12 69], insert 75
        let pairs_iter =
            vec![6, 0, 3, 69, 12]
            .into_iter()
            .map(|key| { (u32_key(key), key) });
        let left_keys_ordered = vec![u32_key(0), u32_key(3), u32_key(6), u32_key(12), u32_key(69)];
        let right_keys_ordered = vec![u32_key(75)];

        for (key, val) in pairs_iter.clone() {
            let tuple_index: usize = page.leaf_find_tuple_index(&key);
            let tuple = {
                let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
                tuple.write_u32_offset(0, val);
                tuple.decode(tup_scm)
            };
            assert!(page.leaf_put(&key, &tuple, tuple_index).unwrap());
        }

        // initialize left & right pages
        let mut left = SlottedPage::new(PAGE_SIZE_4K, None);
        left.leaf_initialize_node(false, key_scm, tup_scm);

        let mut right = SlottedPage::new(PAGE_SIZE_4K, None);
        right.leaf_initialize_node(false, key_scm, tup_scm);

        let pivot_index = pairs_iter.clone().len() / 2;
        let tuple_index = page.leaf_find_tuple_index(&u32_key(75));
        let tuple = {
            let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
            tuple.write_u32_offset(0, 75);
            tuple.decode(tup_scm)
        };
        page.leaf_split_into(
            pivot_index,
            &mut left,
            &mut right,
            tuple_index,
            &u32_key(75),
            &tuple,
            SplitStrategy::EmptyPage,
        ).unwrap();

        // check left page
        assert!(left.num_tuples() == pairs_iter.clone().len());
        assert!(
            (0..left.num_tuples())
            .into_iter()
            .map(|tuple_index| {
                let key = left.leaf_get_key(tuple_index);
                let val = left.leaf_get_val(tuple_index);
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
                assert_eq!(key, val);
                key
            })
            .collect::<Vec<_>>() == right_keys_ordered
        );
    }

    #[test]
    fn it_can_typecheck_the_schema() {
        let key_scm = &[UINT_TYPE];
        let tup_scm = &[ATOM_TYPE];
        let mut page = SlottedPage::new(PAGE_SIZE_4K, None);
        page.leaf_initialize_node(false, key_scm, tup_scm);

        let tuple_index: usize = page.leaf_find_tuple_index(&u32_key(42));
        let tuple = {
            let mut tuple = Tuple::new(SizedBuf::string_storage_size("Hello, World!".len()));
            tuple.write_string_offset(0, "Hello, World!");
            tuple.decode(&[STRING_TYPE])
        };

        let result = page.leaf_put(&tuple, &u32_key(42), tuple_index);
        assert!(result.is_err());
        matches!(result, Err(errors::RecallError::TypeError(_)));
        // assert_eq!(result.unwrap_err(), errors::RecallError::TypeError("expected tuple position 0 to be typed as uint".to_string()));

        let result = page.leaf_put(&u32_key(42), &tuple, tuple_index);
        assert!(result.is_err());
        matches!(result, Err(errors::RecallError::TypeError(_)));
        // assert_eq!(result.unwrap_err(), errors::RecallError::TypeError("expected tuple position 0 to be typed as atom".to_string()));

        let tuple = {
            let mut tuple = Tuple::new(SizedBuf::atom_storage_size("Hello, World!".len()));
            tuple.write_atom_offset(0, "Hello, World!");
            tuple.decode(tup_scm)
        };
        let result = page.leaf_put(&u32_key(42), &tuple, tuple_index);
        assert!(result.is_ok());
        assert!(result.unwrap())
    }

    #[test]
    fn it_can_replace_tuple() {
        let key_scm = &[UINT_TYPE];
        let tup_scm = &[UINT_TYPE];
        let mut page = SlottedPage::new(PAGE_SIZE_4K, None);
        page.leaf_initialize_node(true, key_scm, tup_scm);

        let pairs_iter =
            vec![6, 0, 3, 69, 12]
            .into_iter()
            .map(|key| {
                (u32_key(key), u32_key(key + 1000))
            });

        // put tuples
        for (key, val) in pairs_iter.clone() {
            let tuple_index = page.leaf_find_tuple_index(&key);
            assert!(page.leaf_put(&key, &val, tuple_index).unwrap());
        }

        // replace tuple for key 69
        let tuple_index = page.leaf_find_tuple_index(&u32_key(69));
        let val = page.leaf_get_val(tuple_index);
        assert_eq!(val, vec![ParameterType::UInt(1069)]);
        page.leaf_replace(&u32_key(0), tuple_index).unwrap();

        let got =
            (0..page.num_tuples())
            .into_iter()
            .map(|tid| {
                let key = page.leaf_get_key(tid);
                let val = page.leaf_get_val(tid);
                (key, val)
            })
            .collect::<Vec<(KeyVal, TupVal)>>();
        let want =
            vec![0, 3, 6, 12, 69]
            .into_iter()
            .map(|key| {
                (
                    u32_key(key),
                    if key == 69 {
                        u32_key(0)
                    } else {
                        u32_key(key + 1000)
                    }
                )
            })
            .collect::<Vec<(KeyVal, TupVal)>>();
        assert_eq!(got, want);
    }
}
