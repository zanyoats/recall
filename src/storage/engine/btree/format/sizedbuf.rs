use std::ptr;
use std::slice;

use super::BYTE_SIZE;
use super::U16_SIZE;
use super::U32_SIZE;

#[derive(Clone, PartialEq, Eq)]
pub struct SizedBuf {
    size: usize,
    data: Vec<u8>,
}

impl SizedBuf {
    pub fn new(size: usize) -> Self {
        let data = vec![0u8; size];

        SizedBuf { size, data }
    }

    pub fn get(&self) -> &[u8] {
        &self.data
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn int_storage_size() -> usize {
        U32_SIZE
    }

    pub fn uint_storage_size() -> usize {
        U32_SIZE
    }

    pub fn atom_storage_size(len: usize) -> usize {
        BYTE_SIZE + len
    }

    pub fn string_storage_size(len: usize) -> usize {
        BYTE_SIZE + len + U32_SIZE
    }

    pub fn bytes_storage_size(len: usize) -> usize {
        BYTE_SIZE + len + U32_SIZE
    }

    pub fn copy(src: &Self, dst: &mut Self) {
        dst.data.copy_from_slice(&src.data);
    }

    pub fn read_i8_offset(
        &self,
        offset: usize,
    ) -> (usize, i8) {
        unsafe {
            let ptr = self.data.as_ptr().add(offset);
            let val = ptr.cast::<i8>().read_unaligned();
            (offset + BYTE_SIZE, val)
        }
    }

    pub fn write_i8_offset(
        &mut self,
        offset: usize,
        val: i8,
    ) -> usize {
        unsafe {
            let ptr = self.data.as_mut_ptr().add(offset);
            ptr.cast::<i8>().write(val);
            offset + BYTE_SIZE
        }
    }

    pub fn read_u8_offset(
        &self,
        offset: usize,
    ) -> (usize, u8) {
        unsafe {
            let ptr = self.data.as_ptr().add(offset);
            let val = ptr.read_unaligned();
            (offset + BYTE_SIZE, val)
        }
    }

    pub fn write_u8_offset(
        &mut self,
        offset: usize,
        val: u8,
    ) -> usize {
        unsafe {
            let ptr = self.data.as_mut_ptr().add(offset);
            ptr.write(val);
            offset + BYTE_SIZE
        }
    }

    // Numbers format
    // big endian for multi-byte numbers

    pub fn read_u16_offset(
        &self,
        offset: usize,
    ) -> (usize, u16) {
        unsafe {
            let ptr = self.data.as_ptr().add(offset);
            let val = ptr.cast::<u16>().read_unaligned();
            (offset + U16_SIZE, u16::from_be(val))
        }
    }

    pub fn write_u16_offset(
        &mut self,
        offset: usize,
        val: u16,
    ) -> usize {
        unsafe {
            let ptr = self.data.as_mut_ptr().add(offset);
            ptr.cast::<u16>().write(u16::to_be(val));
            offset + U16_SIZE
        }
    }

    pub fn read_i32_offset(
        &self,
        offset: usize,
    ) -> (usize, i32) {
        unsafe {
            let ptr = self.data.as_ptr().add(offset);
            let val = ptr.cast::<i32>().read_unaligned();
            (offset + U32_SIZE, i32::from_be(val))
        }
    }

    pub fn write_i32_offset(
        &mut self,
        offset: usize,
        val: i32,
    ) -> usize {
        unsafe {
            let ptr = self.data.as_mut_ptr().add(offset);
            ptr.cast::<i32>().write(i32::to_be(val));
            offset + U32_SIZE
        }
    }

    pub fn read_u32_offset(
        &self,
        offset: usize,
    ) -> (usize, u32) {
        unsafe {
            let ptr = self.data.as_ptr().add(offset);
            let val = ptr.cast::<u32>().read_unaligned();
            (offset + U32_SIZE, u32::from_be(val))
        }
    }

    pub fn write_u32_offset(
        &mut self,
        offset: usize,
        val: u32,
    ) -> usize {
        unsafe {
            let ptr = self.data.as_mut_ptr().add(offset);
            ptr.cast::<u32>().write(u32::to_be(val));
            offset + U32_SIZE
        }
    }

    // Atom format

    // <i8>           (signed) byte is the length (max is 127)
    // <length bytes> string data

    pub fn read_atom_offset(&self, offset: usize) -> (usize, String) {
        unsafe {
            let ptr = self.data.as_ptr().add(offset);
            let len = ptr.cast::<i8>().read_unaligned();
            let ptr= ptr.add(BYTE_SIZE);
            let val =
                String::from_utf8(slice::from_raw_parts(
                    ptr,
                    len as usize,
                ).to_vec())
                .unwrap();

            (offset + BYTE_SIZE + len as usize, val)
        }
    }

    pub fn write_atom_offset(&mut self, offset: usize, val: &str) -> usize {
        assert!(val.len() > 0);
        assert!(val.len() <= 127);

        unsafe {
            let ptr = self.data.as_mut_ptr().add(offset);
            ptr.cast::<i8>().write(val.len() as i8);
            let ptr= ptr.add(BYTE_SIZE);
            ptr::copy_nonoverlapping(
                val.as_ptr(),
                ptr,
                val.len(),
            );

            offset + BYTE_SIZE + val.len() as usize
        }
    }

    // String format

    // <u8>           byte is the length (max is 255)
    // <length bytes> string data
    // <u32>          4 bytes for continuation or overflow page num

    pub fn read_string_offset(&self, offset: usize) -> (usize, String) {
        unsafe {
            let ptr = self.data.as_ptr().add(offset);
            let len = ptr.cast::<u8>().read_unaligned();
            let ptr = ptr.add(BYTE_SIZE);
            let val =
                String::from_utf8(slice::from_raw_parts(
                    ptr,
                    len as usize,
                ).to_vec())
                .unwrap();
            let ptr = ptr.add(len as usize);
            let overflow_page_num = ptr.cast::<u32>().read_unaligned();
            let overflow_page_num = u32::from_be(overflow_page_num);
            assert!(overflow_page_num == 0);

            (offset + BYTE_SIZE + len as usize + U32_SIZE, val)
        }
    }

    pub fn write_string_offset(&mut self, offset: usize, val: &str) -> usize {
        unsafe {
            let ptr = self.data.as_mut_ptr().add(offset);
            ptr.cast::<u8>().write(val.len() as u8);
            let ptr = ptr.add(BYTE_SIZE);
            ptr::copy_nonoverlapping(
                val.as_ptr(),
                ptr,
                val.len(),
            );
            let ptr = ptr.add(val.len());
            ptr.cast::<u32>().write(u32::to_be(0));

            offset + BYTE_SIZE + val.len() as usize + U32_SIZE
        }
    }

    // Bytes format

    // <u8>           byte is the length (max is 255)
    // <length bytes> bytes
    // <u32>          4 bytes for continuation or overflow page num

    pub fn read_bytes_offset(&self, offset: usize) -> (usize, Vec<u8>) {
        unsafe {
            let ptr = self.data.as_ptr().add(offset);
            let len = ptr.cast::<u8>().read_unaligned();
            let ptr = ptr.add(BYTE_SIZE);
            let val = slice::from_raw_parts(ptr, len as usize).to_vec();
            let ptr = ptr.add(len as usize);
            let overflow_page_num = ptr.cast::<u32>().read_unaligned();
            let overflow_page_num = u32::from_be(overflow_page_num);
            assert!(overflow_page_num == 0);

            (offset + BYTE_SIZE + len as usize + U32_SIZE, val)
        }
    }

    pub fn write_bytes_offset(&mut self, offset: usize, val: &[u8]) -> usize {
        unsafe {
            let ptr = self.data.as_mut_ptr().add(offset);
            ptr.cast::<u8>().write(val.len() as u8);
            let ptr = ptr.add(BYTE_SIZE);
            ptr::copy_nonoverlapping(
                val.as_ptr(),
                ptr,
                val.len(),
            );
            let ptr = ptr.add(val.len());
            ptr.cast::<u32>().write(u32::to_be(0));

            offset + BYTE_SIZE + val.len() as usize + U32_SIZE
        }
    }
}
