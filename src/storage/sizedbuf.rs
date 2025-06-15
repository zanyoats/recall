use std::convert::TryInto;

use crate::storage::format::BYTE_SIZE;
use crate::storage::format::I32_SIZE;
use crate::storage::format::USIZE_SIZE;

#[derive(Clone, PartialEq, Eq)]
pub struct SizedBuf {
    size: usize,
    data: Vec<u8>,
}

impl From<Vec<u8>> for SizedBuf {
    fn from(data: Vec<u8>) -> Self {
        let size = data.len();
        SizedBuf { size, data }
    }
}

impl From<Box<[u8]>> for SizedBuf {
    fn from(b: Box<[u8]>) -> Self {
        // convert to Vec<u8>, then into SizedBuf
        Vec::from(b).into()
    }
}

impl SizedBuf {
    pub fn new(size: usize) -> Self {
        let data = vec![0u8; size];
        SizedBuf { size, data }
    }

    pub fn append_zero(&mut self) {
        self.data.push(0);
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
        I32_SIZE
    }

    pub fn atom_storage_size(len: usize) -> usize {
        BYTE_SIZE + len
    }

    pub fn string_storage_size(len: usize) -> usize {
        USIZE_SIZE + len
    }

    pub fn copy(src: &Self, dst: &mut Self) {
        dst.data.copy_from_slice(&src.data);
    }

    // ------ primitive reads/writes ------

    pub fn read_i32_offset(&self, offset: usize) -> (usize, i32) {
        let start = offset;
        let end = offset + I32_SIZE;
        let bytes: [u8; I32_SIZE] = self.data[start..end]
            .try_into()
            .expect("slice with incorrect length");
        (end, i32::from_be_bytes(bytes))
    }

    pub fn write_i32_offset(&mut self, offset: usize, val: i32) -> usize {
        let bytes = val.to_be_bytes();
        let dst = &mut self.data[offset..offset + I32_SIZE];
        dst.copy_from_slice(&bytes);
        offset + I32_SIZE
    }

    pub fn read_usize_offset(&self, offset: usize) -> (usize, usize) {
        let start = offset;
        let end = offset + USIZE_SIZE;
        let bytes: [u8; USIZE_SIZE] = self.data[start..end]
            .try_into()
            .expect("slice with incorrect length");
        (end, usize::from_be_bytes(bytes))
    }

    pub fn write_usize_offset(&mut self, offset: usize, val: usize) -> usize {
        let bytes = val.to_be_bytes();
        let dst = &mut self.data[offset..offset + USIZE_SIZE];
        dst.copy_from_slice(&bytes);
        offset + USIZE_SIZE
    }

    // ------ atom (signed-length byte + UTF-8) ------

    pub fn read_atom_offset(&self, offset: usize) -> (usize, String) {
        let len = self.data[offset] as usize;
        let start = offset + BYTE_SIZE;
        let end = start + len;
        let val = String::from_utf8(self.data[start..end].to_vec())
            .expect("invalid UTF-8 in atom");
        (end, val)
    }

    pub fn write_atom_offset(&mut self, offset: usize, val: &str) -> usize {
        assert!(val.len() > 0 && val.len() <= 127);
        self.data[offset] = val.len() as u8;
        let start = offset + BYTE_SIZE;
        let end = start + val.len();
        self.data[start..end].copy_from_slice(val.as_bytes());
        end
    }

    // ------ string (usize-length + UTF-8) ------

    pub fn read_string_offset(&self, offset: usize) -> (usize, String) {
        let (start, len) = self.read_usize_offset(offset);
        let end = start + len;
        let val = String::from_utf8(self.data[start..end].to_vec())
            .expect("invalid UTF-8 in string");
        (end, val)
    }

    pub fn write_string_offset(&mut self, offset: usize, val: &str) -> usize {
        assert!(val.len() <= usize::MAX);
        let start = self.write_usize_offset(offset, val.len());
        let end = start + val.len();
        self.data[start..end].copy_from_slice(val.as_bytes());
        end
    }
}
