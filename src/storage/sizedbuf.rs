use std::convert::TryInto;

use crate::storage::format;

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
        format::I32_SIZE
    }

    pub fn atom_storage_size(len: usize) -> usize {
        format::BYTE_SIZE + len
    }

    pub fn string_storage_size(len: usize) -> usize {
        format::U32_SIZE + len
    }

    pub fn copy(src: &Self, dst: &mut Self) {
        dst.data.copy_from_slice(&src.data);
    }

    // ------ primitive reads/writes ------
    pub fn read_u8_offset(&self, offset: usize) -> (usize, u8) {
        let val = self.data[offset];

        (offset + format::BYTE_SIZE, val)
    }

    pub fn write_u8_offset(&mut self, offset: usize, val: u8) -> usize {
        self.data[offset] = val;
        offset + format::BYTE_SIZE
    }

    pub fn read_u16_offset(&self, offset: usize) -> (usize, u16) {
        let start = offset;
        let end = offset + format::U16_SIZE;
        let bytes: [u8; format::U16_SIZE] = self.data[start..end]
            .try_into()
            .expect("slice with incorrect length");
        (end, u16::from_be_bytes(bytes))
    }

    pub fn write_u16_offset(&mut self, offset: usize, val: u16) -> usize {
        let bytes = val.to_be_bytes();
        let dst = &mut self.data[offset..offset + format::U16_SIZE];
        dst.copy_from_slice(&bytes);
        offset + format::U16_SIZE
    }

    pub fn read_u32_offset(&self, offset: usize) -> (usize, u32) {
        let start = offset;
        let end = offset + format::U32_SIZE;
        let bytes: [u8; format::U32_SIZE] = self.data[start..end]
            .try_into()
            .expect("slice with incorrect length");
        (end, u32::from_be_bytes(bytes))
    }

    pub fn write_u32_offset(&mut self, offset: usize, val: u32) -> usize {
        let bytes = val.to_be_bytes();
        let dst = &mut self.data[offset..offset + format::U32_SIZE];
        dst.copy_from_slice(&bytes);
        offset + format::U32_SIZE
    }

    pub fn read_i32_offset(&self, offset: usize) -> (usize, i32) {
        let start = offset;
        let end = offset + format::I32_SIZE;
        let bytes: [u8; format::I32_SIZE] = self.data[start..end]
            .try_into()
            .expect("slice with incorrect length");
        (end, i32::from_be_bytes(bytes))
    }

    pub fn write_i32_offset(&mut self, offset: usize, val: i32) -> usize {
        let bytes = val.to_be_bytes();
        let dst = &mut self.data[offset..offset + format::I32_SIZE];
        dst.copy_from_slice(&bytes);
        offset + format::I32_SIZE
    }

    // pub fn read_usize_offset(&self, offset: usize) -> (usize, usize) {
    //     let start = offset;
    //     let end = offset + USIZE_SIZE;
    //     let bytes: [u8; USIZE_SIZE] = self.data[start..end]
    //         .try_into()
    //         .expect("slice with incorrect length");
    //     (end, usize::from_be_bytes(bytes))
    // }

    // pub fn write_usize_offset(&mut self, offset: usize, val: usize) -> usize {
    //     let bytes = val.to_be_bytes();
    //     let dst = &mut self.data[offset..offset + USIZE_SIZE];
    //     dst.copy_from_slice(&bytes);
    //     offset + USIZE_SIZE
    // }

    // ------ atom (signed-length byte + UTF-8) ------

    pub fn read_atom_offset(&self, offset: usize) -> (usize, String) {
        let (start, len) = self.read_u8_offset(offset);
        let end = start + len as usize;
        let val = String::from_utf8(self.data[start..end].to_vec())
            .expect("invalid UTF-8 in atom");
        (end, val)
    }

    pub fn write_atom_offset(&mut self, offset: usize, val: &str) -> usize {
        assert!(val.len() > 0 && val.len() <= 127);
        let start = self.write_u8_offset(offset, val.len().try_into().unwrap());
        let end = start + val.len();
        self.data[start..end].copy_from_slice(val.as_bytes());
        end
    }

    // ------ string (usize-length + UTF-8) ------

    pub fn read_string_offset(&self, offset: usize) -> (usize, String) {
        let (start, len) = self.read_u32_offset(offset);
        let end = start + len as usize;
        let val = String::from_utf8(self.data[start..end].to_vec())
            .expect("invalid UTF-8 in string");
        (end, val)
    }

    pub fn write_string_offset(&mut self, offset: usize, val: &str) -> usize {
        assert!(val.len() <= u32::MAX as usize);
        let start = self.write_u32_offset(offset, val.len().try_into().unwrap());
        let end = start + val.len();
        self.data[start..end].copy_from_slice(val.as_bytes());
        end
    }
}
