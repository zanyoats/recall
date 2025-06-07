use std::convert::TryInto;

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

    // ------ primitive reads/writes ------

    pub fn read_i8_offset(&self, offset: usize) -> (usize, i8) {
        let val = self.data[offset] as i8;
        (offset + BYTE_SIZE, val)
    }

    pub fn write_i8_offset(&mut self, offset: usize, val: i8) -> usize {
        self.data[offset] = val as u8;
        offset + BYTE_SIZE
    }

    pub fn read_u8_offset(&self, offset: usize) -> (usize, u8) {
        let val = self.data[offset];
        (offset + BYTE_SIZE, val)
    }

    pub fn write_u8_offset(&mut self, offset: usize, val: u8) -> usize {
        self.data[offset] = val;
        offset + BYTE_SIZE
    }

    pub fn read_u16_offset(&self, offset: usize) -> (usize, u16) {
        let start = offset;
        let end = offset + U16_SIZE;
        let bytes: [u8; 2] = self.data[start..end]
            .try_into()
            .expect("slice with incorrect length");
        (end, u16::from_be_bytes(bytes))
    }

    pub fn write_u16_offset(&mut self, offset: usize, val: u16) -> usize {
        let bytes = val.to_be_bytes();
        let dst = &mut self.data[offset..offset + U16_SIZE];
        dst.copy_from_slice(&bytes);
        offset + U16_SIZE
    }

    pub fn read_i32_offset(&self, offset: usize) -> (usize, i32) {
        let start = offset;
        let end = offset + U32_SIZE;
        let bytes: [u8; 4] = self.data[start..end]
            .try_into()
            .expect("slice with incorrect length");
        (end, i32::from_be_bytes(bytes))
    }

    pub fn write_i32_offset(&mut self, offset: usize, val: i32) -> usize {
        let bytes = val.to_be_bytes();
        let dst = &mut self.data[offset..offset + U32_SIZE];
        dst.copy_from_slice(&bytes);
        offset + U32_SIZE
    }

    pub fn read_u32_offset(&self, offset: usize) -> (usize, u32) {
        let start = offset;
        let end = offset + U32_SIZE;
        let bytes: [u8; 4] = self.data[start..end]
            .try_into()
            .expect("slice with incorrect length");
        (end, u32::from_be_bytes(bytes))
    }

    pub fn write_u32_offset(&mut self, offset: usize, val: u32) -> usize {
        let bytes = val.to_be_bytes();
        let dst = &mut self.data[offset..offset + U32_SIZE];
        dst.copy_from_slice(&bytes);
        offset + U32_SIZE
    }

    // ------ atom (signed-length byte + UTF-8) ------

    pub fn read_atom_offset(&self, offset: usize) -> (usize, String) {
        // 1) length as signed i8
        let len = self.data[offset] as i8;
        let len = len as usize;
        // 2) next `len` bytes
        let start = offset + BYTE_SIZE;
        let end = start + len;
        let val = String::from_utf8(self.data[start..end].to_vec())
            .expect("invalid UTF-8 in atom");
        (end, val)
    }

    pub fn write_atom_offset(&mut self, offset: usize, val: &str) -> usize {
        assert!(val.len() > 0 && val.len() <= 127);
        // 1) write length as i8
        self.data[offset] = val.len() as u8;
        // 2) write the UTF-8 bytes
        let start = offset + BYTE_SIZE;
        let end = start + val.len();
        self.data[start..end].copy_from_slice(val.as_bytes());
        end
    }

    // ------ string (u8-length + UTF-8 + u32 overflow) ------

    pub fn read_string_offset(&self, offset: usize) -> (usize, String) {
        // 1) length as u8
        let len = self.data[offset] as usize;
        // 2) UTF-8 payload
        let start = offset + BYTE_SIZE;
        let end = start + len;
        let val = String::from_utf8(self.data[start..end].to_vec())
            .expect("invalid UTF-8 in string");
        // 3) overflow page (4 bytes BE)
        let ostart = end;
        let oend = ostart + U32_SIZE;
        let bytes: [u8; 4] = self.data[ostart..oend]
            .try_into()
            .expect("overflow slice wrong length");
        let overflow = u32::from_be_bytes(bytes);
        assert!(overflow == 0);
        (oend, val)
    }

    pub fn write_string_offset(&mut self, offset: usize, val: &str) -> usize {
        // 1) length
        assert!(val.len() <= 255);
        self.data[offset] = val.len() as u8;
        // 2) UTF-8 bytes
        let start = offset + BYTE_SIZE;
        let end = start + val.len();
        self.data[start..end].copy_from_slice(val.as_bytes());
        // 3) write zero overflow u32
        let ostart = end;
        let oend = ostart + U32_SIZE;
        let zero_bytes = 0u32.to_be_bytes();
        self.data[ostart..oend].copy_from_slice(&zero_bytes);
        oend
    }

    // ------ bytes (u8-length + raw + u32 overflow) ------

    pub fn read_bytes_offset(&self, offset: usize) -> (usize, Vec<u8>) {
        // length prefix
        let len = self.data[offset] as usize;
        // payload
        let start  = offset + BYTE_SIZE;
        let end    = start + len;
        let val    = self.data[start..end].to_vec();
        // overflow page
        let ostart = end;
        let oend   = ostart + U32_SIZE;
        let bytes: [u8; 4] = self.data[ostart..oend]
            .try_into()
            .expect("overflow slice wrong length");
        let overflow = u32::from_be_bytes(bytes);

        // <— instead of panic with just “overflow == 0”, print what we actually got
        if overflow != 0 {
            eprintln!(
                "⚠️ read_bytes_offset @ offset {}: len={} payload_bytes[{}..{}], overflow={}",
                offset, len, start, end, overflow
            );
            // you can still panic if you like:
            panic!("unexpected overflow page num {}", overflow);
        }

        (oend, val)
    }

    pub fn write_bytes_offset(&mut self, offset: usize, val: &[u8]) -> usize {
        assert!(val.len() <= 255);
        self.data[offset] = val.len() as u8;
        let start = offset + BYTE_SIZE;
        let end = start + val.len();
        self.data[start..end].copy_from_slice(val);
        let ostart = end;
        let oend = ostart + U32_SIZE;
        let zero_bytes = 0u32.to_be_bytes();
        self.data[ostart..oend].copy_from_slice(&zero_bytes);
        oend
    }
}
