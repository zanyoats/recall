use std::ops::Deref;
use std::ops::DerefMut;

use crate::errors;

use crate::storage::sizedbuf::SizedBuf;
use crate::storage::format;

#[derive(Clone, PartialEq, Eq)]
pub struct Tuple {
    buf: SizedBuf,
}

impl From<Vec<u8>> for Tuple {
    fn from(data: Vec<u8>) -> Self {
        Tuple { buf: data.into() }
    }
}

impl From<Box<[u8]>> for Tuple {
    fn from(b: Box<[u8]>) -> Self {
        Tuple { buf: Vec::from(b).into() }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ParameterType {
    Atom(String),
    Str(String),
    Int(i32),
    // following are used only internally
    U8(u8),
    U16(u16),
    U32(u32),
}

impl ParameterType {
    pub fn storage_size(&self) -> usize{
        match self {
            ParameterType::Atom(val) => SizedBuf::atom_storage_size(val.len()),
            ParameterType::Str(val) => SizedBuf::string_storage_size(val.len()),
            ParameterType::Int(_) => SizedBuf::int_storage_size(),
            ParameterType::U8(_) => format::BYTE_SIZE,
            ParameterType::U16(_) => format::U16_SIZE,
            ParameterType::U32(_) => format::U32_SIZE,
        }
    }

    pub fn get_atom(&self) -> &str {
        if let Self::Atom(val) = self { val }
        else { unreachable!() }
    }

    pub fn get_string(&self) -> &str {
        if let Self::Str(val) = self { val }
        else { unreachable!() }
    }

    pub fn get_int(&self) -> i32 {
        if let Self::Int(val) = self { *val }
        else { unreachable!() }
    }

    pub fn get_u8(&self) -> u8 {
        if let Self::U8(val) = self { *val }
        else { unreachable!() }
    }

    pub fn get_u16(&self) -> u16 {
        if let Self::U16(val) = self { *val }
        else { unreachable!() }
    }

    pub fn get_u32(&self) -> u32 {
        if let Self::U32(val) = self { *val }
        else { unreachable!() }
    }
}

impl Deref for Tuple {
    type Target = SizedBuf;

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl DerefMut for Tuple {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf
    }
}

impl Tuple {
    pub fn new(size: usize) -> Self {
        let buf = SizedBuf::new(size);

        Tuple { buf }
    }

    pub fn encode(input: &[ParameterType], scm: &[u8]) -> Result<Self, anyhow::Error> {
        let storage_size =
            input
            .iter()
            .fold(0, |acc, param_type| {
                acc + param_type.storage_size()
            });
        let mut tuple = Tuple::new(storage_size);
        let mut offset = 0;
        for (i, parameter) in scm.iter().enumerate() {
            let param_type = &input[i];

            match *parameter {
                format::ATOM_TYPE => {
                    if let ParameterType::Atom(val) = param_type {
                        offset = tuple.buf.write_atom_offset(offset, &val);
                    } else {
                        return Err(errors::RecallError::RuntimeError(
                            format!("predicate tuple position {} expected type atom", i)
                        ).into());
                    }
                },
                format::STRING_TYPE => {
                    if let ParameterType::Str(val) = param_type {
                        offset = tuple.buf.write_string_offset(offset, &val);
                    } else {
                        return Err(errors::RecallError::RuntimeError(
                            format!("predicate tuple position {} expected type string", i)
                        ).into());
                    }
                },
                format::INT_TYPE => {
                    if let ParameterType::Int(val) = param_type {
                        offset = tuple.buf.write_i32_offset(offset, *val);
                    } else {
                        return Err(errors::RecallError::RuntimeError(
                            format!("predicate tuple position {} expected type int", i)
                        ).into());
                    }
                },
                format::U8_TYPE => {
                    if let ParameterType::U8(val) = param_type {
                        offset = tuple.buf.write_u8_offset(offset, *val);
                    } else {
                        unreachable!()
                    }
                },
                format::U16_TYPE => {
                    if let ParameterType::U16(val) = param_type {
                        offset = tuple.buf.write_u16_offset(offset, *val);
                    } else {
                        unreachable!()
                    }
                },
                format::U32_TYPE => {
                    if let ParameterType::U32(val) = param_type {
                        offset = tuple.buf.write_u32_offset(offset, *val);
                    } else {
                        unreachable!()
                    }
                },
                _ => unreachable!(),
            }
        }
        Ok(tuple)
    }

    pub fn decode(&self, scm: &[u8]) -> Vec<ParameterType> {
        let mut result = vec![];
        let mut offset: usize = 0;
        for parameter in scm.iter() {
            match *parameter {
                format::ATOM_TYPE => {
                    let (offset1, val) = self.buf.read_atom_offset(offset);
                    result.push(ParameterType::Atom(val));
                    offset = offset1;
                },
                format::STRING_TYPE => {
                    let (offset1, val) = self.buf.read_string_offset(offset);
                    result.push(ParameterType::Str(val));
                    offset = offset1;
                },
                format::INT_TYPE => {
                    let (offset1, val) = self.buf.read_i32_offset(offset);
                    result.push(ParameterType::Int(val));
                    offset = offset1;
                },
                format::U8_TYPE => {
                    let (offset1, val) = self.buf.read_u8_offset(offset);
                    result.push(ParameterType::U8(val));
                    offset = offset1;
                },
                format::U16_TYPE => {
                    let (offset1, val) = self.buf.read_u16_offset(offset);
                    result.push(ParameterType::U16(val));
                    offset = offset1;
                },
                format::U32_TYPE => {
                    let (offset1, val) = self.buf.read_u32_offset(offset);
                    result.push(ParameterType::U32(val));
                    offset = offset1;
                },
                _ => unreachable!(),
            }
        }
        result
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_can_write_and_read_tuple_from_page() {
        let insert_key = 42;
        let mut size = 0;

        // tuple schema: (Id: int, Note: string, N: int, Atom: atom, Buf: string)
        let id = insert_key + 1000;
        size += SizedBuf::int_storage_size();
        let note = format!("My key is {}", insert_key);
        size += SizedBuf::string_storage_size(note.len());
        let n = -42;
        size += SizedBuf::int_storage_size();
        let atom = "foo";
        size += SizedBuf::atom_storage_size(atom.len());
        let buf = "hello, world";
        size += SizedBuf::string_storage_size(buf.len());

        let mut tuple = Tuple::new(size);
        let offset = tuple.write_i32_offset(0, id);
        let offset = tuple.write_string_offset(offset, &note);
        let offset = tuple.write_i32_offset(offset, n);
        let offset = tuple.write_atom_offset(offset, &atom);
        tuple.write_string_offset(offset, buf);

        let (offset, val) = tuple.read_i32_offset(0);
        assert_eq!(val, id);
        let (offset, val) = tuple.read_string_offset(offset);
        assert_eq!(val, note);
        let (offset, val) = tuple.read_i32_offset(offset);
        assert_eq!(val, n);
        let (offset, val) = tuple.read_atom_offset(offset);
        assert_eq!(val, atom);
        let (_, val) = tuple.read_string_offset(offset);
        assert_eq!(val, buf);
    }
}
