use std::ops::Deref;
use std::ops::DerefMut;

use crate::errors;

use super::CatalogOps;
use super::CatalogPage;
use super::sizedbuf::SizedBuf;

#[derive(Clone, PartialEq, Eq)]
pub struct Tuple {
    buf: SizedBuf,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParameterType {
    Atom(String),
    String(String),
    Bytes(Vec<u8>),
    UInt(u32),
    Int(i32),
}

impl ParameterType {
    pub fn storage_size(&self) -> usize{
        match self {
            ParameterType::Atom(val) => SizedBuf::atom_storage_size(val.len()),
            ParameterType::String(val) => SizedBuf::string_storage_size(val.len()),
            ParameterType::Bytes(val) => SizedBuf::bytes_storage_size(val.len()),
            ParameterType::UInt(_) => SizedBuf::uint_storage_size(),
            ParameterType::Int(_) => SizedBuf::int_storage_size(),
        }
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

    pub fn encode(input: &[ParameterType], parameters: &[u8]) -> Result<Self, errors::RecallError> {
        let storage_size =
            input
            .iter()
            .fold(0, |acc, param_type| {
                acc + param_type.storage_size()
            });
        let mut tuple = Tuple::new(storage_size);
        let mut offset = 0;
        for (i, parameter) in parameters.iter().enumerate() {
            let param_type = &input[i];

            match *parameter {
                CatalogPage::ATOM_TYPE => {
                    if let ParameterType::Atom(val) = param_type {
                        offset = tuple.buf.write_atom_offset(offset, &val);
                    } else {
                        return Err(errors::RecallError::TypeError(format!("TypeError: predicate tuple position {} expected type atom", i)));
                    }
                }
                CatalogPage::STRING_TYPE => {
                    if let ParameterType::String(val) = param_type {
                        offset = tuple.buf.write_string_offset(offset, &val);
                    } else {
                        return Err(errors::RecallError::TypeError(format!("TypeError: predicate tuple position {} expected type string", i)));
                    }
                }
                CatalogPage::BYTES_TYPE => {
                    if let ParameterType::Bytes(val) = param_type {
                        offset = tuple.buf.write_bytes_offset(offset, &val);
                    } else {
                        return Err(errors::RecallError::TypeError(format!("TypeError: predicate tuple position {} expected type bytes", i)));
                    }
                }
                CatalogPage::UINT_TYPE => {
                    if let ParameterType::UInt(val) = param_type {
                        offset = tuple.buf.write_u32_offset(offset, *val);
                    } else {
                        return Err(errors::RecallError::TypeError(format!("TypeError: predicate tuple position {} expected type unsigned int", i)));
                    }
                }
                CatalogPage::INT_TYPE => {
                    if let ParameterType::Int(val) = param_type {
                        offset = tuple.buf.write_i32_offset(offset, *val);
                    } else {
                        return Err(errors::RecallError::TypeError(format!("TypeError: predicate tuple position {} expected type int", i)));
                    }
                }
                _ => {
                    assert!(false, "catalog parameter type is invalid");
                }
            }
        }
        Ok(tuple)
    }

    pub fn decode(&self, parameters: &[u8]) -> Vec<ParameterType> {
        let mut result = vec![];
        let mut offset: usize = 0;
        for parameter in parameters.iter() {
            match *parameter {
                CatalogPage::ATOM_TYPE => {
                    let (offset1, val) = self.buf.read_atom_offset(offset);
                    result.push(ParameterType::Atom(val));
                    offset = offset1;
                }
                CatalogPage::STRING_TYPE => {
                    let (offset1, val) = self.buf.read_string_offset(offset);
                    result.push(ParameterType::String(val));
                    offset = offset1;
                }
                CatalogPage::BYTES_TYPE => {
                    let (offset1, val) = self.buf.read_bytes_offset(offset);
                    result.push(ParameterType::Bytes(val));
                    offset = offset1;
                }
                CatalogPage::UINT_TYPE => {
                    let (offset1, val) = self.buf.read_u32_offset(offset);
                    result.push(ParameterType::UInt(val));
                    offset = offset1;
                }
                CatalogPage::INT_TYPE => {
                    let (offset1, val) = self.buf.read_i32_offset(offset);
                    result.push(ParameterType::Int(val));
                    offset = offset1;
                }
                _ => {
                    assert!(false, "catalog parameter type is invalid");
                }
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

        // tuple schema: (Id: uint, Note: string, N: int, Atom: atom, Buf: bytes)
        let id = insert_key + 1000;
        size += SizedBuf::uint_storage_size();
        let note = format!("My key is {}", insert_key);
        size += SizedBuf::string_storage_size(note.len());
        let n = -42;
        size += SizedBuf::int_storage_size();
        let atom = "foo";
        size += SizedBuf::atom_storage_size(atom.len());
        let buf = b"hello, world";
        size += SizedBuf::bytes_storage_size(buf.len());

        let mut tuple = Tuple::new(size);
        let offset = tuple.write_u32_offset(0, id);
        let offset = tuple.write_string_offset(offset, &note);
        let offset = tuple.write_i32_offset(offset, n);
        let offset = tuple.write_atom_offset(offset, &atom);
        tuple.write_bytes_offset(offset, buf);

        let (offset, val) = tuple.read_u32_offset(0);
        assert_eq!(val, id);
        let (offset, val) = tuple.read_string_offset(offset);
        assert_eq!(val, note);
        let (offset, val) = tuple.read_i32_offset(offset);
        assert_eq!(val, n);
        let (offset, val) = tuple.read_atom_offset(offset);
        assert_eq!(val, atom);
        let (_, val) = tuple.read_bytes_offset(offset);
        assert_eq!(val, buf.to_vec());
    }
}
