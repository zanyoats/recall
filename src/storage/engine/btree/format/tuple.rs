use std::ops::Deref;
use std::ops::DerefMut;

use crate::errors;

use super::sizedbuf::SizedBuf;
use crate::storage::engine::btree::format::ATOM_TYPE;
use crate::storage::engine::btree::format::STRING_TYPE;
use crate::storage::engine::btree::format::UINT_TYPE;
use crate::storage::engine::btree::format::INT_TYPE;
use crate::storage::engine::btree::format::BYTES_TYPE;

#[derive(Clone, PartialEq, Eq)]
pub struct Tuple {
    buf: SizedBuf,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
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

    pub fn get_atom(&self) -> &str {
        if let Self::Atom(val) = self {
            val
        } else {
            panic!("expected underlying to be an atom")
        }
    }

    pub fn get_string(&self) -> &str {
        if let Self::String(val) = self {
            val
        } else {
            panic!("expected underlying to be a string")
        }
    }

    pub fn get_bytes(&self) -> &[u8] {
        if let Self::Bytes(val) = self {
            val
        } else {
            panic!("expected underlying to be bytes")
        }
    }

    pub fn get_uint(&self) -> u32 {
        if let Self::UInt(val) = self {
            *val
        } else {
            panic!("expected underlying to be an uint")
        }
    }

    pub fn get_int(&self) -> i32 {
        if let Self::Int(val) = self {
            *val
        } else {
            panic!("expected underlying to be an int")
        }
    }

    pub fn typecheck(val: &Vec<Self>, schema: &[u8]) -> Result<(), errors::RecallError> {
        if val.len() != schema.len() {
            return Err(errors::RecallError::TypeError(format!("wanted arity of {} got {}", schema.len(), val.len())))
        }

        for (i, elem) in val.iter().enumerate() {
            let elem_type = &schema[i];
            match *elem_type {
                ATOM_TYPE => {
                    if let ParameterType::Atom(_) = elem {
                        continue
                    }
                    return Err(errors::RecallError::TypeError(format!("expected tuple position {i} to be typed as atom")))
                }
                STRING_TYPE => {
                    if let ParameterType::String(_) = elem {
                        continue
                    }
                    return Err(errors::RecallError::TypeError(format!("expected tuple position {i} to be typed as string")))
                }
                BYTES_TYPE => {
                    if let ParameterType::Bytes(_) = elem {
                        continue
                    }
                    return Err(errors::RecallError::TypeError(format!("expected tuple position {i} to be typed as bytes")))
                }
                UINT_TYPE => {
                    if let ParameterType::UInt(_) = elem {
                        continue
                    }
                    return Err(errors::RecallError::TypeError(format!("expected tuple position {i} to be typed as uint")))
                }
                INT_TYPE => {
                    if let ParameterType::Int(_) = elem {
                        continue
                    }
                    return Err(errors::RecallError::TypeError(format!("expected tuple position {i} to be typed as int")))
                }
                type_spec => {
                    assert!(false, "invalid type specifier stored {:?}", type_spec)
                }
            }
        }

        Ok(())
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
                ATOM_TYPE => {
                    if let ParameterType::Atom(val) = param_type {
                        offset = tuple.buf.write_atom_offset(offset, &val);
                    } else {
                        return Err(errors::RecallError::TypeError(format!("predicate tuple position {} expected type atom", i)));
                    }
                }
                STRING_TYPE => {
                    if let ParameterType::String(val) = param_type {
                        offset = tuple.buf.write_string_offset(offset, &val);
                    } else {
                        return Err(errors::RecallError::TypeError(format!("predicate tuple position {} expected type string", i)));
                    }
                }
                BYTES_TYPE => {
                    if let ParameterType::Bytes(val) = param_type {
                        offset = tuple.buf.write_bytes_offset(offset, &val);
                    } else {
                        return Err(errors::RecallError::TypeError(format!("predicate tuple position {} expected type bytes", i)));
                    }
                }
                UINT_TYPE => {
                    if let ParameterType::UInt(val) = param_type {
                        offset = tuple.buf.write_u32_offset(offset, *val);
                    } else {
                        return Err(errors::RecallError::TypeError(format!("predicate tuple position {} expected type unsigned int", i)));
                    }
                }
                INT_TYPE => {
                    if let ParameterType::Int(val) = param_type {
                        offset = tuple.buf.write_i32_offset(offset, *val);
                    } else {
                        return Err(errors::RecallError::TypeError(format!("predicate tuple position {} expected type int", i)));
                    }
                }
                _ => {
                    assert!(false, "parameter type is invalid");
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
                ATOM_TYPE => {
                    let (offset1, val) = self.buf.read_atom_offset(offset);
                    result.push(ParameterType::Atom(val));
                    offset = offset1;
                }
                STRING_TYPE => {
                    let (offset1, val) = self.buf.read_string_offset(offset);
                    result.push(ParameterType::String(val));
                    offset = offset1;
                }
                BYTES_TYPE => {
                    let (offset1, val) = self.buf.read_bytes_offset(offset);
                    result.push(ParameterType::Bytes(val));
                    offset = offset1;
                }
                UINT_TYPE => {
                    let (offset1, val) = self.buf.read_u32_offset(offset);
                    result.push(ParameterType::UInt(val));
                    offset = offset1;
                }
                INT_TYPE => {
                    let (offset1, val) = self.buf.read_i32_offset(offset);
                    result.push(ParameterType::Int(val));
                    offset = offset1;
                }
                _ => {
                    assert!(false, "parameter type is invalid");
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
