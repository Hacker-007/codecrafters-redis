use bytes::Bytes;

use crate::resp::RESPValue;

pub fn simple_string(bytes: impl Into<Bytes>) -> RESPValue {
    RESPValue::SimpleString(bytes.into())
}

pub fn simple_error(bytes: impl Into<Bytes>) -> RESPValue {
    RESPValue::SimpleString(bytes.into())
}

pub fn integer(value: impl Into<i64>) -> RESPValue {
    RESPValue::Integer(value.into())
}

pub fn bulk_string(bytes: impl Into<Bytes>) -> RESPValue {
    RESPValue::BulkString(bytes.into())
}
pub fn null_bulk_string() -> RESPValue {
    RESPValue::NullBulkString
}

pub fn array(values: Vec<RESPValue>) -> RESPValue {
    RESPValue::Array(values)
}
