pub mod command;
pub mod encoding;
pub mod resp_reader;

use bytes::Bytes;

#[derive(Debug, PartialEq, Eq)]
pub enum RESPValue {
    SimpleString(Bytes),
    SimpleError(Bytes),
    Integer(i64),
    BulkString(Bytes),
    NullBulkString,
    Array(Vec<RESPValue>),
    NullArray,
}

impl RESPValue {
    pub fn into_array(self) -> Option<Vec<RESPValue>> {
        if let RESPValue::Array(values) = self {
            Some(values)
        } else {
            None
        }
    }

    pub fn into_bulk_string(self) -> Option<Bytes> {
        if let RESPValue::BulkString(bytes) = self {
            Some(bytes)
        } else {
            None
        }
    }
}
