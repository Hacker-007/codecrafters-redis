#[derive(Debug, PartialEq, Eq)]
pub enum RESPValue {
    SimpleString(Vec<u8>),
    SimpleError(Vec<u8>),
    Integer(i64),
    BulkString(Vec<u8>),
    NullBulkString,
    Array(Vec<RESPValue>),
    NullArray,
}

impl RESPValue {
    pub fn to_array(self) -> Option<Vec<RESPValue>> {
        if let RESPValue::Array(values) = self {
            Some(values)
        } else {
            None
        }
    }

    pub fn to_bulk_string(self) -> Option<Vec<u8>> {
        if let RESPValue::BulkString(bytes) = self {
            Some(bytes)
        } else {
            None
        }
    }
}