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