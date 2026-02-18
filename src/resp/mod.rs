use bytes::{BufMut, Bytes, BytesMut};

pub mod codec;

#[derive(Debug, PartialEq, Eq)]
pub enum RESPValue {
    SimpleString(Bytes),
    SimpleError(Bytes),
    Integer(i64),
    NullBulkString,
    BulkString(Bytes),
    NullArray,
    Array(Vec<RESPValue>),
}

impl RESPValue {
    /// Encodes the value to bytes according to the
    /// [RESP specification](https://redis.io/docs/latest/develop/reference/protocol-spec/#resp-protocol-description).
    pub fn encode(&self, output: &mut BytesMut) {
        match self {
            RESPValue::SimpleString(bytes) => {
                output.put_u8(b'+');
                output.extend_from_slice(bytes);
                output.extend_from_slice(b"\r\n");
            }
            RESPValue::SimpleError(bytes) => {
                output.put_u8(b'-');
                output.extend_from_slice(bytes);
                output.extend_from_slice(b"\r\n");
            }
            RESPValue::Integer(value) => {
                let prefix = format!(":{}\r\n", value);
                output.extend_from_slice(prefix.as_bytes());
            }
            RESPValue::NullBulkString => {
                output.extend_from_slice(b"$-1\r\n");
            }
            RESPValue::BulkString(bytes) => {
                let prefix = format!("${}\r\n", bytes.len());
                output.extend_from_slice(prefix.as_bytes());
                output.extend_from_slice(bytes);
                output.extend_from_slice(b"\r\n");
            }
            RESPValue::NullArray => {
                output.extend_from_slice(b"*-1\r\n");
            }
            RESPValue::Array(values) => {
                let prefix = format!("*{}\r\n", values.len());
                output.extend_from_slice(prefix.as_bytes());
                for value in values {
                    value.encode(output);
                }
            }
        }
    }
}
