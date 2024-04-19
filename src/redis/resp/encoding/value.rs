use bytes::{BufMut, Bytes, BytesMut};

use crate::redis::resp::RESPValue;

pub fn simple_string(bytes: impl AsRef<[u8]>) -> RESPValue {
    let bytes = Bytes::copy_from_slice(bytes.as_ref());
    RESPValue::SimpleString(bytes)
}

pub fn integer(value: impl Into<i64>) -> RESPValue {
    RESPValue::Integer(value.into())
}

pub fn bulk_string(bytes: impl AsRef<[u8]>) -> RESPValue {
    let bytes = Bytes::copy_from_slice(bytes.as_ref());
    RESPValue::BulkString(bytes)
}

pub fn null_bulk_string() -> RESPValue {
    RESPValue::NullBulkString
}

pub fn array(values: Vec<RESPValue>) -> RESPValue {
    RESPValue::Array(values)
}

impl From<RESPValue> for Bytes {
    fn from(value: RESPValue) -> Self {
        let mut output = BytesMut::new();
        match value {
            RESPValue::SimpleString(bytes) => {
                output.put_u8(b'+');
                output.extend_from_slice(&bytes);
                output.extend_from_slice(b"\r\n");
            }
            RESPValue::SimpleError(bytes) => {
                output.put_u8(b'-');
                output.extend_from_slice(&bytes);
                output.extend_from_slice(b"\r\n");
            }
            RESPValue::Integer(value) => {
                let prefix = format!(":{}\r\n", value);
                output.extend_from_slice(prefix.as_bytes());
            }
            RESPValue::BulkString(bytes) => {
                let prefix = format!("${}\r\n", bytes.len());
                output.extend_from_slice(prefix.as_bytes());
                output.extend_from_slice(&bytes);
                output.extend_from_slice(b"\r\n");
            }
            RESPValue::NullBulkString => {
                output.extend_from_slice(b"$-1\r\n");
            }
            RESPValue::Array(values) => {
                let prefix = format!("*{}\r\n", values.len());
                output.extend_from_slice(prefix.as_bytes());
                values
                    .into_iter()
                    .map(Bytes::from)
                    .for_each(|bytes| output.extend_from_slice(&bytes));
            }
            RESPValue::NullArray => {
                output.extend_from_slice(b"*-1\r\n");
            }
        }

        output.freeze()
    }
}
