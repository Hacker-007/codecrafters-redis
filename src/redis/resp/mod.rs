pub mod command;
pub mod resp_reader;

use bytes::{BufMut, Bytes, BytesMut};

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
                output.put_u8(b':');
                output.extend_from_slice(value.to_string().as_bytes());
                output.extend_from_slice(b"\r\n");
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
                output.put_u8(b'*');
                output.extend_from_slice(values.len().to_string().as_bytes());
                output.extend_from_slice(b"\r\n");
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
