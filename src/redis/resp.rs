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

impl From<RESPValue> for Vec<u8> {
    fn from(value: RESPValue) -> Self {
        let mut output = vec![];
        match value {
            RESPValue::SimpleString(value) => {
                output.push(b'+');
                output.extend(value);
                output.extend_from_slice(b"\r\n");
            }
            RESPValue::SimpleError(value) => {
                output.push(b'-');
                output.extend(value);
                output.extend_from_slice(b"\r\n");
            }
            RESPValue::Integer(value) => {
                output.push(b':');
                output.extend_from_slice(value.to_string().as_bytes());
                output.extend_from_slice(b"\r\n");
            }
            RESPValue::BulkString(bytes) => {
                output.push(b'$');
                output.extend_from_slice(bytes.len().to_string().as_bytes());
                output.extend_from_slice(b"\r\n");
                output.extend(bytes);
                output.extend_from_slice(b"\r\n");
            }
            RESPValue::NullBulkString => {
                output.extend_from_slice(b"$-1\r\n");
            },
            RESPValue::Array(values) => {
                output.push(b'*');
                output.extend_from_slice(values.len().to_string().as_bytes());
                output.extend_from_slice(b"\r\n");
                for value in values {   
                    output.extend(Vec::from(value));
                }
            }
            RESPValue::NullArray => {
                output.extend_from_slice(b"*-1\r\n");
            }
        }

        output
    }
} 

impl RESPValue {
    pub fn into_array(self) -> Option<Vec<RESPValue>> {
        if let RESPValue::Array(values) = self {
            Some(values)
        } else {
            None
        }
    }

    pub fn into_bulk_string(self) -> Option<Vec<u8>> {
        if let RESPValue::BulkString(bytes) = self {
            Some(bytes)
        } else {
            None
        }
    }
}