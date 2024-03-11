use std::{fmt::Display, io::Read};

use super::resp_reader::RESPReader;

#[derive(Debug, PartialEq, Eq)]
pub enum RedisValue {
    SimpleString(String),
    BulkString(Vec<u8>),
    Array(Vec<RedisValue>),
}

impl Display for RedisValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisValue::SimpleString(value) => write!(f, "+{value}\r\n"),
            RedisValue::BulkString(values) => {
                write!(f, "${}\r\n", values.len())?;
                for value in values {
                    write!(f, "{}", *value)?;
                }

                Ok(())
            }
            RedisValue::Array(values) => {
                write!(f, "*{}\r\n", values.len())?;
                for value in values {
                    write!(f, "{value}")?;
                }

                Ok(())
            }
        }
    }
}

impl RedisValue {
    pub fn parse(reader: &mut RESPReader) -> anyhow::Result<Self> {
        let mut buf = [0];
        reader.read_exact(&mut buf)?;
        match buf[0] {
            b'+' => Self::parse_simple_string(reader),
            b'$' => Self::parse_bulk_string(reader),
            b'*' => Self::parse_array(reader),
            ty => Err(anyhow::anyhow!(
                "[redis - error] unknown data type '{ty}' found"
            )),
        }
    }

    fn parse_simple_string(reader: &mut RESPReader) -> anyhow::Result<Self> {
        reader
            .read_string()
            .map(|value| RedisValue::SimpleString(value))
    }

    fn parse_bulk_string(reader: &mut RESPReader) -> anyhow::Result<Self> {
        let length = reader.read_usize()?;
        let mut data = Vec::new();
        data.resize(length, 0);
        reader.read_exact(&mut data)?;
        let mut buf = [0, 0];
        reader.read_exact(&mut buf)?;
        if buf != "\r\n".as_bytes() {
            Err(anyhow::anyhow!(
                "[redis-error] bulk string data is longer than length"
            ))
        } else {
            Ok(RedisValue::BulkString(data))
        }
    }

    fn parse_array(reader: &mut RESPReader) -> anyhow::Result<Self> {
        let num_elements = reader.read_usize()?;
        (0..num_elements)
            .map(|_| Self::parse(reader))
            .collect::<Result<Vec<_>, _>>()
            .map(|values| RedisValue::Array(values))
    }
}
