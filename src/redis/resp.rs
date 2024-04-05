use std::{collections::VecDeque, fmt::Display};

use anyhow::Context;
use tokio::io::AsyncReadExt;

macro_rules! try_parse {
    ($e:expr) => {
        match $e {
            Some(value) => value,
            None => return Ok(None),
        }
    };
}

#[derive(Debug)]
pub enum RESPValue {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    NullBulkString,
    Array(VecDeque<RESPValue>),
    NullArray,
}

impl Display for RESPValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RESPValue::SimpleString(value) => write!(f, "+{value}\r\n"),
            RESPValue::SimpleError(error) => write!(f, "-{error}\r\n"),
            RESPValue::Integer(value) => write!(f, ":{value}\r\n"),
            RESPValue::BulkString(value) => write!(f, "${}\r\n{}\r\n", value.len(), value),
            RESPValue::NullBulkString => write!(f, "$-1\r\n"),
            RESPValue::Array(values) => {
                write!(f, "*{}\r\n", values.len())?;
                for value in values {
                    write!(f, "{value}")?;
                }

                Ok(())
            }
            RESPValue::NullArray => write!(f, "*-1\r\n"),
        }
    }
}

pub struct RESPValueReader {
    buf: Vec<u8>,
    cursor: usize,
}

impl RESPValueReader {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(200),
            cursor: 0,
        }
    }

    pub async fn read_rdb_file<R: AsyncReadExt + Unpin>(
        &mut self,
        reader: &mut R,
    ) -> anyhow::Result<String> {
        loop {
            self.cursor = 0;
            if let Some(value) = self.parse_rdb_file()? {
                self.buf.drain(..self.cursor);
                return Ok(value);
            }

            let mut bytes = [0u8; 200];
            let n = reader.read_buf(&mut &mut bytes[..]).await?;
            if n == 0 {
                return Err(anyhow::anyhow!(
                    "[redis - error] reading from closed connection with client"
                ));
            }

            self.buf.extend_from_slice(&bytes);
        }
    }

    pub async fn read_value<R: AsyncReadExt + Unpin>(
        &mut self,
        reader: &mut R,
    ) -> anyhow::Result<RESPValue> {
        loop {
            self.cursor = 0;
            if let Some(value) = self.parse()? {
                self.buf.drain(..self.cursor);
                return Ok(value);
            }

            let mut bytes = [0u8; 200];
            let n = reader.read_buf(&mut &mut bytes[..]).await?;
            if n == 0 {
                return Err(anyhow::anyhow!(
                    "[redis - error] reading from closed connection with client"
                ));
            }

            self.buf.extend_from_slice(&bytes);
        }
    }

    fn parse(&mut self) -> anyhow::Result<Option<RESPValue>> {
        match try_parse!(self.advance()) {
            b'+' => self.parse_resp_simple_string(),
            b'-' => self.parse_resp_simple_error(),
            b':' => self.parse_resp_integer(),
            b'$' => self.parse_resp_bulk_string(),
            b'*' => self.parse_resp_array(),
            tag => Err(anyhow::anyhow!(
                "[redis - error] unknown data tag '{}' found",
                tag as char
            )),
        }
    }

    fn parse_resp_simple_string(&mut self) -> anyhow::Result<Option<RESPValue>> {
        let start = self.cursor;
        let crlf_pos = try_parse!(self.read_until(b'\r'));
        try_parse!(self.parse_crlf()?);

        let s = String::from_utf8(self.buf[start..crlf_pos].to_vec())?;
        Ok(Some(RESPValue::SimpleString(s)))
    }

    fn parse_resp_simple_error(&mut self) -> anyhow::Result<Option<RESPValue>> {
        let start = self.cursor;
        let crlf_pos = try_parse!(self.read_until(b'\r'));
        try_parse!(self.parse_crlf()?);

        let s = String::from_utf8(self.buf[start..crlf_pos].to_vec())?;
        Ok(Some(RESPValue::SimpleError(s)))
    }

    fn parse_resp_integer(&mut self) -> anyhow::Result<Option<RESPValue>> {
        let value = try_parse!(self.parse_integer()?);
        Ok(Some(RESPValue::Integer(value)))
    }

    fn parse_resp_bulk_string(&mut self) -> anyhow::Result<Option<RESPValue>> {
        let length = try_parse!(self.parse_integer()?);
        if length == -1 {
            return Ok(Some(RESPValue::NullBulkString));
        }

        if self.buf.get(self.cursor + length as usize).is_none() {
            self.buf.reserve(self.cursor + length as usize);
            return Ok(None);
        }

        let s = String::from_utf8(self.buf[self.cursor..self.cursor + length as usize].to_vec())?;
        self.cursor += length as usize;
        try_parse!(self.parse_crlf()?);
        Ok(Some(RESPValue::BulkString(s)))
    }

    fn parse_resp_array(&mut self) -> anyhow::Result<Option<RESPValue>> {
        let length = try_parse!(self.parse_integer()?);
        if length == -1 {
            return Ok(Some(RESPValue::NullArray));
        }

        let mut values = VecDeque::new();
        for _ in 0..length {
            let value = try_parse!(self.parse()?);
            values.push_back(value);
        }

        Ok(Some(RESPValue::Array(values)))
    }

    fn parse_rdb_file(&mut self) -> anyhow::Result<Option<String>> {
        if try_parse!(self.advance()) != b'$' {
            return Err(anyhow::anyhow!("[redis - error] expected an RDB file"));
        }

        let length = try_parse!(self.parse_integer()?);
        if self.buf.get(self.cursor + length as usize).is_none() {
            self.buf.reserve(self.cursor + length as usize);
            return Ok(None);
        }

        let s = String::from_utf8(self.buf[self.cursor..self.cursor + length as usize].to_vec())?;
        self.cursor += length as usize;
        Ok(Some(s))
    }

    fn parse_integer(&mut self) -> anyhow::Result<Option<i64>> {
        let start = self.cursor;
        let crlf_pos = try_parse!(self.read_until(b'\r'));
        try_parse!(self.parse_crlf()?);

        let value = std::str::from_utf8(&self.buf[start..crlf_pos])
            .unwrap()
            .parse::<i64>()
            .context(
                "[redis - error] expected integer to consist of '+', '-', or decimal digits",
            )?;

        Ok(Some(value))
    }

    fn parse_crlf(&mut self) -> anyhow::Result<Option<()>> {
        if try_parse!(self.advance()) != b'\r' || try_parse!(self.advance()) != b'\n' {
            return Err(anyhow::anyhow!("[redis - error] expected CRLF terminator"));
        }

        Ok(Some(()))
    }

    fn read_until(&mut self, terminator: u8) -> Option<usize> {
        self.read_while(|byte| byte != terminator)
    }

    fn read_while(&mut self, predicate: impl Fn(u8) -> bool) -> Option<usize> {
        loop {
            let byte = self.advance()?;
            if !predicate(byte) {
                self.cursor -= 1;
                break;
            }
        }

        Some(self.cursor)
    }

    fn advance(&mut self) -> Option<u8> {
        self.cursor += 1;
        let byte = self.buf.get(self.cursor - 1).copied()?;
        if byte == b'\0' {
            None
        } else {
            Some(byte)
        }
    }
}
