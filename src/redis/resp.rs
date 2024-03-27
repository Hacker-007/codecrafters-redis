use anyhow::Context;
use bytes::{Bytes, BytesMut};
use tokio::{io::AsyncReadExt, net::tcp::OwnedReadHalf};

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
    SimpleString(Bytes),
    SimpleError(Bytes),
    Integer(i64),
    BulkString(Bytes),
    NullBulkString,
    Array(Vec<RESPValue>),
    NullArray,
}

pub struct RESPValueReader {
    buf: BytesMut,
    cursor: usize,
}

impl RESPValueReader {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(8192),
            cursor: 0,
        }
    }

    pub async fn next_value(&mut self, mut reader: OwnedReadHalf) -> anyhow::Result<RESPValue> {
        loop {
            let mut bytes = [0u8; 4096];
            let n = reader.read_buf(&mut &mut bytes[..]).await?;
            if n == 0 {
                return Err(anyhow::anyhow!("[redis - error] reading from closed connection with client"))
            }

            self.buf.extend_from_slice(&bytes);
            self.cursor = 0;
            if let Some(value) = self.parse()? {
                return Ok(value);
            }
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
                "[redis - error] unknown data tag '{tag}' found"
            )),
        }
    }

    fn parse_resp_simple_string(&mut self) -> anyhow::Result<Option<RESPValue>> {
        let start = self.cursor;
        let crlf_pos = try_parse!(self.read_until(b'\r'));
        try_parse!(self.parse_crlf()?);
        Ok(Some(RESPValue::SimpleString(Bytes::copy_from_slice(
            &self.buf[start..crlf_pos],
        ))))
    }

    fn parse_resp_simple_error(&mut self) -> anyhow::Result<Option<RESPValue>> {
        let start = self.cursor;
        let crlf_pos = try_parse!(self.read_until(b'\r'));
        try_parse!(self.parse_crlf()?);
        Ok(Some(RESPValue::SimpleError(Bytes::copy_from_slice(
            &self.buf[start..crlf_pos],
        ))))
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
            return Ok(None);
        }

        let bytes = Bytes::copy_from_slice(&self.buf[self.cursor..self.cursor + length as usize]);
        self.cursor += length as usize;
        try_parse!(self.parse_crlf()?);
        Ok(Some(RESPValue::BulkString(bytes)))
    }

    fn parse_resp_array(&mut self) -> anyhow::Result<Option<RESPValue>> {
        let length = try_parse!(self.parse_integer()?);
        if length == -1 {
            return Ok(Some(RESPValue::NullArray));
        }

        let mut values = vec![];
        for _ in 0..length {
            let value = try_parse!(self.parse()?);
            values.push(value);
        }

        Ok(Some(RESPValue::Array(values)))
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
        self.buf.get(self.cursor - 1).copied()
    }
}
