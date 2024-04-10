use anyhow::Context;
use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use super::RESPValue;

macro_rules! handle_eof {
    ($e:expr) => {
        match $e {
            Some(value) => value,
            None => return Ok(false),
        }
    };
}

macro_rules! check_eof {
    ($e:expr) => {
        if !$e {
            return Ok(false);
        }
    };
}

pub struct RESPReader<R> {
    inner: R,
    buf: BytesMut,
    cursor: usize,
    is_closed: bool,
}

impl<R: AsyncRead + Unpin> RESPReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            buf: BytesMut::with_capacity(4096),
            cursor: 0,
            is_closed: false,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub async fn read_rdb_file(&mut self) -> anyhow::Result<Bytes> {
        loop {
            self.cursor = 0;
            if self.check_rdb_file()? {
                let bytes = self.parse_rdb_file();
                return Ok(bytes);
            }

            let n = self.inner.read_buf(&mut self.buf).await?;
            if n == 0 {
                self.is_closed = true;
                return Err(anyhow::anyhow!(
                    "[redis - error] client connection unexpectedly closed"
                ));
            }
        }
    }

    fn check_rdb_file(&mut self) -> anyhow::Result<bool> {
        let data_tag = handle_eof!(self.check_advance());
        if data_tag != b'$' {
            return Err(anyhow::anyhow!(
                "[redis - error] unexpected data tag '{}' found for RDB file",
                data_tag.escape_ascii().to_string()
            ));
        }

        let start = self.cursor;
        check_eof!(self.check_read_until(|byte| !byte.is_ascii_digit())?);
        let length = std::str::from_utf8(&self.buf[start..self.cursor])
            .context("[redis - error] expected length of RDB file to be a valid number")?
            .parse::<usize>()
            .context("[redis - error] expected length of RDB file to be a valid number")?;

        check_eof!(self.check_crlf()?);
        if self.buf.get(self.cursor + length as usize - 1).is_none() {
            return Ok(false);
        }

        self.cursor += length as usize;
        Ok(true)
    }

    fn parse_rdb_file(&mut self) -> Bytes {
        self.buf.advance(1);
        let length = self.read_until(|byte| !byte.is_ascii_digit());
        let length = std::str::from_utf8(&length)
            .unwrap()
            .parse::<usize>()
            .unwrap();

        self.parse_crlf();
        self.buf.copy_to_bytes(length as usize)
    }

    pub async fn read_value(&mut self) -> anyhow::Result<RESPValue> {
        loop {
            self.cursor = 0;
            if !self.buf.is_empty() && self.check()? {
                let value = self.parse();
                return Ok(value);
            }

            let n = self.inner.read_buf(&mut self.buf).await?;
            if n == 0 {
                self.is_closed = true;
                return Err(anyhow::anyhow!(
                    "[redis - error] client connection unexpectedly closed"
                ));
            }
        }
    }

    fn check(&mut self) -> anyhow::Result<bool> {
        let data_tag = handle_eof!(self.check_advance());
        match data_tag {
            b'+' => self.check_resp_simple_string(),
            b'-' => self.check_resp_simple_error(),
            b':' => self.check_resp_number(),
            b'$' => self.check_resp_bulk_string(),
            b'*' => self.check_resp_array(),
            tag => Err(anyhow::anyhow!(
                "[redis - error] unexpected data tag '{}' found",
                tag.escape_ascii().to_string()
            )),
        }
    }

    fn parse(&mut self) -> RESPValue {
        let data_tag = self.buf[0];
        self.buf.advance(1);
        match data_tag {
            b'+' => self.parse_resp_simple_string(),
            b'-' => self.parse_resp_simple_error(),
            b':' => self.parse_resp_number(),
            b'$' => self.parse_resp_bulk_string(),
            b'*' => self.parse_resp_array(),
            _ => unreachable!(),
        }
    }

    fn check_resp_simple_string(&mut self) -> anyhow::Result<bool> {
        check_eof!(self.check_read_until(|byte| byte == b'\r')?);
        self.check_crlf()
    }

    fn parse_resp_simple_string(&mut self) -> RESPValue {
        let bytes = self.read_until(|byte| byte == b'\r');
        self.parse_crlf();
        RESPValue::SimpleString(bytes)
    }

    fn check_resp_simple_error(&mut self) -> anyhow::Result<bool> {
        check_eof!(self.check_read_until(|byte| byte == b'\r')?);
        self.check_crlf()
    }

    fn parse_resp_simple_error(&mut self) -> RESPValue {
        let bytes = self.read_until(|byte| byte == b'\r');
        self.parse_crlf();
        RESPValue::SimpleError(bytes)
    }

    fn check_resp_number(&mut self) -> anyhow::Result<bool> {
        match handle_eof!(self.check_advance()) {
            b'+' | b'-' => {},
            digit if digit.is_ascii_digit() => self.cursor -= 1,
            byte => return Err(anyhow::anyhow!("[redis - error] expected integer to start with '+', '-', or a digit but got '{byte}' instead")),
        }

        let start = self.cursor;
        check_eof!(self.check_read_until(|byte| !byte.is_ascii_digit())?);
        std::str::from_utf8(&self.buf[start..self.cursor])
            .context("[redis - error] expected integer to be a valid signed 64-bit number")?
            .parse::<i64>()
            .context("[redis - error] expected integer to be a valid signed 64-bit number")?;

        self.check_crlf()
    }

    fn parse_resp_number(&mut self) -> RESPValue {
        let number = self.parse_number();
        self.parse_crlf();
        RESPValue::Integer(number)
    }

    fn check_resp_bulk_string(&mut self) -> anyhow::Result<bool> {
        let is_positive = match handle_eof!(self.check_advance()) {
            b'+' => true,
            b'-' => false,
            digit if digit.is_ascii_digit() => {
                self.cursor -= 1;
                true
            }
            byte => return Err(anyhow::anyhow!("[redis - error] expected integer to start with '+', '-', or a digit but got '{byte}' instead")),
        };

        let start = self.cursor;
        check_eof!(self.check_read_until(|byte| !byte.is_ascii_digit())?);
        let mut length = std::str::from_utf8(&self.buf[start..self.cursor])
            .context("[redis - error] expected length of bulk string to be a valid number")?
            .parse::<i64>()
            .context("[redis - error] expected length of bulk string to be a valid number")?;

        check_eof!(self.check_crlf()?);
        if !is_positive {
            length *= -1;
        }

        if length < -1 {
            return Err(anyhow::anyhow!(
                "[redis - error] expected length of bulk string to be >= -1 but got '{length}'"
            ));
        }

        if length == -1 {
            return Ok(true);
        }

        if self.buf.get(self.cursor + length as usize - 1).is_none() {
            return Ok(false);
        }

        self.cursor += length as usize;
        self.check_crlf()
    }

    fn parse_resp_bulk_string(&mut self) -> RESPValue {
        let length = self.parse_number();
        self.parse_crlf();
        if length == -1 {
            return RESPValue::NullBulkString;
        }

        let bytes = self.buf.copy_to_bytes(length as usize);
        self.parse_crlf();
        RESPValue::BulkString(bytes)
    }

    fn check_resp_array(&mut self) -> anyhow::Result<bool> {
        let is_positive = match handle_eof!(self.check_advance()) {
            b'+' => true,
            b'-' => false,
            digit if digit.is_ascii_digit() => {
                self.cursor -= 1;
                true
            }
            byte => return Err(anyhow::anyhow!("[redis - error] expected integer to start with '+', '-', or a digit but got '{byte}' instead")),
        };

        let start = self.cursor;
        check_eof!(self.check_read_until(|byte| !byte.is_ascii_digit())?);
        let mut length = std::str::from_utf8(&self.buf[start..self.cursor])
            .context("[redis - error] expected length of array to be a valid number")?
            .parse::<i64>()
            .context("[redis - error] expected length of array to be a valid number")?;

        if !is_positive {
            length *= -1;
        }

        check_eof!(self.check_crlf()?);
        if length < -1 {
            return Err(anyhow::anyhow!(
                "[redis - error] expected length of array to be >= -1 but got '{length}'"
            ));
        }

        if length == -1 {
            return Ok(true);
        }

        for _ in 0..length {
            check_eof!(self.check()?)
        }

        Ok(true)
    }

    fn parse_resp_array(&mut self) -> RESPValue {
        let length = self.parse_number();
        self.parse_crlf();
        if length == -1 {
            return RESPValue::NullArray;
        }

        let mut values = vec![];
        for _ in 0..length {
            values.push(self.parse());
        }

        RESPValue::Array(values)
    }

    fn parse_number(&mut self) -> i64 {
        let is_positive = match self.buf[0] {
            b'+' => {
                self.buf.advance(1);
                true
            }
            b'-' => {
                self.buf.advance(1);
                false
            }
            digit if digit.is_ascii_digit() => true,
            _ => unreachable!(),
        };

        let number = self.read_until(|byte| !byte.is_ascii_digit());
        let number = std::str::from_utf8(&number)
            .unwrap()
            .parse::<i64>()
            .unwrap();

        if is_positive {
            number
        } else {
            -number
        }
    }

    fn check_read_until(&mut self, predicate: impl Fn(u8) -> bool) -> anyhow::Result<bool> {
        loop {
            let byte = handle_eof!(self.check_advance());
            if predicate(byte) {
                self.cursor -= 1;
                break;
            }
        }

        Ok(true)
    }

    fn read_until(&mut self, predicate: impl Fn(u8) -> bool) -> Bytes {
        let mut length = 0;
        loop {
            if predicate(self.buf[length]) {
                length -= 1;
                break;
            }

            length += 1;
        }

        self.buf.copy_to_bytes(length + 1)
    }

    fn check_crlf(&mut self) -> anyhow::Result<bool> {
        let cr = handle_eof!(self.check_advance());
        if cr != b'\r' {
            return Err(anyhow::anyhow!(
                "[redis - error] expected carriage return byte but got '{}'",
                cr.escape_ascii().to_string()
            ));
        }

        let lf = handle_eof!(self.check_advance());
        if lf != b'\n' {
            return Err(anyhow::anyhow!(
                "[redis - error] expected line feed byte but got '{lf}'"
            ));
        }

        Ok(true)
    }

    fn parse_crlf(&mut self) {
        self.buf.advance(2);
    }

    fn check_advance(&mut self) -> Option<u8> {
        self.cursor += 1;
        self.buf.get(self.cursor - 1).copied()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{RESPReader, RESPValue};

    #[tokio::test]
    async fn parses_simple_string() {
        let mut stream = RESPReader::new("+PING\r\n".as_bytes());
        let value = stream.read_value().await;
        assert_eq!(
            value.unwrap(),
            RESPValue::SimpleString(Bytes::from_static(b"PING"))
        );
    }

    #[tokio::test]
    async fn parses_simple_error() {
        let mut stream = RESPReader::new("-ERR key does not exist\r\n".as_bytes());
        let value = stream.read_value().await;
        assert_eq!(
            value.unwrap(),
            RESPValue::SimpleError(Bytes::from_static(b"ERR key does not exist"))
        );
    }

    #[tokio::test]
    async fn parses_integer() {
        let mut stream = RESPReader::new(":123\r\n:-123\r\n:+123\r\n:123a\r\n".as_bytes());
        let value = stream.read_value().await;
        assert_eq!(value.unwrap(), RESPValue::Integer(123));
        let value = stream.read_value().await;
        assert_eq!(value.unwrap(), RESPValue::Integer(-123));
        let value = stream.read_value().await;
        assert_eq!(value.unwrap(), RESPValue::Integer(123));
        let value = stream.read_value().await;
        assert!(value.is_err());
    }

    #[tokio::test]
    async fn parses_bulk_string() {
        let mut stream =
            RESPReader::new("$1\r\na\r\n$5\r\nhello\r\n$-1\r\n$0\r\n\r\n$5\r\ntest\r\n".as_bytes());
        let value = stream.read_value().await;
        assert_eq!(
            value.unwrap(),
            RESPValue::BulkString(Bytes::from_static(b"a"))
        );
        let value = stream.read_value().await;
        assert_eq!(
            value.unwrap(),
            RESPValue::BulkString(Bytes::from_static(b"hello"))
        );
        let value = stream.read_value().await;
        assert_eq!(value.unwrap(), RESPValue::NullBulkString);
        let value = stream.read_value().await;
        assert_eq!(
            value.unwrap(),
            RESPValue::BulkString(Bytes::from_static(b""))
        );
        let value = stream.read_value().await;
        assert!(value.is_err());
    }

    #[tokio::test]
    async fn parses_array() {
        let mut stream =
            RESPReader::new("*1\r\n:123\r\n*2\r\n:123\r\n:456\r\n*0\r\n*-1\r\n".as_bytes());
        let value = stream.read_value().await;
        assert_eq!(
            value.unwrap(),
            RESPValue::Array(vec![RESPValue::Integer(123)])
        );
        let value = stream.read_value().await;
        assert_eq!(
            value.unwrap(),
            RESPValue::Array(vec![RESPValue::Integer(123), RESPValue::Integer(456)])
        );
        let value = stream.read_value().await;
        assert_eq!(value.unwrap(), RESPValue::Array(vec![]));
        let value = stream.read_value().await;
        assert_eq!(value.unwrap(), RESPValue::NullArray);
    }
}
