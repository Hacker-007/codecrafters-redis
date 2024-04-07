use anyhow::Context;
use tokio::io::{AsyncRead, AsyncReadExt};

use super::resp::RESPValue;

macro_rules! handle_eof {
    ($e:expr) => {
        match $e {
            Some(value) => value,
            None => return Ok(None),
        }
    };
}

pub struct RESPStream<R> {
    inner: R,
    buf: Vec<u8>,
    cursor: usize,
    is_closed: bool,
}

impl<R: AsyncRead + Unpin> RESPStream<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            buf: Vec::with_capacity(4096),
            cursor: 0,
            is_closed: false,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub async fn read_value(&mut self) -> anyhow::Result<RESPValue> {
        loop {
            self.cursor = 0;
            if let Some(value) = self.try_parse()? {
                self.buf.drain(..self.cursor);
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

    pub async fn read_rdb_file(&mut self) -> anyhow::Result<Vec<u8>> {
        loop {
            self.cursor = 0;
            if let Some(bytes) = self.try_parse_rdb()? {
                self.buf.drain(..self.cursor);
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

    fn try_parse(&mut self) -> anyhow::Result<Option<RESPValue>> {
        let data_tag = handle_eof!(self.advance());
        match data_tag {
            b'+' => self.try_parse_simple_string(),
            b'-' => self.try_parse_simple_error(),
            b':' => self.try_parse_integer(),
            b'$' => self.try_parse_bulk_string(),
            b'*' => self.try_parse_array(),
            _ => Err(anyhow::anyhow!(
                "[redis - error] unexpected data tag '{data_tag}' found"
            )),
        }
    }

    fn try_parse_simple_string(&mut self) -> anyhow::Result<Option<RESPValue>> {
        let data = handle_eof!(self.try_parse_until(|byte| byte == b'\r')?);
        handle_eof!(self.try_parse_crlf()?);
        Ok(Some(RESPValue::SimpleString(data)))
    }

    fn try_parse_simple_error(&mut self) -> anyhow::Result<Option<RESPValue>> {
        let data = handle_eof!(self.try_parse_until(|byte| byte == b'\r')?);
        handle_eof!(self.try_parse_crlf()?);
        Ok(Some(RESPValue::SimpleError(data)))
    }

    fn try_parse_integer(&mut self) -> anyhow::Result<Option<RESPValue>> {
        let number = handle_eof!(self.try_parse_number()?);
        handle_eof!(self.try_parse_crlf()?);
        Ok(Some(RESPValue::Integer(number)))
    }

    fn try_parse_bulk_string(&mut self) -> anyhow::Result<Option<RESPValue>> {
        let length = handle_eof!(self.try_parse_number()?);
        handle_eof!(self.try_parse_crlf()?);
        if length == -1 {
            return Ok(Some(RESPValue::NullBulkString));
        }

        if length < 0 {
            return Err(anyhow::anyhow!("[redis - error] expected bulk string length to be either -1 or a non-negative number"));
        }

        if self.buf.get(self.cursor + length as usize - 1).is_none() {
            return Ok(None);
        }

        let bytes = self.buf[self.cursor..self.cursor + length as usize].to_vec();
        self.cursor += length as usize;
        handle_eof!(self.try_parse_crlf()?);
        Ok(Some(RESPValue::BulkString(bytes)))
    }

    fn try_parse_array(&mut self) -> anyhow::Result<Option<RESPValue>> {
        let length = handle_eof!(self.try_parse_number()?);
        handle_eof!(self.try_parse_crlf()?);
        if length == -1 {
            return Ok(Some(RESPValue::NullArray));
        }

        if length < 0 {
            return Err(anyhow::anyhow!(
                "[redis - error] expected array length to be >= -1"
            ));
        }

        let mut values = vec![];
        for _ in 0..length {
            let value = handle_eof!(self.try_parse()?);
            values.push(value);
        }

        Ok(Some(RESPValue::Array(values)))
    }

    fn try_parse_rdb(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        let byte = handle_eof!(self.advance());
        if byte != b'$' {
            return Err(anyhow::anyhow!("[redis - error] expected RDB file to start with '$'"))
        }
        
        let length = handle_eof!(self.try_parse_number()?);
        handle_eof!(self.try_parse_crlf()?);
        if length < 0 {
            return Err(anyhow::anyhow!("[redis - error] expected RDB file length to be a non-negative number"));
        }
        
        if self.buf.get(self.cursor + length as usize - 1).is_none() {
            return Ok(None);
        }

        let bytes = self.buf[self.cursor..self.cursor + length as usize].to_vec();
        self.cursor += length as usize;
        Ok(Some(bytes))
    }

    fn try_parse_number(&mut self) -> anyhow::Result<Option<i64>> {
        let is_positive = match handle_eof!(self.peek()) {
            b'+' => {
                self.advance().unwrap();
                true
            }
            b'-' => {
                self.advance().unwrap();
                false
            }
            digit if digit.is_ascii_digit() => true,
            byte => {
                return Err(anyhow::anyhow!(
                    "[redis - error] expected '+', '-', or a digit but got '{byte}' instead"
                ))
            }
        };

        let digits = handle_eof!(self.try_parse_until(|byte| !byte.is_ascii_digit())?);
        let mut number: i64 = std::str::from_utf8(&digits)
            .unwrap()
            .parse()
            .context("[redis - error] expected integer to be a valid signed base-10 number")?;

        if !is_positive {
            number *= -1;
        }

        Ok(Some(number))
    }

    fn try_parse_crlf(&mut self) -> anyhow::Result<Option<()>> {
        let cr = handle_eof!(self.advance());
        if cr != b'\r' {
            return Err(anyhow::anyhow!(
                "[redis - error] expected carriage return, but got '{cr}' instead"
            ));
        }

        let lf = handle_eof!(self.advance());
        if lf != b'\n' {
            return Err(anyhow::anyhow!(
                "[redis - error] expected line feed, but got '{lf}' instead"
            ));
        }

        Ok(Some(()))
    }

    fn try_parse_until(
        &mut self,
        predicate: impl Fn(u8) -> bool,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let start = self.cursor;
        loop {
            let byte = handle_eof!(self.advance());
            if predicate(byte) {
                self.rewind();
                break;
            }
        }

        let bytes = self.buf[start..self.cursor].to_vec();
        Ok(Some(bytes))
    }

    fn peek(&self) -> Option<u8> {
        self.buf.get(self.cursor).copied()
    }

    fn advance(&mut self) -> Option<u8> {
        self.cursor += 1;
        self.buf.get(self.cursor - 1).copied()
    }

    fn rewind(&mut self) {
        self.cursor -= 1;
    }
}

#[cfg(test)]
mod tests {
    use crate::redis::resp::RESPValue;

    use super::RESPStream;

    #[tokio::test]
    async fn parses_simple_string() {
        let mut stream = RESPStream::new("+PING\r\n".as_bytes());
        let value = stream.read_value().await;
        assert_eq!(value.unwrap(), RESPValue::SimpleString(b"PING".to_vec()));
    }

    #[tokio::test]
    async fn parses_simple_error() {
        let mut stream = RESPStream::new("-ERR key does not exist\r\n".as_bytes());
        let value = stream.read_value().await;
        assert_eq!(
            value.unwrap(),
            RESPValue::SimpleError(b"ERR key does not exist".to_vec())
        );
    }

    #[tokio::test]
    async fn parses_integer() {
        let mut stream = RESPStream::new(":123\r\n:-123\r\n:+123\r\n:123a\r\n".as_bytes());
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
            RESPStream::new("$1\r\na\r\n$5\r\nhello\r\n$-1\r\n$0\r\n\r\n$5\r\ntest\r\n".as_bytes());
        let value = stream.read_value().await;
        assert_eq!(value.unwrap(), RESPValue::BulkString(b"a".to_vec()));
        let value = stream.read_value().await;
        assert_eq!(value.unwrap(), RESPValue::BulkString(b"hello".to_vec()));
        let value = stream.read_value().await;
        assert_eq!(value.unwrap(), RESPValue::NullBulkString);
        let value = stream.read_value().await;
        assert_eq!(value.unwrap(), RESPValue::BulkString(b"".to_vec()));
        let value = stream.read_value().await;
        assert!(value.is_err());
    }

    #[tokio::test]
    async fn parses_array() {
        let mut stream =
            RESPStream::new("*1\r\n:123\r\n*2\r\n:123\r\n:456\r\n*0\r\n*-1\r\n".as_bytes());
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
