use std::{borrow::Cow, io::Write};

use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::RESPValue;

/// A writer that outputs RESP values in a
/// pretty format, i.e. human-readable format.
///
/// For example, the simple error "-ERR key does not exist"
/// would be written as: "(error) ERR key does not exist".
pub struct RESPWriter<'a, W> {
    writer: &'a mut W,
}

impl<'a, W: Write> RESPWriter<'a, W> {
    pub fn new(writer: &'a mut W) -> Self {
        Self { writer }
    }

    /// Writes `value` to the writer in a pretty format.
    pub fn write(&mut self, value: &RESPValue) -> std::io::Result<()> {
        self.write_indented_value(value, 0)?;
        self.writer.write_all(b"\n")
    }

    /// Writes `value` to the writer in a pretty format, with `indent`
    /// leading spaces.
    fn write_indented_value(&mut self, value: &RESPValue, indent: usize) -> std::io::Result<()> {
        match value {
            RESPValue::SimpleString(bytes) | RESPValue::BulkString(bytes) => {
                write!(self.writer, "\"{}\"", self.render_bytes(bytes))
            }
            RESPValue::SimpleError(bytes) => {
                write!(self.writer, "(error) {}", self.render_bytes(bytes))
            }
            RESPValue::Integer(n) => {
                write!(self.writer, "(integer) {n}")
            }
            RESPValue::Array(elements) if elements.is_empty() => {
                write!(self.writer, "(empty array)")
            }
            RESPValue::Array(elements) => {
                // SAFETY:
                // `elements` must be non-empty, so `ilog10` will not panic.
                let index_width = elements.len().ilog10() as usize + 1;
                for (index, element) in elements.iter().enumerate() {
                    // The indent is only applied to all elements but the
                    // first as the first is written inline.
                    if index > 0 {
                        write!(self.writer, "\n{}", " ".repeat(indent))?;
                    }

                    write!(self.writer, "{:>index_width$}) ", index + 1)?;
                    self.write_indented_value(element, indent + index_width + 2)?;
                }

                Ok(())
            }
            RESPValue::Null => {
                write!(self.writer, "(nil)")
            }
        }
    }

    /// Renders `bytes` as a best-effort UTF-8 string, resorting to
    /// using `U+FFFD REPLACEMENT CHARACTER`` for any invalid characters.
    fn render_bytes<'bytes>(&self, bytes: &'bytes [u8]) -> Cow<'bytes, str> {
        String::from_utf8_lossy(bytes)
    }
}

/// An error that occurred while reading a command
/// in pretty, human-readable format.
#[derive(Debug, Error)]
pub enum CommandReadError {
    #[error("unterminated quoted string")]
    UnterminatedQuote,
    #[error("closing quote followed by non-whitespace character")]
    TrailingCharacters,
}

/// A reader that parses a Redis command in pretty format,
/// i.e. human-readable format, as a RESP value.
///
/// For example, the input `SET "hello world" 42` would
/// be parsed as an array of three bulk strings:
/// `["SET", "hello world", "42"]`.
#[derive(Debug)]
pub struct CommandReader {
    bytes: Bytes,
    pos: usize,
}

impl CommandReader {
    pub fn new(bytes: Bytes) -> Self {
        Self { bytes, pos: 0 }
    }

    /// Parses `bytes` as a Redis command encoded as an
    /// array of bulk strings. This does not perform any
    /// validation of the command, but instead only validates
    /// the structure of the command.
    pub fn read(&mut self) -> Result<RESPValue, CommandReadError> {
        let tokens = self.split_tokens()?;
        let elements = tokens.into_iter().map(RESPValue::BulkString).collect();
        Ok(RESPValue::Array(elements))
    }

    fn split_tokens(&mut self) -> Result<Vec<Bytes>, CommandReadError> {
        let mut tokens = vec![];
        loop {
            self.skip_while(|byte| byte.is_ascii_whitespace());
            if !self.has_remaining() {
                return Ok(tokens);
            }

            let token = self.parse_token()?;
            tokens.push(token);
        }
    }

    /// Parses a single token, which may include unquoted characters
    // followed by a quoted segment.
    fn parse_token(&mut self) -> Result<Bytes, CommandReadError> {
        let mut buffer = BytesMut::new();
        loop {
            match self.next() {
                None => break,
                Some(byte) if byte.is_ascii_whitespace() => break,
                Some(b'\'') => {
                    self.parse_single_quoted(&mut buffer)?;
                    break;
                }
                Some(b'"') => {
                    self.parse_double_quoted(&mut buffer)?;
                    break;
                }
                Some(byte) => buffer.put_u8(byte),
            }
        }

        Ok(buffer.freeze())
    }

    /// Parses a single-quoted string segment, consuming bytes up to
    /// and including the closing `'`. The only escape recognized is
    /// `\'` for a literal single quote.
    fn parse_single_quoted(&mut self, buffer: &mut BytesMut) -> Result<(), CommandReadError> {
        loop {
            match self.next() {
                None => return Err(CommandReadError::UnterminatedQuote),
                Some(b'\'') => return self.check_trailing(),
                Some(b'\\') if self.peek() == Some(b'\'') => {
                    self.skip(1);
                    buffer.put_u8(b'\'');
                }
                Some(byte) => buffer.put_u8(byte),
            }
        }
    }

    /// Parses a double-quoted string segment, consuming bytes up to
    /// and including the closing `"`.
    fn parse_double_quoted(&mut self, buffer: &mut BytesMut) -> Result<(), CommandReadError> {
        loop {
            match self.next() {
                None => return Err(CommandReadError::UnterminatedQuote),
                Some(b'"') => return self.check_trailing(),
                Some(b'\\') => self.parse_double_quoted_escape(buffer),
                Some(byte) => buffer.put_u8(byte),
            }
        }
    }

    /// Parses an escape sequence inside a double-quoted string where
    /// the leading `\` has already been consumed.
    fn parse_double_quoted_escape(&mut self, buffer: &mut BytesMut) {
        // A `\xNN` translates to a single raw byte in hex-format.
        if self.peek() == Some(b'x') {
            if let (Some(h1), Some(h2)) = (self.peek_at(1), self.peek_at(2)) {
                if h1.is_ascii_hexdigit() && h2.is_ascii_hexdigit() {
                    self.skip(3);
                    buffer.put_u8(16 * self.as_hex(h1) + self.as_hex(h2));
                    return;
                }
            }
        }

        match self.next() {
            None => buffer.put_u8(b'\\'),
            Some(b'n') => buffer.put_u8(b'\n'),
            Some(b'r') => buffer.put_u8(b'\r'),
            Some(b't') => buffer.put_u8(b'\t'),
            Some(b'b') => buffer.put_u8(0x08),
            Some(b'a') => buffer.put_u8(0x07),
            Some(byte) => buffer.put_u8(byte),
        }
    }

    /// Checks that a closing quote is followed by whitespace or
    /// end of input.
    fn check_trailing(&self) -> Result<(), CommandReadError> {
        match self.peek() {
            None => Ok(()),
            Some(byte) if byte.is_ascii_whitespace() => Ok(()),
            Some(_) => Err(CommandReadError::TrailingCharacters),
        }
    }

    fn has_remaining(&self) -> bool {
        self.pos < self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<u8> {
        self.bytes.get(self.pos + offset).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let b = self.bytes.get(self.pos).copied()?;
        self.pos += 1;
        Some(b)
    }

    fn skip(&mut self, n: usize) {
        self.pos += n;
    }

    fn skip_while(&mut self, predicate: fn(u8) -> bool) {
        while self.peek().is_some_and(predicate) {
            self.pos += 1;
        }
    }

    /// Parse a hex character within the range [0, F]
    /// as a decimal.
    fn as_hex(&self, byte: u8) -> u8 {
        match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => unreachable!(),
        }
    }
}
