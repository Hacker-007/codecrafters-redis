use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    error::{DecodeError, RedisError},
    resp::{codec::RedisCommandCodec, RESPValue, RedisCommand},
};

macro_rules! try_incomplete {
    ($e:expr) => {
        match $e {
            Ok(Some(value)) => value,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e)?,
        }
    };
}

macro_rules! try_optional {
    ($e:expr) => {
        match $e {
            Some(value) => value,
            None => return Ok(None),
        }
    };
}

pub(crate) use try_incomplete;
pub(crate) use try_optional;

/// Finds the position of the first `\r\n` in the buffer
/// or `None` if it was not found.
pub fn find_crlf(src: &[u8]) -> Option<usize> {
    memchr::memchr_iter(b'\r', src).find(|&i| i + 1 < src.len() && src[i + 1] == b'\n')
}

/// Reads the line content between the start and the next `\r\n`
/// exclusive and advances the internal cursor of `src` past the
/// `\r\n`.
pub fn parse_line(src: &mut BytesMut) -> Bytes {
    let crlf = find_crlf(src).expect("`check` should ensure sufficient bytes");
    let line = src.split_to(crlf);

    // Advance past the `\r\n` characters.
    src.advance(2);
    line.freeze()
}

/// Reads the line content between the start and the next `\r\n`
/// exclusive and parses it as an `i64`.
pub fn check_i64(src: &[u8]) -> Result<i64, DecodeError> {
    let line = std::str::from_utf8(src).map_err(|_| DecodeError::BadInteger {
        bytes: src.to_vec(),
    })?;

    line.parse::<i64>().map_err(|_| DecodeError::BadInteger {
        bytes: src.to_vec(),
    })
}

/// Reads the line content between the start and the next `\r\n`
/// exclusive and parses it as an `i64`, without any validation.
pub fn parse_i64(src: &mut BytesMut) -> i64 {
    let line = parse_line(src);
    std::str::from_utf8(&line)
        .expect("`check` should ensure valid integer")
        .parse::<i64>()
        .expect("`check` should ensure valid integer")
}

/// A stream of arguments, including positional arguments,
/// named arguments, and flags, for a Redis command.
pub struct CommandArgumentStream<'a> {
    src: &'a mut BytesMut,
    remaining: usize,
}

impl<'a> CommandArgumentStream<'a> {
    pub fn new(src: &'a mut BytesMut, length: usize) -> Self {
        Self {
            src,
            remaining: length,
        }
    }

    pub fn remaining(&self) -> usize {
        self.remaining
    }

    pub fn next(&mut self) -> Result<Bytes, DecodeError> {
        if self.remaining == 0 {
            return Err(DecodeError::WrongArity);
        }

        self.remaining -= 1;
        Ok(self.parse_part())
    }

    pub fn next_i64(&mut self) -> Result<i64, DecodeError> {
        let bytes = self.next()?;
        check_i64(&bytes)
    }

    /// Asserts that the argument stream is complete.
    pub fn finish(self) -> Result<(), DecodeError> {
        if self.remaining != 0 {
            return Err(DecodeError::ExpectedCommandEnd);
        }

        Ok(())
    }

    /// Parses a single bulk string from `src`, consuming the
    /// bytes that make up the string.
    fn parse_part(&mut self) -> Bytes {
        debug_assert_eq!(self.src[0], b'$');
        self.src.advance(1);

        let length = parse_i64(self.src);
        let data = self.src.split_to(length as usize);
        self.src.advance(2);
        data.freeze()
    }
}

/// A named flag that can only be set up to
/// once.
pub struct Slot<T>(Option<T>);

impl<T> Slot<T> {
    pub fn new() -> Self {
        Self(None)
    }

    /// Sets this slot to `value`, if it hasn't already been
    /// set before.
    pub fn set(&mut self, value: T) -> Result<(), DecodeError> {
        if self.0.is_some() {
            return Err(DecodeError::DuplicateFlag);
        }

        self.0 = Some(value);
        Ok(())
    }

    pub fn into_inner(self) -> Option<T> {
        self.0
    }
}

/// A named flag that can only be set up to
/// once, optimized for booleans.
pub struct BoolSlot(bool);

impl BoolSlot {
    pub fn new() -> Self {
        Self(false)
    }

    /// Sets this slot to `true`, if it hasn't already been
    /// set before.
    pub fn set(&mut self) -> Result<(), DecodeError> {
        if self.0 {
            return Err(DecodeError::DuplicateFlag);
        }

        self.0 = true;
        Ok(())
    }

    pub fn into_inner(self) -> bool {
        self.0
    }
}
